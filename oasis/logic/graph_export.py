"""
Native product-graph export (client onboarding, headless).

Regenerates the heterogeneous product graph (SKU / Supplier / Department nodes +
edges) that the governance engines (AMIT/MANDE/DHARAM) and the ST-GAT GNN read
from ``neutral_network_export/`` — **natively, with no Obsidian dependency**, so
a client install can rebuild it headlessly.

The original graph was authored in Obsidian (notes = nodes, ``[[wikilinks]]`` =
edges); its substitution links proved opaque (no clean attribute rule — price,
the best single explainer, recovered only ~18%). Rather than replicate an opaque
rule, this builds a **transparent, weighted** substitution edge:

    substitution(SKU) = the top-K SKUs in the SAME department that are most
    similar by a z-normalised composite of [price, velocity_ads], each edge
    carrying a similarity weight (Obsidian's edges were unweighted — this is the
    "discover more" upgrade).

Edges emitted (all consumable by the existing loaders, which key on
source/target/relation and tolerate the extra `weight` column):
    upstream_supply   SKU -> Supplier
    downstream_demand SKU -> Department
    substitution      SKU -> SKU (same dept, weighted)

Pure builders are unit tested; the loader + writer are the integration entry
behind ``python entrypoint.py --mode build-graph``.

Not yet emitted: real SKU<->SKU basket affinity (DHARAM's ``link`` relation) —
that needs POS co-purchase (same BILL_NO) mining and is the documented next step.
"""

from __future__ import annotations

import csv
import json
import math
import os
from collections import defaultdict
from typing import Dict, List

NODE_COLUMNS = ["type", "department", "supplier", "price", "margin_pct", "revenue",
                "gross_profit", "sales_rank", "velocity_ads", "total_quantity",
                "rhapta_fill_rate", "id"]
_PRICE_WINDOW = 30   # candidate neighbours by price before composite refine


# ── pure builders ────────────────────────────────────────────────────────────
def _zscores(values: List[float]) -> Dict[int, float]:
    """Index -> z-score for a list (pure); zero variance -> all 0."""
    n = len(values)
    if n == 0:
        return {}
    mean = sum(values) / n
    var = sum((v - mean) ** 2 for v in values) / n
    sd = math.sqrt(var)
    if sd < 1e-9:
        return {i: 0.0 for i in range(n)}
    return {i: (v - mean) / sd for i, v in enumerate(values)}


def substitution_edges(records: List[dict], k: int = 8) -> List[dict]:
    """Top-K same-department substitutes per SKU by composite [price, velocity]
    similarity, weighted (pure).

    Returns directed edge dicts {source, target, relation, weight}. Within a
    department, candidates are the nearest by price (a window), refined by the
    z-normalised euclidean distance over (price, velocity_ads); weight =
    1/(1+distance).
    """
    by_dept: Dict[str, List[dict]] = defaultdict(list)
    for r in records:
        by_dept[str(r.get("department", "")).strip().upper()].append(r)

    edges: List[dict] = []
    for dept, items in by_dept.items():
        if not dept or len(items) < 2:
            continue
        items = sorted(items, key=lambda r: float(r.get("price", 0) or 0))
        prices = [float(r.get("price", 0) or 0) for r in items]
        vels = [float(r.get("velocity_ads", 0) or 0) for r in items]
        pz = _zscores(prices)
        vz = _zscores(vels)
        n = len(items)
        for i in range(n):
            lo = max(0, i - _PRICE_WINDOW)
            hi = min(n, i + _PRICE_WINDOW + 1)
            scored = []
            for j in range(lo, hi):
                if j == i:
                    continue
                dist = math.sqrt((pz[i] - pz[j]) ** 2 + (vz[i] - vz[j]) ** 2)
                scored.append((dist, j))
            scored.sort()
            for dist, j in scored[:k]:
                edges.append({
                    "source": items[i]["id"], "target": items[j]["id"],
                    "relation": "substitution", "weight": round(1.0 / (1.0 + dist), 4),
                })
    return edges


def structural_edges(records: List[dict]) -> List[dict]:
    """SKU->Supplier (upstream_supply) and SKU->Department (downstream_demand) (pure)."""
    edges: List[dict] = []
    for r in records:
        sid = r["id"]
        sup = str(r.get("supplier", "") or "").strip()
        dept = str(r.get("department", "") or "").strip()
        if sup:
            edges.append({"source": sid, "target": sup,
                          "relation": "upstream_supply", "weight": 1.0})
        if dept:
            edges.append({"source": sid, "target": dept,
                          "relation": "downstream_demand", "weight": 1.0})
    return edges


def build_graph(records: List[dict], k: int = 8) -> tuple:
    """Assemble node rows (SKU + Supplier + Department) and all edges (pure).

    Returns (node_rows, edge_rows) ready to write to nodes.csv / edges.csv.
    """
    node_rows: List[dict] = []
    suppliers, depts = set(), set()
    for r in records:
        sup = str(r.get("supplier", "") or "").strip()
        dept = str(r.get("department", "") or "").strip()
        if sup:
            suppliers.add(sup)
        if dept:
            depts.add(dept)
        node_rows.append({
            "type": "SKU", "department": dept, "supplier": sup,
            "price": r.get("price", 0) or 0, "margin_pct": r.get("margin_pct", 0) or 0,
            "revenue": r.get("revenue", 0) or 0, "gross_profit": r.get("gross_profit", 0) or 0,
            "sales_rank": r.get("sales_rank", 99999) or 99999,
            "velocity_ads": r.get("velocity_ads", 0) or 0,
            "total_quantity": r.get("total_quantity", 0) or 0,
            "rhapta_fill_rate": r.get("rhapta_fill_rate", 0) or 0,
            "id": r["id"],
        })
    for s in sorted(suppliers):
        node_rows.append({c: "" for c in NODE_COLUMNS} | {"type": "Supplier", "id": s})
    for d in sorted(depts):
        node_rows.append({c: "" for c in NODE_COLUMNS} | {"type": "Department", "id": d})

    edge_rows = structural_edges(records) + substitution_edges(records, k=k)
    return node_rows, edge_rows


# ── integration: load + write ────────────────────────────────────────────────
def load_node_records(data_dir: str) -> List[dict]:
    """Assemble network-wide SKU node records from the canonical files.

    price + department from the dept_*.xlsx catalog; supplier from
    product_supplier_map; revenue/margin/rank/qty from sales_profitability;
    velocity_ads from corrected_ads_from_pos.
    """
    import glob

    import pandas as pd

    def _j(name):
        for cand in (name, name.replace(".json", "") ):
            p = os.path.join(data_dir, cand)
            if os.path.exists(p):
                try:
                    return json.load(open(p, encoding="utf-8"))
                except Exception:
                    return {}
        return {}

    supplier_map = {k.strip().upper(): (v if isinstance(v, str) else "")
                    for k, v in _j("product_supplier_map.json").items()}
    profit = {k.strip().upper(): v for k, v in _j("sales_profitability_intelligence_2025_updated.json").items()}
    ads = _j("corrected_ads_from_pos.json")
    ads_map = {k.strip().upper(): (v.get("new_ads", v.get("old_ads", 0)) if isinstance(v, dict) else v)
               for k, v in ads.items()}

    records: Dict[str, dict] = {}
    for f in sorted(glob.glob(os.path.join(data_dir, "dept_*.xlsx"))):
        try:
            df = pd.read_excel(f, usecols=["ITM_NAME", "DEPARTMENT", "SellPrice"])
        except Exception:
            continue
        for _, row in df.iterrows():
            name = str(row.get("ITM_NAME", "")).strip()
            if not name:
                continue
            key = name.upper()
            pr = profit.get(key, {})
            records[key] = {
                "id": name,
                "department": str(row.get("DEPARTMENT", "") or "").strip(),
                "supplier": supplier_map.get(key, ""),
                "price": float(row.get("SellPrice", 0) or 0),
                "velocity_ads": float(ads_map.get(key, 0) or 0),
                "revenue": float(pr.get("revenue", 0) or 0),
                "margin_pct": float(pr.get("margin_pct", 0) or 0),
                "gross_profit": float(pr.get("gross_profit", 0) or 0),
                "sales_rank": float(pr.get("sales_rank", 99999) or 99999),
                "total_quantity": float(pr.get("total_qty_sold", 0) or 0),
                "rhapta_fill_rate": 0.0,
            }
    return list(records.values())


def write_graph(node_rows: List[dict], edge_rows: List[dict], out_dir: str) -> dict:
    """Write nodes.csv, edges.csv, full_graph.json to out_dir."""
    os.makedirs(out_dir, exist_ok=True)
    nodes_path = os.path.join(out_dir, "nodes.csv")
    edges_path = os.path.join(out_dir, "edges.csv")
    graph_path = os.path.join(out_dir, "full_graph.json")

    with open(nodes_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=NODE_COLUMNS)
        w.writeheader()
        for r in node_rows:
            w.writerow({c: r.get(c, "") for c in NODE_COLUMNS})

    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=["source", "target", "relation", "weight"])
        w.writeheader()
        w.writerows(edge_rows)

    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump({"nodes": node_rows, "links": edge_rows}, f)

    return {"nodes": nodes_path, "edges": edges_path, "graph": graph_path}


def build_graph_from_data(data_dir: str, out_dir: str, k: int = 8) -> dict:
    """Full pipeline: load canonical records -> build graph -> write files (integration)."""
    records = load_node_records(data_dir)
    node_rows, edge_rows = build_graph(records, k=k)
    files = write_graph(node_rows, edge_rows, out_dir)
    rel_counts: Dict[str, int] = defaultdict(int)
    for e in edge_rows:
        rel_counts[e["relation"]] += 1
    return {
        "skus": len(records),
        "nodes": len(node_rows),
        "edges": len(edge_rows),
        "relations": dict(rel_counts),
        "files": files,
    }
