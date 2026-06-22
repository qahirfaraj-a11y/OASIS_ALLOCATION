"""
Basket affinity — market-basket analysis over real POS transactions.

Builds the SKU<->SKU co-purchase layer the neutral_network graph was missing
(DHARAM's ``link`` relation). For every pair of items rung up on the same bill we
measure how strongly they travel together, then emit the *genuinely associated*
pairs as weighted ``link`` edges that DHARAM turns into anchor -> attachment
baskets (anchors come from the graph's ``velocity_ads``).

Why lift, not raw counts: a popular item (milk) co-occurs with almost everything,
so frequency alone invents affinities that don't exist. We keep a pair only when
it co-occurs more than chance would predict:

    support(a,b)    = co_count / n_baskets                  # how common the pair is
    confidence(a→b) = co_count / count(a)                   # P(b | a)
    lift(a,b)       = support(a,b) / (support(a)·support(b)) # >1 ⇒ real association
                    = co_count · n_baskets / (count(a)·count(b))

A pair is emitted when co_count ≥ min_count AND lift ≥ min_lift. The edge weight
is the co-occurrence count (DHARAM thresholds on weight ≥ min_affinity_core_count),
so the affinity map ends up holding only above-chance complements.

Pure builders (cooccurrence / affinity_metrics / link_edges) are unit tested;
mine_baskets + write_basket_layer are the DB/graph integration.
"""

from __future__ import annotations

import csv
import json
import os
from collections import Counter, defaultdict
from itertools import combinations
from typing import Dict, Iterable, List, Optional, Tuple


# ── pure market-basket core ──────────────────────────────────────────────────
def cooccurrence(transactions: Iterable[Iterable[str]],
                 min_item_count: int = 1) -> Tuple[Counter, Counter, int]:
    """Count item frequencies and unordered co-occurrence over baskets.

    Returns (pair_counts {(a,b): n, a<b}, item_counts {item: n}, n_baskets).
    Two passes when ``min_item_count`` > 1: rare items are pruned before pairing
    so the pair space stays bounded on large catalogues.
    """
    baskets = [sorted({str(i) for i in t if str(i)}) for t in transactions]
    baskets = [b for b in baskets if len(b) >= 1]
    n_baskets = len(baskets)

    item_counts: Counter = Counter()
    for b in baskets:
        item_counts.update(b)

    keep = ({i for i, c in item_counts.items() if c >= min_item_count}
            if min_item_count > 1 else None)

    pair_counts: Counter = Counter()
    for b in baskets:
        items = [i for i in b if (keep is None or i in keep)]
        if len(items) < 2:
            continue
        for a, c in combinations(items, 2):   # items pre-sorted ⇒ a < c
            pair_counts[(a, c)] += 1
    return pair_counts, item_counts, n_baskets


def affinity_metrics(pair_counts: Counter, item_counts: Counter, n_baskets: int,
                     min_count: int = 2) -> List[dict]:
    """Per-pair support / confidence (both directions) / lift, for pairs with
    co_count ≥ min_count. One row per unordered pair."""
    out: List[dict] = []
    if n_baskets <= 0:
        return out
    for (a, b), co in pair_counts.items():
        if co < min_count:
            continue
        ca, cb = item_counts.get(a, 0), item_counts.get(b, 0)
        if ca <= 0 or cb <= 0:
            continue
        lift = (co * n_baskets) / (ca * cb)
        out.append({
            "a": a, "b": b, "co_count": co,
            "support": co / n_baskets,
            "conf_a_to_b": co / ca,
            "conf_b_to_a": co / cb,
            "lift": lift,
        })
    out.sort(key=lambda m: (m["lift"], m["co_count"]), reverse=True)
    return out


def link_edges(metrics: List[dict], velocity_ads: Optional[Dict[str, float]] = None,
               min_lift: float = 1.0, min_count: int = 2,
               directed: bool = True) -> List[dict]:
    """Emit ``link`` edges (weight = co_count) for above-chance pairs.

    Directionality matters (Kenyan_Retail_Bible Ch. 8.4 "Broken Halo": affinity is
    NOT mutual — diapers pull wipes, wipes do not pull diapers; you only ever
    discount the Anchor). So we emit ONE directed edge anchor → attachment:

      * anchor = the higher-velocity SKU (the price-sensitive "destination" the
        trip is built around — matches Ch. 8.1 and DHARAM's velocity gate);
      * ties / no velocity → the association antecedent (higher confidence
        direction, i.e. the rarer item that predicts the other, à la diapers).

    Set directed=False for the legacy symmetric behaviour (two rows per pair).
    """
    vel = velocity_ads or {}
    edges: List[dict] = []
    for m in metrics:
        if m["co_count"] < min_count or m["lift"] < min_lift:
            continue
        w = int(m["co_count"])
        a, b = m["a"], m["b"]
        if not directed:
            edges.append({"source": a, "target": b, "relation": "link", "weight": w})
            edges.append({"source": b, "target": a, "relation": "link", "weight": w})
            continue
        va, vb = vel.get(a, 0.0), vel.get(b, 0.0)
        if va != vb:
            anchor, attach = (a, b) if va > vb else (b, a)
        else:
            anchor, attach = (a, b) if m["conf_a_to_b"] >= m["conf_b_to_a"] else (b, a)
        edges.append({"source": anchor, "target": attach, "relation": "link", "weight": w})
    return edges


def anchors_and_attachments(metrics: List[dict], velocity_ads: Dict[str, float],
                            min_anchor_ads: float = 0.1, min_count: int = 2,
                            min_lift: float = 1.0,
                            top_k: Optional[int] = None) -> Dict[str, List[dict]]:
    """Synthesis preview: which high-velocity anchors carry which attachments.

    Mirrors DHARAM's selection so we can validate baskets without the engine:
    anchor must have velocity_ads ≥ min_anchor_ads; attachments are above-chance
    co-purchased partners, strongest first.
    """
    by_anchor: Dict[str, List[dict]] = defaultdict(list)
    for m in metrics:
        if m["co_count"] < min_count or m["lift"] < min_lift:
            continue
        for anchor, attach, conf in ((m["a"], m["b"], m["conf_a_to_b"]),
                                     (m["b"], m["a"], m["conf_b_to_a"])):
            if velocity_ads.get(anchor, 0.0) >= min_anchor_ads:
                by_anchor[anchor].append({
                    "attachment": attach, "co_count": m["co_count"],
                    "confidence": round(conf, 4), "lift": round(m["lift"], 3),
                })
    for anchor in by_anchor:
        by_anchor[anchor].sort(key=lambda x: (x["lift"], x["co_count"]), reverse=True)
        if top_k:
            by_anchor[anchor] = by_anchor[anchor][:top_k]
    return dict(by_anchor)


# ── DB / graph integration ───────────────────────────────────────────────────
def load_transactions(db_path: str, org: Optional[str] = None,
                      since: Optional[str] = None) -> List[List[str]]:
    """Read POS_SALES_DTL into baskets: distinct items per (org, bill, date)."""
    import sqlite3
    conn = sqlite3.connect(db_path, timeout=60.0)
    try:
        where = ["COALESCE(VOID_FLAG,'F') <> 'T'"]
        params: List = []
        if org:
            where.append("ORG_CD = ?")
            params.append(org)
        if since:
            where.append("BILL_DT >= ?")
            params.append(since)
        sql = ("SELECT ORG_CD, BILL_DT, BILL_NO, ITM_CD FROM POS_SALES_DTL "
               "WHERE " + " AND ".join(where) + " ORDER BY ORG_CD, BILL_DT, BILL_NO")
        baskets: List[List[str]] = []
        cur_key = None
        cur: set = set()
        for org_cd, bdt, bno, itm in conn.execute(sql, params):
            key = (org_cd, bdt, bno)
            if key != cur_key:
                if cur:
                    baskets.append(sorted(cur))
                cur_key, cur = key, set()
            if itm:
                cur.add(str(itm))
        if cur:
            baskets.append(sorted(cur))
        return baskets
    finally:
        conn.close()


def _load_velocity(nn_dir: str) -> Dict[str, float]:
    """SKU -> velocity_ads from the graph's nodes.csv (best-effort)."""
    path = os.path.join(nn_dir, "nodes.csv")
    vel: Dict[str, float] = {}
    if not os.path.exists(path):
        return vel
    with open(path, "r", encoding="utf-8") as f:
        for row in csv.DictReader(f):
            sid = row.get("id") or row.get("sku") or row.get("ITM_CD")
            if sid:
                try:
                    vel[str(sid)] = float(row.get("velocity_ads", 0) or 0)
                except (TypeError, ValueError):
                    vel[str(sid)] = 0.0
    return vel


def mine_baskets(db_path: str, org: Optional[str] = None, since: Optional[str] = None,
                 min_count: int = 3, min_lift: float = 1.0, min_item_count: int = 5,
                 velocity_ads: Optional[Dict[str, float]] = None) -> dict:
    """End-to-end mine: transactions -> metrics -> directed link edges (no writes).

    velocity_ads (SKU -> ADS) orients each edge anchor -> attachment; without it
    direction falls back to the association antecedent.
    """
    txns = load_transactions(db_path, org=org, since=since)
    pairs, items, n = cooccurrence(txns, min_item_count=min_item_count)
    metrics = affinity_metrics(pairs, items, n, min_count=min_count)
    edges = link_edges(metrics, velocity_ads=velocity_ads,
                       min_lift=min_lift, min_count=min_count)
    return {
        "n_baskets": n, "n_items": len(items), "n_pairs": len(pairs),
        "n_assoc_pairs": len(edges), "metrics": metrics, "edges": edges,
    }


def write_basket_layer(nn_dir: str, edges: List[dict], metrics: List[dict],
                       n_baskets: int) -> dict:
    """Merge ``link`` edges into edges.csv (idempotent) and write the analyst
    artifact basket_affinity.csv + basket_summary.json."""
    os.makedirs(nn_dir, exist_ok=True)
    edges_path = os.path.join(nn_dir, "edges.csv")
    cols = ["source", "target", "relation", "weight"]

    existing: List[dict] = []
    if os.path.exists(edges_path):
        with open(edges_path, "r", encoding="utf-8") as f:
            for row in csv.DictReader(f):
                if (row.get("relation") or "").strip() != "link":   # drop stale link layer
                    existing.append({c: row.get(c, "") for c in cols})
    with open(edges_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=cols)
        w.writeheader()
        w.writerows(existing)
        w.writerows({c: e.get(c, "") for c in cols} for e in edges)

    # rich analyst artifact (why each basket pair is linked)
    aff_path = os.path.join(nn_dir, "basket_affinity.csv")
    with open(aff_path, "w", newline="", encoding="utf-8") as f:
        w = csv.DictWriter(f, fieldnames=[
            "a", "b", "co_count", "support", "conf_a_to_b", "conf_b_to_a", "lift"])
        w.writeheader()
        for m in metrics:
            w.writerow({k: (round(v, 6) if isinstance(v, float) else v)
                        for k, v in m.items()})

    summary = {
        "n_baskets": n_baskets, "assoc_pairs": len(edges),
        "link_edges_written": len(edges),
        "edges_csv": edges_path, "affinity_csv": aff_path,
    }
    with open(os.path.join(nn_dir, "basket_summary.json"), "w", encoding="utf-8") as f:
        json.dump(summary, f, indent=2)
    return summary


def build_baskets_from_db(db_path: str, nn_dir: str, org: Optional[str] = None,
                          since: Optional[str] = None, min_count: int = 3,
                          min_lift: float = 1.0, min_item_count: int = 5) -> dict:
    """Mine co-purchase from POS and write the basket affinity layer + artifact."""
    velocity = _load_velocity(nn_dir)
    res = mine_baskets(db_path, org=org, since=since, min_count=min_count,
                       min_lift=min_lift, min_item_count=min_item_count,
                       velocity_ads=velocity)
    summary = write_basket_layer(nn_dir, res["edges"], res["metrics"], res["n_baskets"])
    summary.update({"n_items": res["n_items"], "n_pairs_seen": res["n_pairs"]})
    return summary
