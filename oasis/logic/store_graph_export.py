"""
Native store-graph export (client onboarding, headless).

Builds ``stores_network.json`` — the *store-level* graph the GNN / store-risk and
the Market Intelligence Tool read — from the client's own organisations
(`ORGANIZATION_MST`), so a new client's stores (not the demo's 14 the reference client
stores) drive the GNN surfaces.

Crucially each node's ``store_id`` is set to the client's ``ORG_CD``, so the
store-risk org-code join is exact (no CFP→ORG mapping needed) — closing the
silent fallback where a mismatched graph made every store inventory-only.

The client POS won't carry footfall/geo/floor-area, so those feature fields fall
back to neutral defaults; the GNN is monitoring-only, so per-store risk is
inventory-dominated and degrades gracefully regardless.

    python entrypoint.py --mode build-store-graph

Pure ``build_stores_network`` is unit tested; the adapter-driven entry is
integration.
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Dict, List, Optional

_DEFAULTS = {
    "store_category": "Medium Anchor",
    "floor_area_sqft": 10000,
    "monthly_budget": 50_000_000,
    "footfall_rank": 5.0,
    "catchment_affluence_index": 3.0,
    "brand_strength_score": 50.0,
    "supplier_diversity_score": 0.5,
    "demand_scale_factor": 1.0,
    "safety_days": 14,
    "reorder_frequency_days": 7,
}


def _stock_profile(products: Optional[List[dict]]) -> List[dict]:
    """Per-department ADS rollup for a store's stock (pure). Optional GNN input."""
    if not products:
        return []
    by_dept: Dict[str, float] = defaultdict(float)
    for p in products:
        dept = str(p.get("department", "GENERAL") or "GENERAL")
        by_dept[dept] += float(p.get("avg_daily_sales", 0) or 0)
    return [{"department": d, "ads_scaled": round(a, 4)} for d, a in by_dept.items()]


def build_stores_network(org_records: List[dict],
                         stock_by_org: Optional[Dict[str, list]] = None,
                         network_name: str = "Client Network") -> dict:
    """Build the stores_network.json structure from organisation records (pure).

    ``org_records`` are ORGANIZATION_MST rows (ORG_CD/ORG_NAME/ORG_CITY/ORG_STATE).
    ``store_id`` == ``ORG_CD`` so per-store risk joins exactly to live stock.
    """
    stock_by_org = stock_by_org or {}
    stores = []
    for i, o in enumerate(org_records):
        cd = str(o.get("ORG_CD", "") or "").strip()
        if not cd:
            continue
        city = str(o.get("ORG_CITY", "") or "")
        region = str(o.get("ORG_STATE", "") or "") or city or "default"
        stores.append({
            "store_id": cd,                      # == ORG_CD -> exact risk join
            "name": str(o.get("ORG_NAME", cd) or cd),
            "city": city,
            "region": region,
            "is_reference_store": (i == 0),
            "stock_profile": _stock_profile(stock_by_org.get(cd)),
            **_DEFAULTS,
        })
    return {
        "network_name": network_name,
        "store_count": len(stores),
        "reference_store": stores[0]["store_id"] if stores else None,
        "stores": stores,
    }


def build_store_graph_from_adapter(adapter, out_path: str,
                                   include_stock: bool = True) -> dict:
    """Build stores_network.json from the live POS org list (+ optional stock)."""
    orgs = adapter.fetch_all_organizations()
    stock_by_org = None
    if include_stock:
        stock_by_org = {}
        for o in orgs:
            cd = o.get("ORG_CD")
            if not cd:
                continue
            try:
                stock_by_org[cd] = adapter.fetch_enriched_products(cd)
            except Exception:
                stock_by_org[cd] = []
    net = build_stores_network(orgs, stock_by_org)
    os.makedirs(os.path.dirname(os.path.abspath(out_path)) or ".", exist_ok=True)
    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(net, f, indent=2)
    return {"stores": net["store_count"], "file": out_path,
            "store_ids": [s["store_id"] for s in net["stores"][:10]]}
