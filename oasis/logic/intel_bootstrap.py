"""
Engine intelligence bootstrap (client onboarding, one command).

The OrderEngine reads its demand/supplier intelligence from JSON files in the
data dir. For a live client these must be generated from *their* data. The
PosErpAdapter already produces the right shapes (fetch_sales_intelligence ->
sales_forecasting, fetch_supplier_patterns -> supplier_patterns); this module
merges them network-wide and writes the ``*_updated.json`` files the engine
prefers (its loader prioritises ``_updated.json``), so the engine picks them up
with no further wiring.

    python entrypoint.py --mode bootstrap-intel       # regenerate from the POS

The merge helpers are pure and unit tested; ``bootstrap_intelligence`` is the
adapter-driven integration entry. This covers the demand + supplier layers; the
AMIT dead-stock / MANDE purge layers remain a forensic-ingestion step (see the
onboarding doc).
"""

from __future__ import annotations

import json
import os
from collections import defaultdict
from typing import Dict, List, Optional

SALES_FILE = "sales_forecasting_2025_updated.json"
SUPPLIER_FILE = "supplier_patterns_2025_updated.json"


def _trend(monthly_values: List[float]) -> tuple:
    """(trend label, trend_pct) from an ordered monthly series (pure)."""
    if len(monthly_values) < 2:
        return "stable", 0.0
    mid = len(monthly_values) // 2
    first = sum(monthly_values[:mid]) / max(mid, 1)
    second = sum(monthly_values[mid:]) / max(len(monthly_values) - mid, 1)
    if first <= 0:
        return "stable", 0.0
    pct = round((second - first) / first * 100, 1)
    if pct > 15:
        return "growing", pct
    if pct < -15:
        return "declining", pct
    return "stable", pct


def merge_sales_intelligence(per_org: List[dict]) -> dict:
    """Combine per-store sales-intelligence dicts into one network series (pure).

    Monthly quantities are summed per product across stores; totals/ADS/trend
    are recomputed. Output matches the sales_forecasting JSON format.
    """
    monthly: Dict[str, Dict[str, float]] = defaultdict(lambda: defaultdict(float))
    for d in per_org:
        for name, rec in (d or {}).items():
            for m, q in (rec.get("monthly_sales", {}) or {}).items():
                monthly[name][m] += float(q or 0)

    out = {}
    for name, ms in monthly.items():
        ms = {m: round(v, 2) for m, v in ms.items()}
        total = round(sum(ms.values()), 2)
        active = sum(1 for v in ms.values() if v > 0)
        avg_m = round(total / active, 1) if active else 0.0
        avg_d = round(total / (active * 30), 4) if active else 0.0
        trend, pct = _trend(list(ms.values()))
        out[name] = {
            "monthly_sales": ms, "total_10mo_sales": total, "months_active": active,
            "avg_monthly_sales": avg_m, "avg_daily_sales": avg_d,
            "trend": trend, "trend_pct": pct,
        }
    return out


def merge_supplier_patterns(per_org: List[dict]) -> dict:
    """Combine per-store supplier patterns into one network view (pure).

    Suppliers are network-wide; order counts are summed across stores and the
    supplier attributes (lead time, reliability, cadence) are kept.
    """
    out: Dict[str, dict] = {}
    for d in per_org:
        for sup, rec in (d or {}).items():
            if sup not in out:
                out[sup] = dict(rec)
            else:
                out[sup]["total_orders_2025"] = (
                    out[sup].get("total_orders_2025", 0) + rec.get("total_orders_2025", 0))
    return out


def bootstrap_intelligence(adapter, data_dir: str, orgs: Optional[List[str]] = None,
                           write: bool = True) -> dict:
    """Generate demand + supplier intelligence from a live POS adapter (integration).

    Returns a summary; writes the ``*_updated.json`` files the engine prefers
    when ``write`` is True.
    """
    if orgs is None:
        orgs = [o["ORG_CD"] for o in adapter.fetch_all_organizations()]

    sales = merge_sales_intelligence([adapter.fetch_sales_intelligence(o) for o in orgs])
    suppliers = merge_supplier_patterns([adapter.fetch_supplier_patterns(o) for o in orgs])

    files = {}
    if write:
        os.makedirs(data_dir, exist_ok=True)
        sf_path = os.path.join(data_dir, SALES_FILE)
        sp_path = os.path.join(data_dir, SUPPLIER_FILE)
        with open(sf_path, "w", encoding="utf-8") as f:
            json.dump(sales, f)
        with open(sp_path, "w", encoding="utf-8") as f:
            json.dump(suppliers, f)
        files = {"sales_forecasting": sf_path, "supplier_patterns": sp_path}

    return {
        "orgs": len(orgs),
        "sales_forecasting_products": len(sales),
        "supplier_patterns_suppliers": len(suppliers),
        "files": files,
    }
