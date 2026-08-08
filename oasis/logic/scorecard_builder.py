"""
Build greenfield allocation recommendations from a client's OWN store data.

Greenfield allocation — "what should a new site carry, and how do I spend an
opening budget" — has until now required ``Full_Product_Allocation_Scorecard_*.csv``:
a 23,000-row file holding one retailer's per-SKU revenue, margins, GMROI and
named supplier terms. That file cannot ship. Competitors of the retailer it
describes are named in this product's own scenario templates, and its absolute
GMROI is exactly what the supplier-portal work established must never leave the
premises. So the feature was dead on arrival for every other client, and
would have been a data leak if it hadn't been.

This module replaces the file. It answers the same question from the catalogue
and velocity the client already has, which is both shippable and more correct:
a new store should be stocked from *this* chain's demand, not someone else's.

Two modes, and the distinction is the point:

``store``    one outlet's catalogue — for re-basing an existing site.
``network``  the union across every outlet, which is what a NEW site needs.
             Demand is averaged over the stores that actually carry a line, not
             summed: a new store behaves like an average store, not like the
             whole chain at once. ``carried_by`` then records how many outlets
             stock it, which is the chain's own assortment consensus and the
             honest replacement for the old scorecard's ``Is_Staple`` flag —
             a line every branch carries is a staple by revealed behaviour.

The output is the shape ``greenfield_runner.load_scorecard_recommendations``
produces, so ``run_greenfield_allocation`` consumes it unchanged and no CSV is
written or read.
"""

from __future__ import annotations

import statistics
from typing import Any, Dict, List, Optional

#: A line carried by at least this share of outlets counts as a staple for a
#: new site. Four in five branches stocking something is a deliberate range
#: decision, not an accident of one store's buyer.
STAPLE_CARRIAGE_RATIO = 0.8

#: Below this the line has no demand signal worth allocating capital against.
MIN_ADS = 0.01


def _num(value: Any, default: float = 0.0) -> float:
    try:
        f = float(value)
    except (TypeError, ValueError):
        return default
    return f if f == f else default          # NaN guard


def _modal(values: List[Any], default: str = "") -> str:
    vals = [str(v).strip() for v in values if v not in (None, "")]
    if not vals:
        return default
    try:
        return statistics.mode(vals)
    except statistics.StatisticsError:
        return vals[0]


def _median(values: List[float], default: float = 0.0) -> float:
    vals = [v for v in values if v > 0]
    return statistics.median(vals) if vals else default


def build_recommendations(stock_by_org: Dict[str, List[dict]],
                          mode: str = "network",
                          min_ads: float = MIN_ADS) -> Dict[str, Any]:
    """Engine-ready greenfield recs from ``{org_cd: [enriched products]}``. Pure.

    Kept free of any adapter or database so it can be unit-tested directly and
    reused by the Streamlit console.
    """
    if mode not in ("network", "store"):
        return {"recs": [], "skus": 0, "stores": 0, "mode": mode,
                "error": f"unknown mode: {mode}"}

    stores = [o for o, rows in stock_by_org.items() if rows]
    if not stores:
        return {"recs": [], "skus": 0, "stores": 0, "mode": mode,
                "error": "No product data in any store."}

    # Group every store's rows for a SKU together.
    grouped: Dict[Any, List[dict]] = {}
    for rows in stock_by_org.values():
        for r in rows or []:
            code = (r.get("item_code") or r.get("itm_cd")
                    or r.get("product_name"))
            if code is None:
                continue
            grouped.setdefault(code, []).append(r)

    recs: List[dict] = []
    for code, rows in grouped.items():
        # Average over the stores that CARRY it, not over the whole estate:
        # dividing by outlets that never stocked a line understates a
        # legitimately regional product into nonexistence.
        ads_values = [_num(r.get("avg_daily_sales")) for r in rows]
        carried = [a for a in ads_values if a > 0]
        avg_ads = (sum(carried) / len(carried)) if carried else 0.0
        if avg_ads < min_ads:
            continue

        sell = _median([_num(r.get("selling_price")) for r in rows])
        cost = _median([_num(r.get("cost_price")) for r in rows])
        margin = round((sell - cost) / sell * 100, 2) if sell > 0 and cost > 0 else None

        carried_by = len(carried)
        is_staple = (carried_by >= max(1, round(len(stores) * STAPLE_CARRIAGE_RATIO))
                     if mode == "network" else False)

        recs.append({
            "product_name": _modal([r.get("product_name") for r in rows],
                                   str(code)),
            "selling_price": sell,
            "cost_price": cost,
            "avg_daily_sales": round(avg_ads, 4),
            "product_category": _modal([r.get("department") or r.get("category")
                                        for r in rows], "GENERAL"),
            "pack_size": max(1, int(_num(
                _modal([r.get("pack_size") for r in rows], "1"), 1))),
            "moq_floor": 0,
            # A new site has no order history, by definition. Leaving a real
            # store's count here would let the engine treat an unopened shop
            # as an established buyer.
            "historical_order_count": 0,
            "is_staple_override": is_staple,
            "margin_pct": margin,
            "supplier_name": _modal([r.get("supplier_name") for r in rows],
                                    "UNKNOWN"),
            "item_code": code,
            "carried_by": carried_by,
            "carriage_ratio": round(carried_by / len(stores), 3),
            "recommended_quantity": 0,
            "reasoning": "",
        })

    recs.sort(key=lambda r: -(r["avg_daily_sales"] * max(r["selling_price"], 0.0)))
    return {"recs": recs, "skus": len(recs), "stores": len(stores),
            "mode": mode, "error": None}


def build_from_adapter(adapter, org_cds: List[str], mode: str = "network",
                       min_ads: float = MIN_ADS) -> Dict[str, Any]:
    """The same, reading each store through the one verified adapter."""
    stock: Dict[str, List[dict]] = {}
    for org in org_cds:
        try:
            stock[org] = adapter.fetch_enriched_products(org) or []
        except Exception:
            stock[org] = []
    return build_recommendations(stock, mode=mode, min_ads=min_ads)


def summarise(result: Dict[str, Any]) -> Dict[str, Any]:
    """Headline figures for a built scorecard — what a buyer looks at first."""
    recs = result.get("recs") or []
    if not recs:
        return {"skus": 0, "staples": 0, "departments": 0, "suppliers": 0,
                "daily_revenue": 0.0, "avg_margin_pct": None}
    margins = [r["margin_pct"] for r in recs if r["margin_pct"] is not None]
    return {
        "skus": len(recs),
        "staples": sum(1 for r in recs if r["is_staple_override"]),
        "departments": len({r["product_category"] for r in recs}),
        "suppliers": len({r["supplier_name"] for r in recs}),
        "daily_revenue": round(sum(r["avg_daily_sales"] * r["selling_price"]
                                   for r in recs), 2),
        "avg_margin_pct": round(sum(margins) / len(margins), 2) if margins else None,
    }
