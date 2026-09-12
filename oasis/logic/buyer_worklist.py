"""Lines the engine deliberately refused, put in front of a person.

WHY THIS EXISTS
    The ordering engine declines a line when ONE PACK would be more cover than
    MAX_AUTO_ORDER_COVER_DAYS: "special order or transfer, not
    replenishment". That refusal is correct. An ironing board selling 0.02 a
    day, where the smallest orderable quantity is 62 days of stock, is not a
    replenishment decision and a replenishment rule should not make it.

    That is now the whole of the rule, but it was not when this list was
    first built. The cap used to judge the rounded-up position, which also
    caught 102 lines whose own order-up-to level was inside the cap and that
    could have been served by one pack fewer -- refusing 61 days of cover by
    buying 0 rather than 54. Those belong in a purchase order, not on a
    buyer's desk, and order_up_to.py now rounds towards the cap instead of
    refusing at it. What is left here is only the population for which no
    orderable quantity exists.

    What happened next was not correct. The engine set transfer_candidate and
    handed off to the transfer module, which cannot take sub-1/day lines: a
    donor's fractional excess is floored, so it needs three or more units
    before one is releasable, and a flat transfer cost fails its own
    cost-ratio gate on a single unit. Refused by ordering, unreachable by
    transfer, and filtered out of the MOQ gate's own reject list because that
    gate skips q <= 0 — so nobody ever saw them.

    Measured on the live book: 374 such lines, ALL of them already at zero
    stock, none reaching a purchase order and none appearing in the MOQ
    reject list either, together representing KES 460,561 of annual gross
    profit against KES 334,228 of one-time capital to stock them once. The
    median line would take 91 days to sell a single pack.

WHAT THIS DOES NOT DO
    It does not decide. Whether to stock a slow high-value line or delist it
    is an assortment decision that depends on range strategy, shelf space and
    what a buyer knows about the category — none of which this engine has.
    The engine has already done the part it is qualified for: how slow the
    line is, how much cover one pack represents, what it earns, what it ties
    up. That analysis was being computed and thrown away. This hands it over.

    The return figure is deliberately not called a recommendation. Annual
    gross profit is what the line earned WHEN IT WAS STOCKED, and every line
    here is at zero now, so the rate may be stale. It is an order of
    magnitude for a person to weigh, not a number to act on unread.
"""
from __future__ import annotations

from typing import Any, Dict, List, Optional, Sequence

#: Lines earning less than this a year are listed but flagged, because the
#: decision is usually "delist" and a buyer's attention is the scarce thing.
TRIVIAL_GP_PER_YEAR = 1_000.0


def _f(rec: Dict[str, Any], *names: str, default: float = 0.0) -> float:
    """First of several spellings that carries a usable number."""
    for n in names:
        v = rec.get(n)
        if v in (None, ""):
            continue
        try:
            f = float(v)
        except (TypeError, ValueError):
            continue
        if f != 0.0:
            return f
    return default


def build(recommendations: Sequence[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """The refused lines, priced, worst-first by what they earn.

    Pure: takes the recommendations the pipeline already produced and returns
    rows. No I/O, so it can be tested without a database and called from any
    surface that has a scan in hand.
    """
    out: List[Dict[str, Any]] = []
    for r in recommendations or []:
        if not r.get("auto_order_suppressed"):
            continue

        d = _f(r, "avg_daily_sales")
        pack = max(1.0, _f(r, "pack_size", default=1.0))
        cost = _f(r, "unit_cost", "cost_price", "cost_est")
        sell = _f(r, "selling_price", "sell_price")
        # margin_pct arrives as 0.0 on most of these rows, so derive rather
        # than trust it; fall back to it only when prices are missing.
        gp_unit = (sell - cost) if (sell > 0 and cost > 0) else \
            cost * _f(r, "margin_pct") / 100.0

        gp_year = d * 365.0 * gp_unit if d > 0 else 0.0
        capital = pack * cost
        out.append({
            "sku": r.get("sku") or r.get("item_code") or "",
            "product_name": r.get("product_name") or "",
            "supplier_name": r.get("supplier_name") or "",
            "department": r.get("department") or r.get("product_category") or "",
            "reason": r.get("suppress_reason") or "",
            "avg_daily_sales": d,
            "current_stock": _f(r, "current_stock", "current_stocks"),
            "pack_size": pack,
            "one_pack_days": (pack / d) if d > 0 else float("inf"),
            "gp_per_unit": gp_unit,
            "gp_per_year": gp_year,
            "capital_once": capital,
            # None rather than a divide-by-zero or a fake infinity: a line we
            # cannot price should not sort above one we can.
            "return_on_capital": (gp_year / capital) if capital > 0 else None,
            "trivial": gp_year < TRIVIAL_GP_PER_YEAR,
        })

    out.sort(key=lambda x: -x["gp_per_year"])
    return out


def summarise(rows: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Headline numbers for the panel that shows the list."""
    rows = list(rows or [])
    empty = [r for r in rows if r["current_stock"] <= 0]
    worth = [r for r in rows if not r["trivial"]]
    return {
        "lines": len(rows),
        "out_of_stock": len(empty),
        "gp_per_year": sum(r["gp_per_year"] for r in rows),
        "capital_once": sum(r["capital_once"] for r in rows),
        "worth_reviewing": len(worth),
        "median_one_pack_days": _median(
            [r["one_pack_days"] for r in rows if r["one_pack_days"] != float("inf")]),
    }


def _median(xs: Sequence[float]) -> Optional[float]:
    xs = sorted(xs)
    if not xs:
        return None
    mid = len(xs) // 2
    return xs[mid] if len(xs) % 2 else (xs[mid - 1] + xs[mid]) / 2.0
