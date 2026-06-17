"""
Capital-recovery feeder for the journey value thread (SH-B).

The journey's ``value_recovered`` meter was never populated. This module
computes the capital trapped in dead stock (the AMIT opportunity) from the
live product feed and translates it into a recovery figure for
``journey_state``:

    value_target    = high-water mark of trapped capital seen (the opportunity)
    value_recovered = max(0, target - current_trapped)   (freed from the peak)

So on first run recovered = 0 (target baselined to current trapped); as dead
stock is cleared, current trapped falls and recovered rises; if dead stock
grows past the peak, the target rises and recovered holds at 0. The meter then
reads recovered/target = "% of trapped capital cleared".

Pure helpers (``trapped_capital``, ``trapped_capital_network``) are import-safe
and unit tested; ``update_journey_recovery`` persists via journey_state.
"""

from __future__ import annotations

from typing import Dict, Sequence

# AMIT dead-stock rule (matches the engines / Exec ROI tab).
DEAD_ADS = 0.2
DEAD_SOH = 15.0


def _unit_cost(p: dict) -> float:
    cost = float(p.get("cost_price", p.get("wac", 0)) or 0)
    if cost > 0:
        return cost
    # Fallback: 75% of selling price (consistent with the rest of the codebase).
    return float(p.get("selling_price", p.get("sell_price", 0)) or 0) * 0.75


def trapped_capital(products: Sequence[dict],
                    dead_ads: float = DEAD_ADS, dead_soh: float = DEAD_SOH) -> float:
    """KES trapped in dead stock for one product list (pure).

    Dead = ADS < dead_ads AND on-hand > dead_soh; value = on-hand × unit cost.
    """
    total = 0.0
    for p in products or []:
        ads = float(p.get("avg_daily_sales", 0) or 0)
        soh = float(p.get("current_stocks", p.get("current_stock", 0)) or 0)
        if ads < dead_ads and soh > dead_soh:
            total += soh * _unit_cost(p)
    return round(total, 2)


def trapped_capital_network(products_by_org: Dict[str, Sequence[dict]],
                            dead_ads: float = DEAD_ADS, dead_soh: float = DEAD_SOH) -> float:
    """Total trapped capital across all stores (pure)."""
    return round(sum(trapped_capital(ps, dead_ads, dead_soh)
                     for ps in (products_by_org or {}).values()), 2)


def compute_recovery(current_trapped: float, prior_target: float) -> tuple:
    """Pure high-water-mark recovery math → (target, recovered).

    target = max(prior_target, current_trapped); recovered = target - current.
    """
    current = max(0.0, float(current_trapped or 0))
    target = max(float(prior_target or 0), current)
    recovered = round(max(0.0, target - current), 2)
    return round(target, 2), recovered


def update_journey_recovery(current_trapped: float, path: str = None) -> dict:
    """Fold a fresh trapped-capital reading into the persisted journey state."""
    from . import journey_state as JS
    state = JS.load_state(path)
    target, recovered = compute_recovery(current_trapped, state.get("value_target"))
    return JS.set_value_recovered(recovered, target=target, path=path)
