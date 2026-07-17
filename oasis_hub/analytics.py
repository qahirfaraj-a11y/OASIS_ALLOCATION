"""
Supplier-facing analytics computed purely from the movement stream.

P0 of the supplier-portal plan (SUPPLIER_PORTAL_PLAN.md §8): turn the raw
`hub_stock_movement` feed — which the hub already receives — into the signals a
supplier actually logs in for, with ZERO on-premise coupling:

  * velocity (ADS) per SKU,
  * 7-day-vs-prior trend,
  * latest on-hand and days-of-cover per (SKU, store),
  * stockout-risk alerts (cover below a threshold).

Everything here is a pure function over a list of ``VisibleMovement`` (already
ownership+consent gated and identity-masked by ``visibility``), so it is
unit-testable without a database and the privacy contract is enforced upstream.
"""

from datetime import datetime, timedelta
from typing import List, Optional

# defaults (tunable per call)
WINDOW_DAYS = 28          # analysis window for velocity/cover
TREND_DAYS = 7            # recent vs prior comparison span
RISK_DAYS = 7.0          # days-of-cover below this → stockout-risk alert


def _reference_time(movements) -> Optional[datetime]:
    """Anchor 'now' to the latest movement so historical demo data still works
    and results are deterministic for tests."""
    times = [m.occurred_at for m in movements if m.occurred_at is not None]
    return max(times) if times else None


def compute_overview(
    movements: List,
    *,
    as_of: Optional[datetime] = None,
    window_days: int = WINDOW_DAYS,
    trend_days: int = TREND_DAYS,
    risk_days: float = RISK_DAYS,
) -> dict:
    """Roll a supplier's visible movements into the Overview payload.

    Returns {window_days, kpis, top_movers, stockout_risk}. Robust to missing
    on-hand (days-of-cover simply reports None and raises no false alert) and to
    an empty feed.
    """
    ref = as_of or _reference_time(movements)
    if ref is None:
        return {"window_days": window_days,
                "kpis": {"skus": 0, "total_units": 0.0, "avg_daily_units": 0.0,
                         "stores": 0, "at_risk": 0},
                "top_movers": [], "stockout_risk": []}

    window_start = ref - timedelta(days=window_days)
    recent_start = ref - timedelta(days=trend_days)
    prior_start = ref - timedelta(days=2 * trend_days)

    sales = [m for m in movements
             if m.movement_type == "sale" and m.occurred_at is not None]

    # ── per-SKU velocity + trend ─────────────────────────────────────────
    per_sku: dict = {}
    for m in sales:
        if m.occurred_at < window_start:
            continue
        d = per_sku.setdefault(m.sku_code, {
            "sku_code": m.sku_code, "sku_name": m.sku_name,
            "department": m.department, "units": 0.0,
            "recent": 0.0, "prior": 0.0,
        })
        qty = m.qty or 0.0
        d["units"] += qty
        if m.occurred_at >= recent_start:
            d["recent"] += qty
        elif m.occurred_at >= prior_start:
            d["prior"] += qty

    for d in per_sku.values():
        d["ads"] = round(d["units"] / window_days, 3)
        if d["prior"] > 0:
            d["trend_pct"] = round((d["recent"] - d["prior"]) / d["prior"] * 100, 1)
        elif d["recent"] > 0:
            d["trend_pct"] = None          # new/no prior baseline → "new"
        else:
            d["trend_pct"] = 0.0

    # ── latest on-hand per (SKU, store) ──────────────────────────────────
    latest_oh: dict = {}
    for m in movements:
        if m.movement_type != "stock_on_hand" or m.on_hand is None:
            continue
        key = (m.sku_code, m.store_handle)
        cur = latest_oh.get(key)
        if cur is None or m.occurred_at > cur["at"]:
            latest_oh[key] = {"on_hand": m.on_hand, "at": m.occurred_at,
                              "store_handle": m.store_handle,
                              "store_masked": m.store_masked}

    # ── per-store ADS (for days-of-cover) ────────────────────────────────
    store_sku_units: dict = {}
    for m in sales:
        if m.occurred_at < window_start:
            continue
        k = (m.sku_code, m.store_handle)
        store_sku_units[k] = store_sku_units.get(k, 0.0) + (m.qty or 0.0)

    stockout_risk = []
    for (sku, store), oh in latest_oh.items():
        units = store_sku_units.get((sku, store), 0.0)
        ads = units / window_days
        cover = round(oh["on_hand"] / ads, 1) if ads > 0 else None
        if cover is not None and cover <= risk_days:
            info = per_sku.get(sku, {})
            stockout_risk.append({
                "sku_code": sku,
                "sku_name": info.get("sku_name"),
                "store_handle": store,
                "store_masked": oh["store_masked"],
                "on_hand": oh["on_hand"],
                "ads": round(ads, 3),
                "days_of_cover": cover,
            })
    stockout_risk.sort(key=lambda r: r["days_of_cover"])

    top_movers = sorted(per_sku.values(), key=lambda d: d["ads"], reverse=True)

    total_units = sum(d["units"] for d in per_sku.values())
    stores = {m.store_handle for m in sales if m.occurred_at >= window_start}
    kpis = {
        "skus": len(per_sku),
        "total_units": round(total_units, 1),
        "avg_daily_units": round(total_units / window_days, 2),
        "stores": len(stores),
        "at_risk": len(stockout_risk),
    }
    return {"window_days": window_days, "kpis": kpis,
            "top_movers": top_movers, "stockout_risk": stockout_risk}
