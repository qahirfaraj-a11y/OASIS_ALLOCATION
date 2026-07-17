"""Unit tests for the hub-native supplier Overview analytics (portal P0)."""

import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis_hub.analytics import compute_overview
from oasis_hub.visibility import VisibleMovement

REF = datetime(2026, 7, 14, 12, 0, 0)


def _mv(sku, mtype, qty, days_ago, on_hand=None, store="Store A"):
    return VisibleMovement(
        movement_id=f"{sku}-{mtype}-{days_ago}-{store}",
        store_handle=store, store_masked=False, city="Nairobi",
        sku_code=sku, sku_name=f"{sku} name", department="Beverages", brand="B",
        movement_type=mtype, qty=qty, unit_price=50.0, on_hand=on_hand,
        occurred_at=REF - timedelta(days=days_ago),
    )


def test_empty_feed_is_safe():
    o = compute_overview([])
    assert o["kpis"]["skus"] == 0 and o["top_movers"] == [] and o["stockout_risk"] == []


def test_velocity_ads():
    # 28 units of COKE over the 28-day window → ADS 1.0/day
    movs = [_mv("COKE", "sale", 2, d) for d in range(0, 28, 2)]  # 14 sales × 2 = 28
    o = compute_overview(movs, as_of=REF, window_days=28)
    mover = next(m for m in o["top_movers"] if m["sku_code"] == "COKE")
    assert mover["units"] == 28
    assert mover["ads"] == 1.0


def test_trend_recent_vs_prior():
    # prior 7d: 10 units; recent 7d: 20 units → +100%
    movs = [_mv("X", "sale", 10, 10), _mv("X", "sale", 20, 3)]
    o = compute_overview(movs, as_of=REF)
    m = next(m for m in o["top_movers"] if m["sku_code"] == "X")
    assert m["trend_pct"] == 100.0


def test_stockout_risk_flagged_when_cover_low():
    # sells 4/day (28 units over 7 days within a 28d window → ADS = 28/28 = 1.0/day),
    # on_hand 3 → 3 days cover ≤ 7 → risk
    sales = [_mv("MILK", "sale", 4, d) for d in range(0, 7)]   # 28 units
    snap = _mv("MILK", "stock_on_hand", 0, 0, on_hand=3)
    o = compute_overview(sales + [snap], as_of=REF, window_days=28, risk_days=7)
    assert o["kpis"]["at_risk"] == 1
    r = o["stockout_risk"][0]
    assert r["sku_code"] == "MILK" and r["on_hand"] == 3
    assert r["days_of_cover"] == 3.0


def test_no_onhand_means_no_false_risk():
    o = compute_overview([_mv("X", "sale", 5, 1)], as_of=REF)
    assert o["kpis"]["at_risk"] == 0 and o["stockout_risk"] == []


def test_healthy_cover_not_flagged():
    sales = [_mv("Y", "sale", 1, d) for d in range(0, 7)]      # ADS ~0.25/day
    snap = _mv("Y", "stock_on_hand", 0, 0, on_hand=100)         # ~400 days cover
    o = compute_overview(sales + [snap], as_of=REF, risk_days=7)
    assert o["kpis"]["at_risk"] == 0


def test_risk_sorted_most_urgent_first():
    movs = []
    for sku, oh in [("A", 2), ("B", 1), ("C", 3)]:
        movs += [_mv(sku, "sale", 2, d, store=sku) for d in range(0, 7)]  # ADS ~0.5
        movs.append(_mv(sku, "stock_on_hand", 0, 0, on_hand=oh, store=sku))
    o = compute_overview(movs, as_of=REF, risk_days=30)
    covers = [r["days_of_cover"] for r in o["stockout_risk"]]
    assert covers == sorted(covers)          # ascending urgency
