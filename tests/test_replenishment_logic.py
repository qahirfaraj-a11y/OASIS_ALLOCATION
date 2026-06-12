"""Golden tests for SimulationOrderUtil.calculate_order_quantity() —
the deterministic replenishment brain behind the Smart Ordering tab.

Uses current_day=1 / use_real_date=False so the supplier-schedule check is
deterministic (day 1 is always an ordering day in the fallback heuristic).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.order_engine import OrderEngine
from oasis.logic.simulation_bridge import SimulationOrderUtil


@pytest.fixture(scope="module")
def util(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("data")
    engine = OrderEngine(str(data_dir))
    return SimulationOrderUtil(str(data_dir), engine=engine)


def _sku(**kw):
    base = {
        "product_name": "TEST PRODUCT",
        "supplier_name": "ACME SUPPLIES",
        "median_gap_days": 7,
        "lead_time_days": 2,
        "demand_cv": 0.2,
        "is_fresh": False,
        "days_since_delivery": 10,
        "total_units_sold_last_90d": 500,
        "current_stock": 50,
        "on_order_qty": 0,
        "avg_daily_sales": 10.0,
        "reorder_point": 60.0,
        "target_coverage_days": 14.0,
        "sales_rank": 999,
    }
    base.update(kw)
    return base


def _run(util, sku, **kw):
    recs = util.calculate_order_quantity([sku], current_day=1,
                                         use_real_date=False, **kw)
    assert len(recs) == 1
    return recs[0]


class TestNetRequirement:
    def test_standard_reorder(self, util):
        # Below ROP (50 ≤ 60). Dry target = max(14, gap7 + lead2 + buffer)
        # buffer = 1.5×(1 + 2×0.2) = 2.1 → 11.1 → target days stays 14.
        # Net = 10×14 − 50 − 0 = 90.
        rec = _run(util, _sku())
        assert rec["recommended_quantity"] == pytest.approx(90.0)
        assert "Net Req" in rec["reasoning"]

    def test_on_order_subtracted(self, util):
        rec = _run(util, _sku(on_order_qty=30))
        assert rec["recommended_quantity"] == pytest.approx(60.0)

    def test_above_rop_no_order(self, util):
        rec = _run(util, _sku(current_stock=200))
        assert rec["recommended_quantity"] == 0
        assert "Above ROP" in rec["reasoning"]

    def test_key_sku_boost(self, util):
        rec = _run(util, _sku(sales_rank=100))
        assert rec["recommended_quantity"] == pytest.approx(90.0 * 1.20)
        assert "Key SKU Boost" in rec["reasoning"]

    def test_cycle_stock_floor_stretches_target(self, util):
        # target_coverage_days=3 but gap7+lead2+buffer2.1=11.1 floor applies:
        # Net = 10×11.1 − 50 = 61.
        rec = _run(util, _sku(target_coverage_days=3.0))
        assert rec["recommended_quantity"] == pytest.approx(61.0)

    def test_rop_fallback_when_missing(self, util):
        # reorder_point=0 + ADS>0 → fallback ROP = 10×(2 + 1.5×1.4) = 41.
        # Stock 30 ≤ 41 → triggers with fallback note.
        rec = _run(util, _sku(reorder_point=0.0, current_stock=30))
        assert "[ROP Fallback" in rec["reasoning"]
        assert rec["recommended_quantity"] > 0


class TestBlockingRules:
    def test_dry_dead_stock_blocked(self, util):
        rec = _run(util, _sku(days_since_delivery=250,
                              total_units_sold_last_90d=0,
                              current_stock=10))
        assert rec["recommended_quantity"] == 0
        assert "Dead Stock" in rec["reasoning"]

    def test_stale_fresh_blocked(self, util):
        rec = _run(util, _sku(is_fresh=True, days_since_delivery=150,
                              total_units_sold_last_90d=0,
                              current_stock=10))
        assert rec["recommended_quantity"] == 0
        assert "Stale Fresh" in rec["reasoning"]

    def test_aged_but_selling_item_not_blocked(self, util):
        rec = _run(util, _sku(days_since_delivery=250,
                              total_units_sold_last_90d=400))
        assert rec["recommended_quantity"] > 0


class TestRiskBuffering:
    def test_gnn_risk_inflates_safety(self, util):
        rec = _run(util, _sku(), gnn_risk_score=0.9)
        assert "[GNN Risk Burst" in rec["reasoning"]

    def test_low_risk_no_burst(self, util):
        rec = _run(util, _sku(), gnn_risk_score=0.3)
        assert "[GNN Risk Burst" not in rec["reasoning"]

    def test_volatile_demand_raises_cycle_floor(self, util):
        # cv=1.0 → buffer = 1.5×3 = 4.5 → floor = 7+2+4.5 = 13.5 (< 14
        # target, so qty unchanged) — but with target 3 the floor binds:
        # Net = 10×13.5 − 50 = 85.
        rec = _run(util, _sku(target_coverage_days=3.0, demand_cv=1.0))
        assert rec["recommended_quantity"] == pytest.approx(85.0)
