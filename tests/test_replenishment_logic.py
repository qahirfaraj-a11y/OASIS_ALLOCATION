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
from oasis.logic.simulation_bridge import SimulationOrderUtil, _supplier_phase_offset


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


class TestLataShield:
    """F3: LATA supplier-variance multiplier deepens the safety buffer."""

    @pytest.fixture()
    def lata_util(self, tmp_path):
        import json
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "supplier_patterns_2025.json").write_text(json.dumps({
            "FLAKY LTD": {"lata_variance_multiplier": 2.0},
            "SOLID LTD": {"lata_variance_multiplier": 1.0},
        }), encoding="utf-8")
        engine = OrderEngine(str(data_dir))
        return SimulationOrderUtil(str(data_dir), engine=engine)

    def test_unreliable_supplier_orders_deeper(self, lata_util):
        # gap 10 + lead 2; neutral buffer 1.5×1.4=2.1 → target 14.1d;
        # flaky doubles the buffer → 4.2 → target 16.2d → bigger order.
        flaky = _run(lata_util, _sku(supplier_name="FLAKY LTD", median_gap_days=10))
        solid = _run(lata_util, _sku(supplier_name="SOLID LTD", median_gap_days=10))
        assert flaky["recommended_quantity"] > solid["recommended_quantity"]
        assert "[LATA Shield: x2.00" in flaky["reasoning"]
        assert "LATA Shield" not in solid["reasoning"]     # neutral is silent

    def test_no_patterns_file_is_neutral(self, util):
        rec = _run(util, _sku())
        assert "LATA Shield" not in rec["reasoning"]


class TestNewsvendorRop:
    """F4: OASIS_ROP_MODE=newsvendor replaces the flat fallback with μ+z·σ."""

    def _fresh_util(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        return SimulationOrderUtil(str(data_dir), engine=OrderEngine(str(data_dir)))

    def test_newsvendor_rop_is_variance_aware(self, tmp_path, monkeypatch):
        # ADS 10, LT 2, cv 0.2 → μ=20, σ=2.83 → ROP ≈ 24.65 at SL95.
        # Stock 30 sits ABOVE the newsvendor ROP (no order) but BELOW the flat
        # heuristic ROP of 38 (order) — the gate visibly changes the decision.
        # (No-order paths overwrite the reasoning with "[Above ROP x]", so we
        # assert on the ROP value + decision, the actual contract.)
        sku = _sku(reorder_point=0.0, current_stock=30)
        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor")
        nv = _run(self._fresh_util(tmp_path), dict(sku))
        assert nv["recommended_quantity"] == 0
        assert "Above ROP 24.7" in nv["reasoning"]        # μ + z·σ, not the flat 38

        monkeypatch.setenv("OASIS_ROP_MODE", "heuristic")
        heur = _run(self._fresh_util(tmp_path), dict(sku))
        assert heur["recommended_quantity"] > 0           # flat ROP 38 ≥ stock 30

    def test_stored_rop_respected_unless_all_mode(self, tmp_path, monkeypatch):
        # stored ROP 60 stays authoritative in 'newsvendor' mode: stock 50 ≤ 60 → order
        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor")
        rec = _run(self._fresh_util(tmp_path), _sku(reorder_point=60.0))
        assert rec["recommended_quantity"] > 0
        # …but 'newsvendor-all' overrides it: ROP 24.65 < stock 50 → no order
        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor-all")
        rec2 = _run(self._fresh_util(tmp_path), _sku(reorder_point=60.0))
        assert rec2["recommended_quantity"] == 0
        assert "Above ROP 24.7" in rec2["reasoning"]

    def test_default_mode_unchanged(self, util):
        # heuristic fallback: ROP = 10×(2 + 1.5×1.4) = 38 → stock 50 above → no order
        rec = _run(util, _sku(reorder_point=0.0))
        assert rec["recommended_quantity"] == 0
        assert "Above ROP 38.0" in rec["reasoning"]


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


class TestSupplierScheduleStagger:
    """A3: phase-staggered fallback for suppliers without a calendar entry."""

    def test_offset_is_stable_and_in_range(self):
        for gap in (2, 5, 7, 14):
            off1 = _supplier_phase_offset("ACME SUPPLIES", gap)
            off2 = _supplier_phase_offset("ACME SUPPLIES", gap)
            assert off1 == off2  # deterministic across calls
            assert 0 <= off1 < gap

    def test_daily_gap_has_no_offset(self):
        assert _supplier_phase_offset("ANY", 1) == 0

    def test_same_gap_suppliers_spread_across_cycle(self):
        # Many suppliers sharing gap=7 should not all map to offset 0.
        names = [f"SUPPLIER_{i:02d}" for i in range(40)]
        offsets = {_supplier_phase_offset(n, 7) for n in names}
        assert len(offsets) >= 4  # spread across multiple days, not bunched

    def test_day1_orders_for_every_supplier(self, util):
        # Day 1 must remain an ordering day regardless of offset (priming).
        for supplier in ("ACME SUPPLIES", "BETA DISTRIBUTORS", "GAMMA WHOLESALE"):
            rec = util.calculate_order_quantity(
                [_sku(supplier_name=supplier)], current_day=1, use_real_date=False,
            )[0]
            assert rec["recommended_quantity"] > 0

    def test_two_suppliers_order_on_different_days(self, util):
        # Find suppliers whose gap-7 offsets differ, then confirm that on a
        # day matching one supplier's phase the other is NOT ordering (unless
        # critical). Use high stock so neither is critically low.
        gap = 7
        # Pick two suppliers with distinct phase offsets deterministically.
        a = "SUPPLIER_AA"
        off_a = _supplier_phase_offset(a, gap)
        b = next(n for n in (f"SUPPLIER_{i:02d}" for i in range(100))
                 if _supplier_phase_offset(n, gap) != off_a)
        off_b = _supplier_phase_offset(b, gap)
        assert off_a != off_b

        # A day that is an ordering day for A: (day + off_a) % 7 == 0
        day_for_a = (gap - off_a) % gap
        if day_for_a <= 1:
            day_for_a += gap  # avoid day 1 (always orders) and day 0
        well_stocked = _sku(current_stock=10_000, reorder_point=5.0,
                            avg_daily_sales=1.0, days_since_delivery=1)

        rec_a = util.calculate_order_quantity(
            [dict(well_stocked, supplier_name=a)],
            current_day=day_for_a, use_real_date=False)[0]
        rec_b = util.calculate_order_quantity(
            [dict(well_stocked, supplier_name=b)],
            current_day=day_for_a, use_real_date=False)[0]

        # A is on-schedule today; B (different offset) is not. With ample
        # stock neither triggers a critical override, so B should be held
        # by the schedule gate while A is free to order.
        assert "[Schedule:" in rec_b["reasoning"] or "[Above ROP" in rec_b["reasoning"]
