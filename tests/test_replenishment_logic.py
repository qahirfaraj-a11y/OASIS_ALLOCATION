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


@pytest.fixture(autouse=True)
def _pin_the_classic_path(monkeypatch):
    """These are classic-path tests, so they must SAY so.

    calculate_order_quantity forks on order_up_to.is_enabled(), which reads
    OASIS_ORDER_MODEL and then the engines config — and the config it lands on
    is the developer's own tuned file, which is untracked machine state. So
    the result of this file depended on whose laptop ran it: the same commit
    passed here and failed in CI once the derived model became the default.
    A golden test that reads machine state is not golden.

    Pinned per-test rather than per-module because monkeypatch is
    function-scoped; a module-scoped autouse fixture cannot take it, and
    reaching for os.environ directly is how a leaked variable ends up
    steering a later file.
    """
    monkeypatch.setenv("OASIS_ORDER_MODEL", "classic")


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
    """F3: LATA deepens the safety buffer for an unreliable supplier.

    THE MECHANISM CHANGED; THE PROPERTY DID NOT.

    This class used to seed `lata_variance_multiplier` into a
    supplier_patterns_2025.json and assert a "[LATA Shield: x2.00" label. That
    multiplier is gone from the ordering path -- 4ba5523d removed the load
    because nothing read it, and simulation_bridge now says so in one line:
    `lata_multiplier = 1.0  # NEUTRALISED -- sigma_L enters via the
    quadrature`. LATA works through the supplier's MEASURED lead-time spread:

        safety_days = z * sqrt(P*cv^2 + sigma_L^2),   P = gap + lead

    Two further traps this class walked into, both pinned below.

    `default_patterns()` resolves from the REPO root and caches in a module
    global, so a tmp_path data_dir cannot reach it. The old
    `test_no_patterns_file_is_neutral` asserted that a missing file leaves the
    shield silent; it can never pass, because the repo's real file loads
    whatever the test writes.

    Comparing two suppliers only says something about DEPTH if both are
    allowed to order on the same day. With a 10-day gap the reliable supplier
    exits at the schedule gate and the comparison passes for the wrong reason.
    A daily supplier removes the calendar from the question.
    """

    @pytest.fixture()
    def lata_util(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        engine = OrderEngine(str(data_dir))
        util = SimulationOrderUtil(str(data_dir), engine=engine)
        # The attribute the ordering path consumes (simulation_bridge:432,
        # :661). Injected here rather than written to data_dir, which the
        # loader does not read -- see the class docstring.
        util._lead_patterns = {
            "FLAKY LTD": {"lead_time_stdev": 6.0, "lead_time_days": 2},
            "SOLID LTD": {"lead_time_stdev": 0.25, "lead_time_days": 2},
        }
        return util

    def test_unreliable_supplier_orders_deeper(self, lata_util):
        """gap 1 so every day is an ordering day: this is depth, not calendar.

        TWO THINGS HAVE TO BE TRUE FOR THIS TO MEASURE ANYTHING.

        These are classic-path tests (see _pin_the_classic_path), and on that
        path sigma_L reaches the quantity ONLY through the cycle floor
        `gap + lead + safety_buffer`. Against the fixture's default 14-day
        target neither supplier's floor binds, both take the target, and both
        order 90 -- the comparison passes or fails on nothing. target 3 puts
        the floor in charge, which is where the shield lives here.

        Measured: FLAKY sigma_L=6.00d -> safety 7.76d -> floor 10.76d;
        SOLID sigma_L=0.25d -> safety 1.15d -> floor 4.15d, under the 5 days
        already on the shelf, so it needs nothing.

        The derived path (OASIS_ORDER_MODEL=order_up_to, what production runs)
        sizes on sigma_L directly rather than through a floor: the same pair
        there is 120 units against 60.
        """
        flaky = _run(lata_util, _sku(supplier_name="FLAKY LTD",
                                     median_gap_days=1, target_coverage_days=3.0))
        solid = _run(lata_util, _sku(supplier_name="SOLID LTD",
                                     median_gap_days=1, target_coverage_days=3.0))
        assert flaky["recommended_quantity"] > solid["recommended_quantity"]
        assert "sigma_L=6.00d" in flaky["reasoning"]
        assert "sigma_L=0.25d" in solid["reasoning"]

    def test_the_shield_names_the_artefact_it_used(self, lata_util):
        """The label must say WHICH quantity it applied. "[LATA Shield]" alone
        reads as the retired multiplier; the number shown is sigma_L."""
        rec = _run(lata_util, _sku(supplier_name="FLAKY LTD", median_gap_days=1))
        assert "[LATA Shield: lead_time_stdev sigma_L=" in rec["reasoning"]

    def test_an_unmeasured_supplier_takes_the_chain_spread(self, lata_util):
        """Not silence -- the chain-wide sigma_L, derived as the p75 of every
        measured supplier. A supplier we cannot measure is not a supplier with
        no variance, and treating it as neutral is how the 2.22d constant used
        to reach every line unannounced."""
        rec = _run(lata_util, _sku(supplier_name="NEVER SEEN LTD",
                                   median_gap_days=1))
        assert "[LATA Shield: lead_time_stdev sigma_L=" in rec["reasoning"]
        assert "sigma_L=6.00d" not in rec["reasoning"]


class TestNewsvendorRop:
    """F4: OASIS_ROP_MODE=newsvendor replaces the flat fallback with μ+z·σ."""

    def _fresh_util(self, tmp_path):
        data_dir = tmp_path / "data"
        data_dir.mkdir(exist_ok=True)
        return SimulationOrderUtil(str(data_dir), engine=OrderEngine(str(data_dir)))

    # THE HORIZON MOVED, AND THESE TESTS WERE PINNING THE DEFECT.
    #
    # Both used to assert "Above ROP 24.7" -- mu + z*sigma computed over the
    # LEAD TIME ALONE (ADS 10 x LT 2 = 20, +z*2.83). That was the bug
    # simulation_bridge names a few lines above the newsvendor branch: "THE
    # HORIZON IS P = R + L, NOT L ... silently drops R". A line cannot reorder
    # between review cycles, so it must survive the review period too. With
    # R=7 and L=2 the horizon is 9 days, and the ROP is 99.9, not 24.7.
    #
    # The contract each test states still holds; only the numbers moved, and
    # the useful band moved with them. The measured ROPs are now:
    #
    #     heuristic,      no stored ROP    90.0   (the d*(R+L) floor)
    #     newsvendor,     no stored ROP    99.9   (mu_P + z*sigma_P)
    #     newsvendor,     stored 60        90.0   (stored kept, then floored)
    #     newsvendor-all, stored 60        99.9   (stored overridden)
    #
    # so stock 95 is the one place each pair disagrees, and a test that does
    # not sit in that band is asserting nothing about the gate.

    def test_newsvendor_rop_is_variance_aware(self, tmp_path, monkeypatch):
        """The newsvendor ROP carries a variance premium the flat floor does
        not: 99.9 against 90.0, the z*sigma_P term. At stock 95 that premium
        is the whole decision."""
        sku = _sku(reorder_point=0.0, current_stock=95)

        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor")
        nv = _run(self._fresh_util(tmp_path), dict(sku))
        assert nv["recommended_quantity"] > 0            # 95 <= 99.9

        monkeypatch.setenv("OASIS_ROP_MODE", "heuristic")
        heur = _run(self._fresh_util(tmp_path), dict(sku))
        assert heur["recommended_quantity"] == 0         # 95 > 90.0
        assert "Above ROP 90.0" in heur["reasoning"]

    def test_the_newsvendor_horizon_is_the_protection_interval(self, tmp_path,
                                                               monkeypatch):
        """Pins the horizon itself, because that is what was wrong. Over L
        alone this SKU's ROP is 24.7; over P = R + L it is 99.9.

        Two SKUs, because the two facts do not survive on the same line: the
        no-order branch ASSIGNS its reasoning rather than appending, so
        "[Above ROP 99.9]" is all that is left of a line that did not order.
        That is the same overwrite the old comment here flagged, and it is why
        the horizon has to be read off a line that DID order.
        """
        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor")
        above = _run(self._fresh_util(tmp_path),
                     _sku(reorder_point=0.0, current_stock=200))
        assert above["recommended_quantity"] == 0
        assert "Above ROP 99.9" in above["reasoning"]

        ordered = _run(self._fresh_util(tmp_path),
                       _sku(reorder_point=0.0, current_stock=30))
        assert ordered["recommended_quantity"] > 0
        assert "[ROP Newsvendor: SL95% P=9.0d]" in ordered["reasoning"]

    def test_stored_rop_respected_unless_all_mode(self, tmp_path, monkeypatch):
        """A stored ROP stays authoritative in 'newsvendor' -- floored at the
        protection interval, never reduced -- and only 'newsvendor-all'
        replaces it. Stock 95 is above the floored 90 and below the
        recomputed 99.9, so the two modes decide differently."""
        sku = _sku(reorder_point=60.0, current_stock=95)

        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor")
        rec = _run(self._fresh_util(tmp_path), dict(sku))
        assert rec["recommended_quantity"] == 0
        assert "Above ROP 90.0" in rec["reasoning"]

        monkeypatch.setenv("OASIS_ROP_MODE", "newsvendor-all")
        rec2 = _run(self._fresh_util(tmp_path), dict(sku))
        assert rec2["recommended_quantity"] > 0

    def test_the_trigger_floors_at_the_protection_interval(self, util):
        """This test used to assert the defect, and is the clearest statement
        of it available.

        It read: heuristic fallback ROP = 10*(2 + 1.5*1.4) = 38, stock 50 sits
        above it, so no order. But the line sells 10/day, so 50 units is FIVE
        days of cover — against a protection interval of nine, a 7-day review
        period plus a 2-day lead. It empties on day five and cannot be
        restocked until day nine. "No order" was a four-day stockout written
        down as correct behaviour.

        The reorder point is now floored at d*(R+L), so the trigger protects
        the same horizon as the target it gates. Measured on the real book
        before the floor: 193 lines reached their own ordering day above the
        reorder point and below d*(R+L), worth KES 269,751 on one day's shelf.
        """
        rec = _run(util, _sku(reorder_point=0.0))
        assert rec["recommended_quantity"] > 0
        assert "floored at the protection interval" in rec["reasoning"]

    def test_a_line_genuinely_above_the_interval_still_does_not_order(self, util):
        """The floor must not turn the trigger into 'always order'. 200 units
        is 20 days of cover against a 9-day interval, and stays untouched."""
        rec = _run(util, _sku(reorder_point=0.0, current_stock=200))
        assert rec["recommended_quantity"] == 0


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
        """target 3d loses to the cycle floor gap7 + lead2 + safety.

        The safety term is no longer the ad-hoc 1.5*(1 + 2*cv) = 2.1 this
        pinned. simulation_bridge now takes it from the engine's own
        derivation, for the reason recorded there: the old form divided
        sigma_L by the lead time, which makes a supplier's lead-time exposure
        SHRINK as its lead time grows -- backwards, since L=1+/-1d and
        L=10+/-1d carry the same one day of demand at risk.

            safety = z * sqrt(P*cv^2 + sigma_L^2),  P = gap + lead = 9

        z = 1.28 (service level 0.90) and sigma_L = 1.93d, the chain-wide p75
        across every measured supplier -- which is why
        supplier_lead_patterns.json has to be tracked for this to reproduce.

            1.28 * sqrt(9*0.2^2 + 1.93^2) = 2.587  ->  floor 11.587d
            Net = 10*11.587 - 50 = 65.87
        """
        rec = _run(util, _sku(target_coverage_days=3.0))
        assert rec["recommended_quantity"] == pytest.approx(65.87, abs=0.01)

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
        """Five times the demand spread must buy a deeper floor.

        Same derivation as test_cycle_stock_floor_stretches_target, cv 0.2
        -> 1.0:  1.28 * sqrt(9*1.0^2 + 1.93^2) = 4.566  ->  floor 13.566d,
        Net = 10*13.566 - 50 = 85.66.

        Note what the quadrature does that the old 1.5*(1 + 2*cv) could not:
        at cv 0.2 the supplier's 1.93d lead-time spread DOMINATES the demand
        term (3.72 against 0.36 under the root), and at cv 1.0 demand takes
        over (9.00 against 3.72). One derivation covers both regimes; the
        linear heuristic charged them as if only demand existed.
        """
        rec = _run(util, _sku(target_coverage_days=3.0, demand_cv=1.0))
        assert rec["recommended_quantity"] == pytest.approx(85.66, abs=0.01)
        base = _run(util, _sku(target_coverage_days=3.0, demand_cv=0.2))
        assert rec["recommended_quantity"] > base["recommended_quantity"]


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
