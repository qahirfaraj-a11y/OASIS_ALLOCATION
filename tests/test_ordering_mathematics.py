"""The mathematics the recent ordering work introduced, pinned.

Eleven shipping files changed and one test file moved with them, so the cv
model, the dead-stock refusal and the cap-beats-floor rule all shipped
unpinned. Each of these encodes a property that was argued for from the
arithmetic, so a future change that quietly reverses one fails here rather
than in a client's order book.
"""

import importlib
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import order_up_to as ou


class TestTheDemandCv:
    """cv(d) = sqrt(1/d + phi^2): a Poisson term plus overdispersion."""

    def test_relative_variability_falls_as_velocity_rises(self):
        """THE CORRECTION, in one assertion.

        The velocity multiplier it replaced ran the other way: >10/day got
        x1.4 and <=1/day got x0.8. Counting arrivals in a window is Poisson to
        first order, so sigma = sqrt(d) and cv = 1/sqrt(d) -- a slow line is
        RELATIVELY more variable, not less. 13,553 of 15,037 SKUs on the live
        book sell a unit a day or less, so the old table had the sign wrong
        across 90% of the range.
        """
        cvs = [ou.demand_cv(d) for d in (0.2, 1.0, 5.0, 10.0, 60.0)]
        assert cvs == sorted(cvs, reverse=True)
        assert all(a > b for a, b in zip(cvs, cvs[1:])), cvs

    def test_a_slow_line_is_more_variable_than_a_fast_one(self):
        assert ou.demand_cv(0.2) > 2 * ou.demand_cv(60.0)

    def test_the_poisson_term_is_exactly_one_over_root_d(self):
        """phi = 0 leaves the parameter-free half, which is the half that
        needs no fitting and no data to justify."""
        for d in (0.25, 1.0, 4.0, 25.0):
            assert ou.demand_cv(d, phi=0.0) == pytest.approx(1.0 / math.sqrt(d))

    def test_overdispersion_is_the_floor_at_high_velocity(self):
        """A fast mover keeps the basket and payday effects: cv -> phi, never
        to zero. Zero would say a 60/day line has no demand risk at all."""
        assert ou.demand_cv(1e6, phi=0.4) == pytest.approx(0.4, abs=1e-3)
        assert ou.demand_cv(1e6, phi=0.4) > 0.39

    def test_the_default_phi_preserves_the_old_flat_assumption(self):
        """0.40 was the chain-wide flat cv. Defaulting to it means nothing
        changes for a fast mover on the day this shipped -- the change is
        confined to the tail, where the evidence is."""
        assert ou.DEMAND_OVERDISPERSION == pytest.approx(0.40)

    def test_it_is_capped(self):
        """An unbounded cv on a line selling 0.001/day is a dead line being
        described as a variable one, and it would size an order to match."""
        assert ou.demand_cv(1e-9) == ou.CV_CAP

    def test_zero_demand_does_not_divide_by_zero(self):
        """1/d is undefined at zero, so the function substitutes the d=1
        value rather than diverging. Any bounded choice does: sigma_d is
        cv * d, so at d = 0 the safety term is zero whatever the cv says.
        What matters is that it is finite and raises nothing -- asserting a
        specific number here would pin an arbitrary one.
        """
        for bad in (0.0, -5.0, None):
            v = ou.demand_cv(bad)
            assert math.isfinite(v)
            assert 0 < v <= ou.CV_CAP

    def test_phi_is_configurable_for_when_it_is_finally_fitted(self, monkeypatch):
        """The shape is confirmed on real data; the magnitude is not. This is
        the one dial to turn when daily till timestamps make it fittable."""
        monkeypatch.setenv("OASIS_DEMAND_OVERDISPERSION", "0.9")
        reloaded = importlib.reload(ou)
        try:
            assert reloaded.DEMAND_OVERDISPERSION == pytest.approx(0.9)
            assert reloaded.demand_cv(1e6) == pytest.approx(0.9, abs=1e-3)
        finally:
            monkeypatch.delenv("OASIS_DEMAND_OVERDISPERSION", raising=False)
            importlib.reload(ou)


class TestDeadStockDoesNotOrderItself:
    """A line where one pack is months of cover is a special-order decision."""

    def _line(self, **kw):
        p = {"avg_daily_sales": 0.01, "supplier_name": "ACME",
             "current_stock": 0.0, "lead_time_days": 2.0,
             "department": "GENERAL", "sku": "SLOW-1", "pack_size": 1}
        p.update(kw)
        return ou.recommend(p, schedule={}, patterns={})

    def test_it_refuses_rather_than_buying(self):
        r = self._line()
        assert r["quantity"] == 0
        assert r["auto_order_suppressed"] is True

    def test_it_says_why(self):
        """The refusal used to be indistinguishable from a healthy line that
        needed nothing, so the transfer module had to rediscover the
        population by accident from raw cover."""
        assert self._line()["suppress_reason"]

    def test_a_normal_line_is_not_suppressed(self):
        r = self._line(avg_daily_sales=10.0, sku="FAST-1")
        assert r["auto_order_suppressed"] is False
        assert r["quantity"] > 0

    def test_the_threshold_is_configurable(self):
        assert ou.MAX_AUTO_ORDER_COVER_DAYS > 0


class TestTheShelfLifeCapOutranksTheFloor:
    """Where a merchandising ceiling and the protection floor disagree, the
    ceiling wins and the line is reported infeasible -- the fix is a shorter
    lead time, not a deeper chiller."""

    def test_a_daily_fresh_line_is_held_to_its_ceiling(self):
        """d*(R+L) came to 2.28 days on the dairies against a 1.2-day life.
        The floor was overruling a spoilage limit, which orders stock that
        cannot be sold before it dies."""
        S = ou.clamp_level(1000.0, d=100.0, shelf_life_days=1.0,
                           min_protection=8.0)
        assert S <= 100.0 * ou.FRESH_COVER_CEILING_DAYS + 1e-9

    def test_a_long_life_line_keeps_its_buffer(self):
        """The ceiling is a fresh rule. A 60-day product must not be trimmed
        to a fresh cover just because the same function handles both."""
        S = ou.clamp_level(500.0, d=10.0, shelf_life_days=60.0,
                           min_protection=8.0)
        assert S == pytest.approx(500.0)

    def test_the_clamp_never_exceeds_the_shelf_life(self):
        S = ou.clamp_level(10_000.0, d=10.0, shelf_life_days=30.0)
        assert S <= 10.0 * 30.0 + 1e-9

    def test_a_facing_is_a_floor(self):
        assert ou.clamp_level(1.0, d=1.0, shelf_life_days=30.0,
                              min_display=25.0) == 25.0

    def test_no_shelf_life_means_no_ceiling(self):
        assert ou.clamp_level(900.0, d=10.0, shelf_life_days=0.0) == 900.0

    def test_a_broken_supply_term_is_capped_not_amplified(self):
        """When R fell back to the blanket 7 days, the d*P floor turned a
        1.2-day milk clamp into 8.28 days of milk -- amplifying the lookup
        failure instead of protecting service."""
        S = ou.clamp_level(0.0, d=10.0, shelf_life_days=2.0, min_protection=90.0)
        assert S <= 10.0 * 2.0 * ou.MAX_SHELF_MULTIPLE + 1e-9


class TestTheVelocityMultiplierIsGone:
    """Pinned as absent. It was wrong in direction AND in placement, and a
    'depth scaling' band table is the kind of thing that grows back."""

    def test_the_bands_are_not_in_the_enrichment_path(self):
        import inspect
        from oasis.logic import intelligence_mixin as IM
        src = inspect.getsource(IM)
        assert "velocity_multiplier" not in src

    def test_velocity_reaches_the_order_through_the_cv_instead(self):
        """The replacement is not 'nothing' -- it is the same effect applied
        to the safety term, where it belongs, rather than to cycle stock."""
        assert ou.demand_cv(0.5) > ou.demand_cv(50.0)
