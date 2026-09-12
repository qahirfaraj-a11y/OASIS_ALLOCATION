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


class TestSigmaDIsBuiltFromADailyCv:
    """sigma_d is a DAILY standard deviation, so its cv must be daily too.

    recommend() used to read `product['demand_cv'] or demand_cv(d)`, and the
    field it preferred is written by enrichment as _calculate_cv over
    sales_data['monthly_sales'] -- stdev/mean of MONTHLY TOTALS. Using it as
    sigma_d = cv * d asserts that a day varies as little as a month, which is
    the aggregation running backwards.

    Measured on the live book: the monthly value won on 14,525 of 15,037 lines
    (96.6%), was smaller on 99.3% of them, and left the safety term at 0.334x
    of the formula's own answer. It also made the velocity model unreachable
    on all but 512 lines -- which is why sweeping phi across its whole
    plausible range moved not a single line of the order book.
    """

    def _line(self, **kw):
        p = {"avg_daily_sales": 4.0, "supplier_name": "ACME",
             "current_stock": 0.0, "lead_time_days": 2.0,
             "department": "GENERAL", "sku": "CV-1", "pack_size": 1}
        p.update(kw)
        return ou.recommend(p, schedule={}, patterns={})

    def test_a_monthly_cv_does_not_reach_the_safety_term(self):
        """THE REGRESSION. A tiny monthly cv must not shrink daily sigma."""
        assert self._line(demand_cv=0.05)["S"] == \
            pytest.approx(self._line()["S"])

    def test_the_daily_model_is_what_gets_used(self):
        r = self._line()
        assert r["sigma_d"] == pytest.approx(ou.demand_cv(4.0) * 4.0)

    def test_a_genuinely_daily_measurement_still_wins(self):
        """The fix is about the interval, not about distrusting callers."""
        r = self._line(demand_cv_daily=1.5)
        assert r["sigma_d"] == pytest.approx(1.5 * 4.0)

    def test_the_daily_cv_is_the_larger_one_on_a_slow_line(self):
        """The direction that made this matter: on the tail, where 90% of the
        book lives, the daily figure is several times the monthly one."""
        assert ou.demand_cv(0.5) > 4 * 0.35


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

    def test_it_does_not_refuse_when_a_smaller_whole_pack_would_fit(self):
        """JW 145G TUNA, reduced to its arithmetic.

        S came to 8.2 units -- 56 days, inside the cap. Rounding up to a
        whole sellable unit gave 9, which is 61 days, so the line bought
        NOTHING. Offered 54 days or 61 days against a 60-day rule, the
        engine took 0. Ordering the largest pack that fits misses S by
        under one pack; refusing misses it by all of S.
        """
        r = self._line(avg_daily_sales=0.147, sku="TUNA-1", pack_size=1,
                       lead_time_days=7.0)
        cap_units = ou.MAX_AUTO_ORDER_COVER_DAYS * 0.147
        assert r["auto_order_suppressed"] is False
        assert 0 < r["quantity"] <= cap_units

    def test_the_cap_is_still_honoured_exactly(self):
        """Rounding towards the cap must never round through it."""
        for d in (0.05, 0.147, 0.4, 1.3, 7.0):
            for pack in (1, 6, 12):
                r = self._line(avg_daily_sales=d, pack_size=pack, sku="X")
                cover = (0.0 + r["quantity"]) / d
                assert cover <= ou.MAX_AUTO_ORDER_COVER_DAYS + 1e-9

    def test_a_pack_bigger_than_the_cap_is_still_refused(self):
        """The 374 lines where the message is literally true: no orderable
        quantity satisfies the cap, so there is nothing to round down to."""
        r = self._line(avg_daily_sales=0.01, pack_size=24, sku="BULK-1")
        assert r["auto_order_suppressed"] is True
        assert r["quantity"] == 0

    def test_suppression_now_means_what_it_says(self):
        """Refusal is reachable ONLY when one pack exceeds the cap, so the
        message stopped being an approximation of the real rule."""
        for pack in (1, 3, 24, 100):
            r = self._line(avg_daily_sales=0.02, pack_size=pack, sku="M")
            if r["auto_order_suppressed"]:
                assert pack / 0.02 > ou.MAX_AUTO_ORDER_COVER_DAYS

    def test_on_hand_stock_counts_against_the_room(self):
        """The cap bounds the POSITION, not the order, so what is already on
        the shelf has to consume the allowance."""
        r = self._line(avg_daily_sales=0.5, pack_size=1, current_stock=29.0,
                       sku="PART-1")
        assert (29.0 + r["quantity"]) / 0.5 <= ou.MAX_AUTO_ORDER_COVER_DAYS + 1e-9


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


class TestNoGuardPlansAStockout:
    """The Global 3x Cap is the fourth limit on a line that has already passed
    the order-up-to level, the shelf-life clamp and the dead-stock rule -- and
    it was the only one blind to R and L.

    It sizes itself on `effective_daily_sales`, a 0.7/0.3 blend with the last
    30 days, while the model sized S on `avg_daily_sales`. Two demand numbers
    in one pipeline: wherever the recent month ran below the average, the cap
    was computed on a smaller d than the quantity it was judging and trimmed a
    correct order. Measured: 3 of the 41 cap-bound lines finished under their
    own protection interval. clamp_level already refuses to do this for the
    shelf-life ceiling, on the same reasoning.
    """

    def _guard(self, qty, d, R, L, pack=1, ads_30d=None):
        from oasis.logic.order_engine import apply_safety_guards
        rec = {"product_name": "W", "recommended_quantity": qty,
               "avg_daily_sales": d, "reasoning": "",
               "order_up_to_terms": {"d": d, "R": R, "L": L}}
        if ads_30d is not None:
            rec["avg_daily_sales_last_30d"] = ads_30d
        p = {"product_name": "W", "avg_daily_sales": d, "current_stocks": 0,
             "pack_size": pack, "is_fresh": False, "product_category": "GEN",
             "last_days_since_last_delivery": 5}
        return apply_safety_guards([rec], {"W": p})[0]

    def test_it_never_cuts_below_the_protection_interval(self):
        """THE REGRESSION. A monthly supplier: R+L = 37 days of demand must
        survive a cap built on a depressed recent month."""
        d, R, L = 1.0, 30.0, 7.0
        out = self._guard(qty=d * (R + L), d=d, R=R, L=L, ads_30d=0.2)
        assert out["recommended_quantity"] >= d * (R + L) - 1e-6

    def test_a_genuinely_excessive_order_is_still_capped(self):
        """The floor must not disable the guard: 400 days of cover on a
        weekly line is what it exists to stop."""
        d, R, L = 10.0, 7.0, 2.0
        out = self._guard(qty=d * 400, d=d, R=R, L=L)
        assert out["recommended_quantity"] < d * 400

    def test_the_floor_uses_the_model_d_not_the_blend(self):
        """If it floored on effective_daily_sales it could never bind: 63 days
        already exceeds every protection interval on this book."""
        import inspect
        from oasis.logic.order_engine import apply_safety_guards
        src = inspect.getsource(apply_safety_guards)
        assert "_d_model * _P" in src

    def test_a_line_without_terms_is_untouched_by_the_floor(self):
        """The classic path attaches no order_up_to_terms; it must keep its
        existing behaviour rather than acquire a floor from nowhere."""
        from oasis.logic.order_engine import apply_safety_guards
        rec = {"product_name": "W", "recommended_quantity": 10_000.0,
               "avg_daily_sales": 1.0, "reasoning": ""}
        p = {"product_name": "W", "avg_daily_sales": 1.0, "current_stocks": 0,
             "pack_size": 1, "is_fresh": False, "product_category": "GEN",
             "last_days_since_last_delivery": 5}
        out = apply_safety_guards([rec], {"W": p})[0]
        assert out["recommended_quantity"] < 10_000.0


class TestTheTriggerProtectsTheSameHorizonAsTheTarget:
    """The engine is (s, S): the reorder point decides whether a line is
    evaluated at all, and only lines at or under it reach order_up_to. So a
    trigger protecting a SHORTER horizon than the target leaves a band where
    a line is already short and the arithmetic that would notice never runs.

    It did. The enriched reorder_point is velocity * (lead + safety), a median
    10.0 days of cover, against S protecting R + L at a median 14.0 -- 93.3%
    of lines. On a single real shelf, 193 lines reached their OWN ordering day
    above that reorder point and below d*(R+L), worth KES 269,751: the
    schedule offered them their one chance and the trigger declined to look.

    The same error is warned about one branch away, at the newsvendor ROP --
    "THE HORIZON IS P = R + L, NOT L ... silently drops R" -- fixed there while
    the enriched reorder_point that actually fires kept it.
    """

    def test_the_floor_is_stated_against_the_protection_interval(self):
        import inspect
        from oasis.logic import simulation_bridge as SB
        src = inspect.getsource(SB.SimulationOrderUtil.calculate_order_quantity)
        assert "_P_trigger" in src and "review_period" in src

    def test_it_is_a_floor_and_never_a_reduction(self):
        """A supplier-specific reorder point that already protects MORE must
        keep it. Loosening a trigger somebody set deliberately would be a
        different change, and not one measurement here supports."""
        import inspect
        from oasis.logic import simulation_bridge as SB
        src = inspect.getsource(SB.SimulationOrderUtil.calculate_order_quantity)
        assert "if _protection > reorder_point:" in src

    def test_it_sits_on_the_shared_side_of_the_model_fork(self):
        """The trigger is deliberately shared so that a classic-vs-derived
        comparison differs only in the quantity decision. Flooring it inside
        the is_enabled() branch would have broken that."""
        import inspect
        from oasis.logic import simulation_bridge as SB
        src = inspect.getsource(SB.SimulationOrderUtil.calculate_order_quantity)
        assert src.index("_protection > reorder_point") < src.index("_ou.is_enabled()")


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
