"""The derived order quantity — every term separately checkable.

The whole reason for this form is that each part can be argued with on its own.
These tests pin the parts, not just the answer.
"""

import json
import math
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import order_up_to as ou


class TestTheFlag:

    def test_classic_is_the_default(self, monkeypatch):
        """A new model that switches itself on is a change nobody chose."""
        monkeypatch.delenv("OASIS_ORDER_MODEL", raising=False)
        assert ou.model_name() == "classic"
        assert not ou.is_enabled()

    def test_it_can_be_selected(self, monkeypatch):
        monkeypatch.setenv("OASIS_ORDER_MODEL", "order_up_to")
        assert ou.is_enabled()

    def test_an_unrecognised_model_stays_classic(self, monkeypatch):
        """Silently running an unknown model would be worse than ignoring it."""
        monkeypatch.setenv("OASIS_ORDER_MODEL", "experimental_v3")
        assert not ou.is_enabled()


class TestServiceLevel:

    def test_the_default_is_ninety_percent(self, monkeypatch):
        monkeypatch.delenv("OASIS_SERVICE_LEVEL", raising=False)
        assert ou.service_level() == 0.90
        assert ou.z_score() == pytest.approx(1.28)

    def test_z_rises_with_service(self):
        assert ou.z_score(0.50) < ou.z_score(0.90) < ou.z_score(0.99)

    def test_fifty_percent_buys_no_safety_stock(self):
        """The median is the median: half the time you run out."""
        assert ou.z_score(0.50) == 0.0

    def test_it_interpolates_between_tabulated_points(self):
        z = ou.z_score(0.925)
        assert ou.z_score(0.90) < z < ou.z_score(0.95)

    def test_nonsense_falls_back_rather_than_crashing(self, monkeypatch):
        monkeypatch.setenv("OASIS_SERVICE_LEVEL", "banana")
        assert ou.service_level() == 0.90
        monkeypatch.setenv("OASIS_SERVICE_LEVEL", "1.5")
        assert ou.service_level() == 0.90


class TestReviewPeriod:

    def _sched(self, tmp_path, mapping):
        p = tmp_path / ou.SCHEDULE_FILE
        p.write_text(json.dumps(mapping), encoding="utf-8")
        return str(tmp_path)

    def test_one_weekday_is_a_weekly_review(self, tmp_path):
        root = self._sched(tmp_path, {"MONDAY": ["SB0009 - BROOKSIDE DAIRY"]})
        s = ou.load_review_schedule(root)
        assert ou.review_period("BROOKSIDE DAIRY", s) == 7.0

    def test_two_weekdays_halve_it(self, tmp_path):
        root = self._sched(tmp_path, {"MONDAY": ["SX - ACME"], "THURSDAY": ["SX - ACME"]})
        s = ou.load_review_schedule(root)
        assert ou.review_period("ACME", s) == 3.5

    def test_the_supplier_code_prefix_is_stripped(self, tmp_path):
        root = self._sched(tmp_path, {"TUESDAY": ["SC0003 - COCA COLA BEVERAGES KENYA"]})
        s = ou.load_review_schedule(root)
        assert ou.review_period("Coca Cola Beverages Kenya", s) == 7.0

    def test_an_unscheduled_supplier_gets_the_chain_default(self, tmp_path):
        root = self._sched(tmp_path, {"MONDAY": ["SX - ACME"]})
        s = ou.load_review_schedule(root)
        assert ou.review_period("SOMEBODY ELSE", s) == ou.DEFAULT_REVIEW_DAYS

    def test_a_missing_schedule_is_not_fatal(self, tmp_path):
        assert ou.load_review_schedule(str(tmp_path)) == {}
        assert ou.review_period("ANYONE", {}) == ou.DEFAULT_REVIEW_DAYS


class TestSigmaLead:

    def test_a_measured_spread_is_used(self):
        assert ou.sigma_lead({"lead_time_stdev": 4.5}) == 4.5

    def test_an_unmeasured_supplier_gets_the_chain_figure_not_zero(self):
        """Zero would say 'this supplier is never late', which deletes the
        larger half of the safety term for every unmeasured vendor."""
        assert ou.sigma_lead(None) == ou.DEFAULT_SIGMA_LEAD
        assert ou.sigma_lead({}) == ou.DEFAULT_SIGMA_LEAD
        assert ou.sigma_lead({"lead_time_stdev": "not a number"}) == ou.DEFAULT_SIGMA_LEAD

    def test_a_genuinely_reliable_supplier_may_measure_zero(self):
        assert ou.sigma_lead({"lead_time_stdev": 0.0}) == 0.0


class TestTheFormula:

    def test_cycle_stock_is_demand_over_the_protection_interval(self):
        # d=10, R=7, L=3 -> P=10 -> 100 units, no safety at z=0
        S = ou.order_up_to_level(d=10, sigma_d=0, lead_days=3, review_days=7,
                                 sigma_lead_days=0, z=0.0)
        assert S == pytest.approx(100.0)

    def test_safety_grows_with_the_ROOT_of_the_horizon_not_linearly(self):
        """The classic path scales safety with the whole horizon. Quadrupling
        the interval should roughly double the demand-side buffer, not
        quadruple it."""
        a = ou.demand_sigma_over(9, d=0, sigma_d=2.0, sigma_lead_days=0)
        b = ou.demand_sigma_over(36, d=0, sigma_d=2.0, sigma_lead_days=0)
        assert b == pytest.approx(2 * a)

    def test_lead_time_variance_enters_scaled_by_the_demand_rate(self):
        """d^2·sigma_L^2 — a fast line on an erratic supplier needs far more
        cover than a slow one, for the same supplier."""
        slow = ou.demand_sigma_over(9, d=1.0, sigma_d=0, sigma_lead_days=2.0)
        fast = ou.demand_sigma_over(9, d=10.0, sigma_d=0, sigma_lead_days=2.0)
        assert fast == pytest.approx(10 * slow)

    def test_the_two_variance_terms_add_in_quadrature(self):
        both = ou.demand_sigma_over(9, d=10.0, sigma_d=2.0, sigma_lead_days=2.0)
        expected = math.sqrt(9 * 4 + 100 * 4)
        assert both == pytest.approx(expected)

    def test_the_omitted_term_is_the_larger_one_at_real_values(self):
        """R=7, L=2, cv=0.4, sigma_L=2.22 — the client's measured figures."""
        d = 10.0
        demand_only = ou.demand_sigma_over(9, d, 0.4 * d, 0.0)
        lead_only = ou.demand_sigma_over(9, d, 0.0, 2.22)
        assert lead_only > demand_only

    def test_a_line_that_does_not_sell_orders_nothing(self):
        assert ou.order_up_to_level(0, 1, 3, 7, 2, 1.28) == 0.0


class TestClamps:

    def test_shelf_life_caps_the_level(self):
        # 10/day, 5-day shelf life -> never hold more than 50
        assert ou.clamp_level(500, d=10, shelf_life_days=5) == 50.0

    def test_a_facing_minimum_raises_it(self):
        assert ou.clamp_level(2, d=1, min_display=12) == 12.0

    def test_no_limits_leaves_the_level_alone(self):
        assert ou.clamp_level(123.4, d=10) == 123.4

    def test_clamps_never_go_negative(self):
        assert ou.clamp_level(-5, d=10) == 0.0


class TestQuantity:

    def test_it_nets_off_stock_and_what_is_already_coming(self):
        assert ou.order_quantity(S=100, on_hand=30, on_order=20, pack_size=1) == 50

    def test_a_covered_position_orders_nothing(self):
        assert ou.order_quantity(S=100, on_hand=100, on_order=10) == 0.0

    def test_it_rounds_up_to_whole_packs(self):
        assert ou.order_quantity(S=100, on_hand=0, on_order=0, pack_size=12) == 108

    def test_a_zero_pack_size_does_not_divide_by_zero(self):
        assert ou.order_quantity(S=10, on_hand=0, on_order=0, pack_size=0) == 10


class TestRecommend:

    def _p(self, **kw):
        p = {"avg_daily_sales": 10.0, "demand_cv": 0.4, "lead_time_days": 2.0,
             "supplier_name": "ACME", "current_stock": 0.0, "on_order_qty": 0.0,
             "pack_size": 1.0}
        p.update(kw)
        return p

    def test_it_returns_the_terms_not_just_a_number(self):
        """A quantity nobody can decompose is a quantity nobody can argue
        with, which is the failure this whole form exists to fix."""
        t = ou.recommend(self._p())
        for term in ("R", "L", "P", "d", "sigma_d", "sigma_lead", "z",
                     "cycle_stock", "safety_stock", "S", "quantity"):
            assert term in t

    def test_cycle_plus_safety_reconstructs_the_level(self):
        t = ou.recommend(self._p())
        assert t["cycle_stock"] + t["safety_stock"] == pytest.approx(t["S_unclamped"])

    def test_a_line_with_no_sales_rate_is_refused_with_a_reason(self):
        t = ou.recommend(self._p(avg_daily_sales=0))
        assert t["quantity"] == 0.0
        assert "no measured sales rate" in t["reason"]

    def test_stock_already_held_reduces_the_order(self):
        empty = ou.recommend(self._p(current_stock=0))["quantity"]
        stocked = ou.recommend(self._p(current_stock=50))["quantity"]
        assert stocked == empty - 50

    def test_a_higher_service_level_orders_more(self):
        low = ou.recommend(self._p(), z=0.0)["quantity"]
        high = ou.recommend(self._p(), z=2.33)["quantity"]
        assert high > low

    def test_the_shelf_life_clamp_is_reported(self):
        t = ou.recommend(self._p(shelf_life_days=2))
        assert t["clamped"] is True

    def test_the_description_names_every_term(self):
        text = ou.describe(ou.recommend(self._p()))
        for word in ("Reviewed every", "survive", "variability", "lead-time"):
            assert word in text

    def test_no_order_says_so_plainly(self):
        text = ou.describe(ou.recommend(self._p(current_stock=10_000)))
        assert "No order" in text
