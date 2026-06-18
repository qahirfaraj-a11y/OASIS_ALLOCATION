"""Tests for the newsvendor risk baseline (Risk Redesign P3)."""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.risk_baseline import (
    norm_cdf, z_score, stockout_probability, safety_stock,
    reorder_point, baseline_recommendation, baseline_risk,
)


class TestDistributionPrimitives:
    def test_norm_cdf(self):
        assert abs(norm_cdf(0.0) - 0.5) < 1e-9
        assert abs(norm_cdf(1.96) - 0.975) < 1e-3

    def test_z_score(self):
        assert abs(z_score(0.5)) < 1e-6
        assert abs(z_score(0.975) - 1.95996) < 1e-3
        assert abs(z_score(0.95) - 1.64485) < 1e-3

    def test_z_score_monotonic(self):
        assert z_score(0.99) > z_score(0.95) > z_score(0.90)


class TestStockoutProbability:
    def test_no_demand_no_risk(self):
        assert stockout_probability(0, 0, 0) == 0.0

    def test_intermittent_poisson_tail(self):
        # mu=2 (intermittent), available 0 -> P(D>0) = 1 - e^-2 ≈ 0.8647
        p = stockout_probability(0, 2.0, 1.4)
        assert abs(p - (1 - math.exp(-2))) < 1e-6

    def test_intermittent_high_cover_low_risk(self):
        # mu=2, plenty on hand -> near 0
        assert stockout_probability(20, 2.0, 1.4) < 0.001

    def test_high_volume_normal(self):
        # mu=100 (>=20 -> normal), available == mu -> ~0.5
        assert abs(stockout_probability(100, 100, 10) - 0.5) < 1e-6

    def test_high_volume_well_stocked(self):
        # available = mu + 3sigma -> ~0.0013
        assert stockout_probability(130, 100, 10) < 0.01

    def test_zero_sigma_step(self):
        # high volume, zero variance -> step at mu
        assert stockout_probability(99, 100, 0) == 1.0
        assert stockout_probability(101, 100, 0) == 0.0


class TestSafetyStockAndReorder:
    def test_safety_stock_scales(self):
        # z(0.95)≈1.645 -> safety ≈ 1.645*10
        assert abs(safety_stock(10, 0.95) - 16.4485) < 1e-2

    def test_higher_service_more_stock(self):
        assert safety_stock(10, 0.99) > safety_stock(10, 0.95)

    def test_reorder_point_is_cycle_plus_safety(self):
        rop = reorder_point(40, 10, 0.95)
        assert abs(rop - (40 + safety_stock(10, 0.95))) < 1e-9


class TestBaselineRecommendation:
    def test_well_stocked_no_order(self):
        row = {"mu_ltd": 40, "sigma_ltd": 10, "soh": 200}
        rec = baseline_recommendation(row, service_level=0.95)
        assert rec["recommended_order"] == 0.0
        assert rec["stockout_prob"] < 0.01

    def test_low_stock_orders_up_to_reorder_point(self):
        row = {"mu_ltd": 40, "sigma_ltd": 10, "soh": 5}
        rec = baseline_recommendation(row, service_level=0.95)
        assert rec["recommended_order"] > 0
        # order brings available up to the reorder point
        assert abs((5 + rec["recommended_order"]) - rec["reorder_point"]) < 1e-6

    def test_on_order_reduces_need(self):
        row = {"mu_ltd": 40, "sigma_ltd": 10, "soh": 5}
        no_pipe = baseline_recommendation(row, on_order=0)["recommended_order"]
        with_pipe = baseline_recommendation(row, on_order=30)["recommended_order"]
        assert with_pipe < no_pipe

    def test_baseline_risk_helper(self):
        row = {"mu_ltd": 2.0, "sigma_ltd": 1.4, "soh": 0}
        assert abs(baseline_risk(row) - (1 - math.exp(-2))) < 1e-6
