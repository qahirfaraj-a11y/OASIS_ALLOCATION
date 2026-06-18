"""Tests for the risk feature builder (Risk Redesign P2)."""

import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.risk_features import (
    demand_stats, daily_demand_sigma, supply_stats,
    leadtime_demand_distribution, feature_row,
)


class TestDemandStats:
    def test_steady_demand(self):
        d = demand_stats({"Jan": 30, "Feb": 30, "Mar": 30}, total_months=12)
        assert d["mean_monthly"] == 30.0
        assert d["cv"] == 0.0
        assert d["months_active"] == 3
        # intermittency over a 12-month window: 3 active
        assert d["intermittency"] == round(1 - 3 / 12, 4)
        assert abs(d["ads"] - 90 / (3 * 30.4)) < 1e-4  # ads rounded to 5dp

    def test_volatile_has_cv(self):
        d = demand_stats({"Jan": 10, "Feb": 0, "Mar": 50})
        assert d["cv"] > 0
        assert d["months_active"] == 2

    def test_empty(self):
        d = demand_stats({})
        assert d["ads"] == 0.0 and d["intermittency"] == 1.0

    def test_net_negative_series_clamps_ads(self):
        # returns exceeded sales -> negative total -> ADS clamped at 0
        d = demand_stats({"Jan": 5, "Feb": -20})
        assert d["ads"] == 0.0


class TestDailyDemandSigma:
    def test_poisson_floor_for_slow_mover(self):
        # ads=4, cv=0 -> cv-based 0, poisson sqrt(4)=2 dominates
        assert daily_demand_sigma(4.0, 0.0) == 2.0

    def test_cv_dominates_for_volatile(self):
        # ads=100, cv=0.5 -> 50 vs sqrt(100)=10 -> 50
        assert daily_demand_sigma(100.0, 0.5) == 50.0


class TestSupplyStats:
    def test_extracts_and_defaults(self):
        s = supply_stats({"estimated_delivery_days": 2, "lata_stdev_days": 0.56,
                          "lata_avg_fulfillment_rate": 0.9, "reliability_score": 25})
        assert s["lead_time_days"] == 2.0
        assert s["lead_time_sd"] == 0.56
        assert s["fulfillment_rate"] == 0.9
        assert s["reliability"] == 0.25

    def test_missing_pattern_uses_defaults(self):
        s = supply_stats(None)
        assert s["lead_time_days"] == 3.0 and s["lead_time_sd"] == 0.0
        assert s["fulfillment_rate"] == 1.0


class TestLeadtimeDemandDistribution:
    def test_deterministic_lead_time(self):
        # ads=10, sigma_d=2, L=4, sL=0 -> mu=40, sigma=sqrt(4*4)=4
        mu, sd = leadtime_demand_distribution(10, 2, 4, 0)
        assert mu == 40.0
        assert abs(sd - 4.0) < 1e-9

    def test_lead_time_variance_adds(self):
        # ads=10, sigma_d=2, L=4, sL=1 -> var = 4*4 + 100*1 = 116
        mu, sd = leadtime_demand_distribution(10, 2, 4, 1)
        assert mu == 40.0
        assert abs(sd - math.sqrt(116)) < 1e-9

    def test_zero_demand(self):
        mu, sd = leadtime_demand_distribution(0, 0, 5, 1)
        assert mu == 0.0 and sd == 0.0


class TestFeatureRow:
    def test_assembles_and_computes_cover(self):
        row = feature_row(
            name="TEST SKU", soh=40,
            monthly_sales={"Jan": 30, "Feb": 30, "Mar": 30},
            pattern={"estimated_delivery_days": 4, "lata_stdev_days": 0.0,
                     "reliability_score": 80, "lata_avg_fulfillment_rate": 1.0},
            impact={"sales_rank": 12, "revenue": 5000, "margin_pct": 20},
        )
        assert row["name"] == "TEST SKU"
        assert row["lead_time_days"] == 4.0
        assert row["mu_ltd"] > 0
        # cover vs lead-time demand is finite and positive
        assert row["cover_vs_ltd"] > 0
        assert row["sales_rank"] == 12 and row["revenue"] == 5000.0

    def test_dead_sku_infinite_cover(self):
        row = feature_row("DEAD", soh=50, monthly_sales={}, pattern=None)
        assert row["ads"] == 0.0
        assert row["days_of_cover"] == float("inf")
        assert row["cover_vs_ltd"] == float("inf")
