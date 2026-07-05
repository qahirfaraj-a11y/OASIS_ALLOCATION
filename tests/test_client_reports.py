"""Tests for the client-facing reports: Day-0 assessment + supplier scorecard."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.day0_assessment import assess
from oasis.logic.supplier_scorecard import (
    classify, scorecard_rows, scorecard_summary, write_scorecard,
)


def _catalog():
    return [
        {"name": "BROOKSIDE 500ML MILK", "dept": "DAIRY", "vendor": "BROOKSIDE",
         "price": 60.0, "stock": 0.0},        # sells but no stock → ghost
        {"name": "KINGSMIL 600G BREAD", "dept": "BAKERY", "vendor": "KINGSMIL",
         "price": 65.0, "stock": 30.0},       # seller with stock → mover
        {"name": "XMAS CANDLE SET", "dept": "XMAS", "vendor": "DECOR",
         "price": 500.0, "stock": 10.0},      # stocked, never sells → dead 5000
    ]


class TestAssess:
    def test_ghost_dead_and_coverage(self):
        demand = {"BROOKSIDE 500ML MILK": 304.0,     # 1/day over 10 months
                  "KINGSMIL 600G BREAD": 608.0,      # 2/day
                  "UNKNOWN ITEM": 100.0}             # assortment gap
        a = assess(_catalog(), demand, months=10)
        assert a["ghost_sellers"] == 1
        assert abs(a["ghost_lost_rev_day"] - 60.0) < 1.0     # 1/day × KES 60
        assert a["dead_skus"] == 1 and a["dead_stock_value"] == 5000.0
        assert a["assortment_gaps"] == 1
        # coverage = (304+608) / (304+608+100)
        assert abs(a["coverage_pct"] - 90.1) < 0.2
        assert a["top_movers"][0]["Item"].startswith("KINGSMIL")   # highest ADS
        assert a["top_movers"][0]["Days Cover"] == 15.0            # 30 / 2

    def test_empty_demand(self):
        a = assess(_catalog(), {}, months=10)
        # only the two STOCKED items can be dead; the zero-stock one traps nothing
        assert a["ghost_sellers"] == 0 and a["dead_skus"] == 2


class TestScorecard:
    def _patterns(self):
        return {
            "FLAKY LTD": {"lata_variance_multiplier": 1.8, "total_orders_2025": 100,
                          "avg_order_value_kes": 10000, "order_frequency": "weekly",
                          "estimated_delivery_days": 5, "avg_gap_days": 7},
            "SOLID LTD": {"lata_variance_multiplier": 0.9, "total_orders_2025": 400,
                          "avg_order_value_kes": 20000, "order_frequency": "daily",
                          "estimated_delivery_days": 1, "avg_gap_days": 1},
        }

    def test_classify_bands(self):
        assert classify(0.9) == "RELIABLE"
        assert classify(1.2) == "WATCH"
        assert classify(1.8) == "HOSTILE"

    def test_rows_sorted_by_spend_and_summary(self):
        rows = scorecard_rows(self._patterns())
        assert rows[0]["Supplier"] == "SOLID LTD"          # 8M > 1M spend
        assert rows[0]["Class"] == "RELIABLE"
        s = scorecard_summary(rows)
        assert s["suppliers"] == 2 and s["unreliable"] == 1
        assert s["at_risk_spend"] == 1_000_000.0           # FLAKY: 100×10k
        assert abs(s["at_risk_pct"] - 11.1) < 0.2          # 1M of 9M

    def test_write_artifacts(self, tmp_path):
        import json
        data_dir = tmp_path / "data"
        data_dir.mkdir()
        (data_dir / "supplier_patterns_2025.json").write_text(
            json.dumps(self._patterns()), encoding="utf-8")
        res = write_scorecard(str(data_dir), str(tmp_path / "reports"), tenant="T")
        assert os.path.exists(res["markdown"]) and os.path.exists(res["csv"])
        assert "HOSTILE" in open(res["markdown"], encoding="utf-8").read()
