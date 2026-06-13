"""Tests for the shared greenfield runner used by both allocation UIs."""

import os
import sys

import pandas as pd
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.greenfield_runner import (
    find_latest_scorecard, load_scorecard_recommendations,
    run_greenfield_allocation, GreenfieldResult,
)
from oasis.logic.order_engine import OrderEngine


SCORECARD_COLS = (
    "Product,Unit_Price,Avg_Daily_Sales,Department,Pack_Size,"
    "Is_Staple,Margin_Pct,Supplier\n"
)


def _write_scorecard(path, rows):
    with open(path, "w", encoding="utf-8") as f:
        f.write(SCORECARD_COLS)
        for r in rows:
            f.write(",".join(str(x) for x in r) + "\n")


class TestFindLatestScorecard:
    def test_picks_highest_version(self, tmp_path):
        for v in (1, 3, 2, 10):
            (tmp_path / f"Full_Product_Allocation_Scorecard_v{v}.csv").write_text("x")
        latest = find_latest_scorecard(str(tmp_path))
        assert latest.endswith("v10.csv")

    def test_none_when_absent(self, tmp_path):
        assert find_latest_scorecard(str(tmp_path)) is None

    def test_unversioned_default_fallback(self, tmp_path):
        (tmp_path / "Full_Product_Allocation_Scorecard_v3.csv").write_text("x")
        # default name exists but no glob match for v* pattern beyond it
        assert find_latest_scorecard(str(tmp_path)).endswith("v3.csv")


class TestLoadScorecard:
    def test_column_mapping(self, tmp_path):
        sc = tmp_path / "Full_Product_Allocation_Scorecard_v1.csv"
        _write_scorecard(sc, [
            ("SUGAR 2KG", 250, 12.0, "SUGAR", 12, "TRUE", 12, "ACME"),
            ("TOY CAR", 450, 0.4, "TOYS", 6, "False", 30, "GAMMA"),
        ])
        recs = load_scorecard_recommendations(str(sc))
        assert len(recs) == 2
        r0 = recs[0]
        assert r0["product_name"] == "SUGAR 2KG"
        assert r0["selling_price"] == 250.0
        assert r0["avg_daily_sales"] == 12.0
        assert r0["product_category"] == "SUGAR"
        assert r0["pack_size"] == 12
        assert r0["is_staple_override"] is True
        assert r0["margin_pct"] == 12.0
        assert r0["supplier_name"] == "ACME"
        assert recs[1]["is_staple_override"] is False

    def test_blank_margin_becomes_none(self, tmp_path):
        sc = tmp_path / "Full_Product_Allocation_Scorecard_v1.csv"
        with open(sc, "w", encoding="utf-8") as f:
            f.write(SCORECARD_COLS)
            f.write("RICE,100,5,RICE,1,FALSE,,ACME\n")
        recs = load_scorecard_recommendations(str(sc))
        assert recs[0]["margin_pct"] is None


@pytest.fixture(scope="module")
def engine(tmp_path_factory):
    data_dir = tmp_path_factory.mktemp("gf_data")
    return OrderEngine(str(data_dir))


class TestRunGreenfield:
    def _recs(self):
        return [
            {"product_name": "SUGAR 2KG", "selling_price": 250.0,
             "avg_daily_sales": 12.0, "product_category": "SUGAR",
             "pack_size": 12, "moq_floor": 0, "historical_order_count": 0,
             "is_staple_override": True, "margin_pct": 12.0,
             "supplier_name": "ACME", "recommended_quantity": 0, "reasoning": ""},
            {"product_name": "COOKING OIL 1L", "selling_price": 350.0,
             "avg_daily_sales": 8.0, "product_category": "COOKING OIL",
             "pack_size": 6, "moq_floor": 0, "historical_order_count": 0,
             "is_staple_override": True, "margin_pct": 14.0,
             "supplier_name": "BETA", "recommended_quantity": 0, "reasoning": ""},
        ]

    def test_returns_result_with_basket(self, engine):
        res = run_greenfield_allocation(engine, self._recs(), budget=1_000_000.0)
        assert isinstance(res, GreenfieldResult)
        assert isinstance(res.basket, pd.DataFrame)
        assert not res.is_empty
        assert res.cash_spend > 0
        assert set(res.basket.columns) >= {
            "Product", "Department", "Qty", "Allocated_Cost",
            "Expected_Revenue", "Reasoning", "Type", "Avg_Daily_Sales",
        }

    def test_summary_metrics_match_basket(self, engine):
        res = run_greenfield_allocation(engine, self._recs(), budget=1_000_000.0)
        assert res.summary["total_cash_used"] == res.cash_spend
        # Utilization derived from realized cash, not the raw engine accumulator
        expected_util = round(res.cash_spend / 1_000_000.0 * 100, 2)
        assert res.summary["utilization_pct"] == expected_util

    def test_only_positive_quantities_in_basket(self, engine):
        res = run_greenfield_allocation(engine, self._recs(), budget=1_000_000.0)
        assert (res.basket["Qty"] > 0).all()

    def test_empty_recs_gives_empty_basket(self, engine):
        res = run_greenfield_allocation(engine, [], budget=500_000.0)
        assert res.is_empty
        assert res.cash_spend == 0.0
