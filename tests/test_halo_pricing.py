"""Tests for the Halo pricing matrix (basket affinity → revenue recommendations)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.halo_pricing import (
    halo_pricing_rows, halo_summary, load_affinity, product_meta_from_adapter,
)


def _meta():
    return {
        "MILK": {"name": "Brookside Milk 500ml", "price": 60.0, "cost": 55.0, "ads": 100.0},
        "BREAD": {"name": "Kingsmil Bread 600g", "price": 65.0, "cost": 50.0, "ads": 40.0},
        "GUM": {"name": "Big G", "price": 10.0, "cost": 4.0, "ads": 5.0},
    }


def _metric(a, b, co, cab, cba, lift):
    return {"a": a, "b": b, "co_count": co,
            "conf_a_to_b": cab, "conf_b_to_a": cba, "lift": lift}


class TestHaloPricingRows:
    def test_anchor_is_higher_velocity_and_revenue_math(self):
        rows = halo_pricing_rows([_metric("MILK", "BREAD", 400, 0.4, 0.9, 4.0)], _meta())
        assert len(rows) == 1
        r = rows[0]
        assert r["Anchor"].startswith("Brookside")           # MILK anchors (ADS 100 > 40)
        assert r["Attachment"].startswith("Kingsmil")
        assert abs(r["Confidence"] - 0.4) < 1e-9              # P(bread | milk)
        # halo revenue = anchor_ads * conf * attach_price = 100 * 0.4 * 65
        assert abs(r["Halo Rev/Day"] - 2600.0) < 1.0
        # bread margin (65-50)/65 = 23% < 25 → headroom play
        assert "headroom" in r["Play"]

    def test_lift_and_confidence_gates(self):
        ms = [_metric("MILK", "BREAD", 10, 0.4, 0.9, 1.2),     # lift below 1.5
              _metric("MILK", "GUM", 10, 0.01, 0.2, 3.0)]      # conf below 0.05
        assert halo_pricing_rows(ms, _meta()) == []

    def test_unknown_sku_skipped(self):
        assert halo_pricing_rows([_metric("MILK", "GHOST", 10, 0.4, 0.4, 3.0)], _meta()) == []

    def test_sorted_by_halo_revenue(self):
        ms = [_metric("MILK", "GUM", 50, 0.2, 0.9, 2.0),       # 100*0.2*10 = 200
              _metric("MILK", "BREAD", 400, 0.4, 0.9, 4.0)]    # 2600
        rows = halo_pricing_rows(ms, _meta())
        assert rows[0]["Halo Rev/Day"] > rows[1]["Halo Rev/Day"]


class TestSummaryAndIo:
    def test_summary_counts(self):
        rows = halo_pricing_rows([_metric("MILK", "BREAD", 400, 0.4, 0.9, 4.0)], _meta())
        s = halo_summary(rows)
        assert s["pairs"] == 1 and s["anchors"] == 1
        assert s["est_daily_halo_revenue"] == 2600.0

    def test_load_affinity_missing_dir(self, tmp_path):
        assert load_affinity(str(tmp_path)) == []

    def test_product_meta_mapping(self):
        meta = product_meta_from_adapter([
            {"item_code": "X", "product_name": "Xi", "sell_price": 10,
             "cost_price": 6, "avg_daily_sales": 2.5},
            {"item_code": "", "product_name": "skipme"},
        ])
        assert meta["X"]["ads"] == 2.5 and meta["X"]["cost"] == 6.0
        assert len(meta) == 1
