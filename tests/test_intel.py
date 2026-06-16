"""Tests for the Intelligence Console (oasis/ui/intel.py).

Pure helpers + registry/role visibility only — no Streamlit rendering."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import intel
from oasis.ui import shell


class TestClassifyCover:
    def test_depleted(self):
        assert intel.classify_cover(5.0, 0.0) == "DEPLETED"

    def test_critical_urgent_low(self):
        assert intel.classify_cover(10.0, 4.0) == "CRITICAL"   # 0.4d
        assert intel.classify_cover(10.0, 8.0) == "URGENT"     # 0.8d
        assert intel.classify_cover(10.0, 20.0) == "LOW"       # 2.0d
        assert intel.classify_cover(10.0, 50.0) == "OK"        # 5.0d

    def test_overstock(self):
        assert intel.classify_cover(1.0, 60.0) == "OVERSTOCK"  # 60d

    def test_dead_when_no_sales_and_stock(self):
        assert intel.classify_cover(0.0, 40.0) == "DEAD"
        assert intel.classify_cover(0.0, 0.0) == "OK"


class TestVelocityAlerts:
    def _stock(self):
        return {
            "ORG001": [
                {"product_name": "MILK", "avg_daily_sales": 10, "current_stocks": 0},   # depleted
                {"product_name": "BREAD", "avg_daily_sales": 10, "current_stocks": 8},  # urgent 0.8d
                {"product_name": "RICE", "avg_daily_sales": 2, "current_stocks": 100},  # 50d, ok
                {"product_name": "TOY", "avg_daily_sales": 0, "current_stocks": 5},      # no ads
            ],
        }

    def test_only_at_risk_returned_sorted(self):
        rows = intel.velocity_alert_rows(self._stock(), {"ORG001": "Main"})
        names = [r["Product"] for r in rows]
        assert names == ["MILK", "BREAD"]   # depleted before urgent; rice/toy excluded
        assert rows[0]["Severity"] == "DEPLETED"
        assert rows[0]["Store"] == "Main"

    def test_threshold_widens_set(self):
        rows = intel.velocity_alert_rows(self._stock(), cover_threshold_days=7.0)
        assert {r["Product"] for r in rows} == {"MILK", "BREAD"}  # rice still 50d

    def test_empty(self):
        assert intel.velocity_alert_rows({}) == []


class TestStockReviewSummary:
    def test_counts(self):
        stock = {
            "A": [
                {"avg_daily_sales": 10, "current_stocks": 0},    # depleted
                {"avg_daily_sales": 1, "current_stocks": 60},    # overstock
                {"avg_daily_sales": 0, "current_stocks": 40},    # dead
                {"avg_daily_sales": 5, "current_stocks": 50},    # ok (10d)
            ]
        }
        s = intel.stock_review_summary(stock)
        assert s["total"] == 4
        assert s["DEPLETED"] == 1
        assert s["OVERSTOCK"] == 1
        assert s["DEAD"] == 1
        assert s["OK"] == 1


class TestPerStoreHealth:
    def test_rollup_and_sort(self):
        stock = {
            "A": [{"avg_daily_sales": 10, "current_stocks": 0}],   # at-risk
            "B": [{"avg_daily_sales": 1, "current_stocks": 60},    # overstock
                  {"avg_daily_sales": 0, "current_stocks": 40}],   # dead
        }
        rows = intel.per_store_health(stock, {"A": "Alpha", "B": "Beta"})
        # store with at-risk SKUs sorts first
        assert rows[0]["Store"] == "Alpha"
        beta = next(r for r in rows if r["Store"] == "Beta")
        assert beta["Overstock"] == 1 and beta["Dead"] == 1


class TestTopMovers:
    def test_ranks_by_revenue(self):
        import pandas as pd
        df = pd.DataFrame([
            {"item_name": "MILK", "qty": 5, "net_amt": 500},
            {"item_name": "MILK", "qty": 3, "net_amt": 300},
            {"item_name": "GUM", "qty": 100, "net_amt": 100},
        ])
        movers = intel.top_movers(df, n=5)
        assert movers[0]["Product"] == "MILK"
        assert movers[0]["Revenue"] == 800
        assert movers[0]["Units"] == 8

    def test_empty_df(self):
        import pandas as pd
        assert intel.top_movers(pd.DataFrame()) == []
        assert intel.top_movers(None) == []


class TestRegistry:
    def test_pulse_first_and_native(self):
        reg = intel.build_intel_registry()
        assert reg[0].key == "pulse"
        assert reg[0].render is intel.render_pulse

    def test_native_monitoring_pages(self):
        reg = {p.key: p for p in intel.build_intel_registry()}
        assert reg["velocity"].render is intel.render_velocity_alerts
        assert reg["stock_review"].render is intel.render_stock_review
        assert reg["live_sales"].render is intel.render_live_sales
        assert reg["network"].render is intel.render_network_intel

    def test_role_visibility(self):
        reg = intel.build_intel_registry()
        # finance is not in the intel oversight groups → sees only _ALL (pulse)
        fin = {p.key for p in shell.visible_pages(reg, "finance")}
        assert fin == {"pulse"}
        # executive (oversight) sees the monitoring pages
        execv = {p.key for p in shell.visible_pages(reg, "executive")}
        assert {"pulse", "velocity", "stock_review", "live_sales"} <= execv
        # operator sees everything incl. sim lab
        op = {p.key for p in shell.visible_pages(reg, "ilink_operator")}
        assert op == {p.key for p in reg}
