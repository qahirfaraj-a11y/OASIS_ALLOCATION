"""
Phase B Verification Tests
==========================
Tests for Gap Fixes: G12 (EWMA ADS), G3 (ROP fallback), G4 (thresholds), G11 (stock refresh).
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestG12WeightedADS(unittest.TestCase):
    """G12: Sales ADS should use recency-weighted calculation."""

    def test_calc_weighted_ads_method_exists(self):
        """PosErpAdapter should have _calc_weighted_ads method."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        self.assertTrue(hasattr(PosErpAdapter, "_calc_weighted_ads"))

    def test_weighted_ads_in_enriched_products(self):
        """fetch_enriched_products should reference weighted_ads."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        import inspect
        source = inspect.getsource(PosErpAdapter.fetch_enriched_products)
        self.assertIn("_calc_weighted_ads", source,
                      "fetch_enriched_products should call _calc_weighted_ads")
        self.assertIn("weighted_ads", source,
                      "fetch_enriched_products should use weighted_ads value")

    def test_weighted_ads_returns_dict_format(self):
        """_calc_weighted_ads should return {itm_cd: {weighted_ads, total_90d, ads_30d, ads_60d}}."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        import inspect
        source = inspect.getsource(PosErpAdapter._calc_weighted_ads)
        self.assertIn("weighted_ads", source)
        self.assertIn("total_90d", source)
        self.assertIn("ads_30d", source)
        # Check weight constants
        self.assertIn("0.60", source, "Should use 60% weight for last 30 days")
        self.assertIn("0.30", source, "Should use 30% weight for 30-60 days")
        self.assertIn("0.10", source, "Should use 10% weight for 60-90 days")


class TestG3ROPFallback(unittest.TestCase):
    """G3: ROP should not default to 0 when data is missing."""

    def test_rop_fallback_in_bridge(self):
        """calculate_order_quantity should contain ROP fallback logic."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        source = inspect.getsource(SimulationOrderUtil.calculate_order_quantity)
        self.assertIn("ROP Fallback", source,
                      "Should contain ROP Fallback logic")
        self.assertIn("fallback_rop", source,
                      "Should calculate a fallback ROP")
        self.assertIn("intelligence data missing", source,
                      "Should tag reasoning with intelligence data missing")

    def test_rop_fallback_uses_ads_and_lead_time(self):
        """Fallback ROP should be based on ADS * (lead_time + safety)."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        source = inspect.getsource(SimulationOrderUtil.calculate_order_quantity)
        # Should have: fallback_rop = avg_daily_sales * (lead_time + ...)
        self.assertIn("avg_daily_sales", source)
        self.assertIn("lead_time", source)


class TestG4ConfigurableThresholds(unittest.TestCase):
    """G4: Dead stock/freshness thresholds should be configurable."""

    def test_thresholds_in_init(self):
        """SimulationOrderUtil should accept thresholds parameter."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        sig = inspect.signature(SimulationOrderUtil.__init__)
        self.assertIn("thresholds", sig.parameters,
                      "Should accept thresholds parameter")

    def test_thresholds_contains_all_keys(self):
        """Default thresholds should include all required keys."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as MockEng, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MockCal:
            mock_eng = MagicMock()
            mock_eng.load_local_databases.return_value = None
            MockEng.return_value = mock_eng
            MockCal.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"))
            self.assertIn("fresh_stale_days", util.thresholds)
            self.assertIn("dry_dead_days", util.thresholds)
            self.assertIn("dry_dead_min_sales", util.thresholds)
            self.assertIn("key_sku_boost_pct", util.thresholds)
            self.assertIn("critical_stockout_days", util.thresholds)

    def test_custom_thresholds_override(self):
        """Custom thresholds should override defaults."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as MockEng, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MockCal:
            mock_eng = MagicMock()
            mock_eng.load_local_databases.return_value = None
            MockEng.return_value = mock_eng
            MockCal.return_value = MagicMock()
            
            custom = {'fresh_stale_days': 90, 'dry_dead_days': 150}
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"), thresholds=custom)
            self.assertEqual(util.thresholds['fresh_stale_days'], 90)
            self.assertEqual(util.thresholds['dry_dead_days'], 150)

    def test_thresholds_used_in_calculation(self):
        """calculate_order_quantity should use self.thresholds instead of hardcoded values."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        source = inspect.getsource(SimulationOrderUtil.calculate_order_quantity)
        self.assertIn("self.thresholds", source,
                      "Should reference self.thresholds")
        self.assertNotIn("> 120:", source,
                         "Should NOT have hardcoded 120 threshold")
        self.assertNotIn("> 200:", source,
                         "Should NOT have hardcoded 200 threshold")

    def test_settings_tab_has_threshold_ui(self):
        """Dashboard should have ordering threshold controls in Settings."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("Ordering Thresholds", content)
        self.assertIn("Fresh Stale Threshold", content)
        self.assertIn("Dead Stock Threshold", content)
        self.assertIn("ordering_thresholds", content)


class TestG11StockRefresh(unittest.TestCase):
    """G11: Dashboard should have a stock refresh button."""

    def test_refresh_button_exists(self):
        """Dashboard should have a Refresh Stock button."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("Refresh Stock", content,
                      "Dashboard should have Refresh Stock button")

    def test_refresh_clears_caches(self):
        """Refresh should clear load_products and load_network_stock caches."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("load_products.clear()", content)
        self.assertIn("load_network_stock.clear()", content)
        self.assertIn("load_all_stocks.clear()", content)


if __name__ == "__main__":
    unittest.main()
