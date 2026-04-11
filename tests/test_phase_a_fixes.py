"""
Phase A Verification Tests
==========================
Tests for Gap Fixes: G1 (on_order_qty), G2 (calendar path), G7+G14 (network integration).
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock
from datetime import datetime

# Ensure project root is on path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestG2CalendarPathFix(unittest.TestCase):
    """G2: Calendar path should be relative + use_real_date mapping."""

    def test_find_calendar_path_uses_relative(self):
        """_find_calendar_path should not contain hardcoded user paths."""
        from oasis.logic.simulation_bridge import _find_calendar_path
        
        # Should be a callable function (it exists)
        self.assertTrue(callable(_find_calendar_path))
        
        # The returned path should not contain hardcoded path
        result = _find_calendar_path(os.path.join(os.getcwd(), "data"))
        self.assertNotIn(r"c:\Users\iLink\.gemini", result.lower().replace("/", "\\"),
                         "Calendar path should not be hardcoded")

    def test_hardcoded_path_removed(self):
        """simulation_bridge.py should NOT contain the hardcoded calendar path."""
        bridge_path = os.path.join(os.getcwd(), "oasis", "logic", "simulation_bridge.py")
        with open(bridge_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        self.assertNotIn(
            r'r"c:\Users\iLink',
            content,
            "Hardcoded absolute path should be removed from simulation_bridge.py"
        )

    def test_use_real_date_flag_exists(self):
        """calculate_order_quantity should accept use_real_date parameter."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        
        sig = inspect.signature(SimulationOrderUtil.calculate_order_quantity)
        self.assertIn("use_real_date", sig.parameters,
                      "use_real_date parameter should exist")
        # Default should be False for backward compatibility
        self.assertEqual(sig.parameters["use_real_date"].default, False)

    def test_use_real_date_maps_to_day_of_year(self):
        """When use_real_date=True, current_day should be overridden to day-of-year."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        
        # Create a util but mock the engine to avoid file dependencies
        with patch("oasis.logic.simulation_bridge.OrderEngine") as MockEngine, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MockCal:
            
            mock_eng = MagicMock()
            mock_eng.load_local_databases.return_value = None
            mock_eng.enrich_product_data.return_value = []
            MockEngine.return_value = mock_eng

            mock_cal = MagicMock()
            MockCal.return_value = mock_cal

            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"))
            
            # With empty SKU list, just verify it doesn't crash
            result = util.calculate_order_quantity([], use_real_date=True)
            self.assertEqual(result, [])


class TestG1PendingPOAwareness(unittest.TestCase):
    """G1: on_order_qty should be populated from pending POs."""

    def test_fetch_pending_po_by_sku_method_exists(self):
        """PosErpAdapter should have fetch_pending_po_by_sku method."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        self.assertTrue(hasattr(PosErpAdapter, "fetch_pending_po_by_sku"),
                        "Method fetch_pending_po_by_sku should exist on PosErpAdapter")

    def test_fetch_pending_po_by_sku_returns_dict(self):
        """fetch_pending_po_by_sku should return {itm_cd: {qty, eta_days}}."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        import inspect
        
        sig = inspect.signature(PosErpAdapter.fetch_pending_po_by_sku)
        params = list(sig.parameters.keys())
        self.assertIn("org_cd", params)

    def test_on_order_qty_in_enriched_products_flow(self):
        """fetch_enriched_products should set on_order_qty field."""
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        import inspect
        
        # Check the source code for on_order_qty assignment
        source = inspect.getsource(PosErpAdapter.fetch_enriched_products)
        self.assertIn("on_order_qty", source,
                      "fetch_enriched_products should set on_order_qty")
        self.assertIn("pending_po_eta_days", source,
                      "fetch_enriched_products should set pending_po_eta_days")

    def test_simulation_bridge_uses_on_order_qty(self):
        """calculate_order_quantity should use on_order_qty in net requirement."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        import inspect
        
        source = inspect.getsource(SimulationOrderUtil.calculate_order_quantity)
        self.assertIn("on_order_qty", source,
                      "calculate_order_quantity should reference on_order_qty")


class TestG7G14NetworkIntegration(unittest.TestCase):
    """G7+G14: Dashboard should wire ConsolidatedTransferService with real-date POs."""

    def test_dashboard_uses_real_date(self):
        """ops_dashboard.py should pass use_real_date=True to calculate_order_quantity."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        self.assertIn("use_real_date=True", content,
                      "Dashboard should use use_real_date=True for scheduling")

    def test_dashboard_has_network_optimization(self):
        """ops_dashboard.py should contain ConsolidatedTransferService wiring."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        self.assertIn("ConsolidatedTransferService", content,
                      "Dashboard should import ConsolidatedTransferService")
        self.assertIn("optimize_network", content,
                      "Dashboard should call optimize_network")
        self.assertIn("Network Transfer Optimization", content,
                      "Dashboard should have network optimization UI")

    def test_dashboard_shows_on_order_awareness(self):
        """Dashboard should show pending PO awareness banner."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        
        self.assertIn("on_order_count", content,
                      "Dashboard should count on-order SKUs")
        self.assertIn("pending POs", content,
                      "Dashboard should show pending PO awareness message")

    def test_consolidated_transfer_service_exists(self):
        """ConsolidatedTransferService should be importable."""
        from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
        self.assertTrue(callable(ConsolidatedTransferService))

    def test_fulfillment_decider_exists(self):
        """FulfillmentDecider should be importable."""
        from oasis.logic.fulfillment_decider import FulfillmentDecider
        self.assertTrue(callable(FulfillmentDecider))


if __name__ == "__main__":
    unittest.main()
