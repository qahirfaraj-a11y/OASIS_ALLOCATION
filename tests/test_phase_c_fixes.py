"""
Phase C Verification Tests
==========================
Tests for: MOT Gate, Transfer-First Routing, G8 Dynamic Donor, G9 Distance Cost, G17 PO Dedup.
"""
import os
import sys
import unittest
from unittest.mock import patch, MagicMock

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))


class TestMOTGateLogic(unittest.TestCase):
    """Minimum Order Threshold gate in SimulationOrderUtil."""

    def test_apply_minimum_order_gate_exists(self):
        """SimulationOrderUtil should have apply_minimum_order_gate method."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        self.assertTrue(hasattr(SimulationOrderUtil, "apply_minimum_order_gate"))

    def test_mot_splits_above_below(self):
        """Gate should split recs into po_recs (above MOT) and transfer_recs (below)."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as ME, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MC:
            ME.return_value = MagicMock()
            ME.return_value.load_local_databases.return_value = None
            MC.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"),
                                        thresholds={'min_order_units': 10, 'min_order_value_kes': 5000})
            
            recs = [
                # Supplier A: 15 units total → above MOT
                {'product_name': 'P1', 'supplier_name': 'Supplier A', 'recommended_quantity': 8, 'selling_price': 100},
                {'product_name': 'P2', 'supplier_name': 'Supplier A', 'recommended_quantity': 7, 'selling_price': 100},
                # Supplier B: 3 units total → below MOT
                {'product_name': 'P3', 'supplier_name': 'Supplier B', 'recommended_quantity': 3, 'selling_price': 50},
            ]
            
            result = util.apply_minimum_order_gate(recs)
            self.assertIn('po_recs', result)
            self.assertIn('transfer_recs', result)
            self.assertEqual(len(result['po_recs']), 2)  # A's items
            self.assertEqual(len(result['transfer_recs']), 1)  # B's item

    def test_mot_tags_fulfillment_type(self):
        """Above-MOT items should be SUPPLIER_PO, below-MOT should be TRANSFER_FIRST."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as ME, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MC:
            ME.return_value = MagicMock()
            ME.return_value.load_local_databases.return_value = None
            MC.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"),
                                        thresholds={'min_order_units': 10, 'min_order_value_kes': 5000})
            
            recs = [
                {'product_name': 'P1', 'supplier_name': 'BigCo', 'recommended_quantity': 20, 'selling_price': 500},
                {'product_name': 'P2', 'supplier_name': 'SmallCo', 'recommended_quantity': 2, 'selling_price': 50},
            ]
            
            result = util.apply_minimum_order_gate(recs)
            for r in result['po_recs']:
                self.assertEqual(r['fulfillment'], 'SUPPLIER_PO')
            for r in result['transfer_recs']:
                self.assertEqual(r['fulfillment'], 'TRANSFER_FIRST')
                self.assertIn('Below MOT', r['reasoning'])

    def test_mot_value_threshold_pass(self):
        """Even with few units, high-value items should pass if value threshold met."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as ME, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MC:
            ME.return_value = MagicMock()
            ME.return_value.load_local_databases.return_value = None
            MC.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"),
                                        thresholds={'min_order_units': 50, 'min_order_value_kes': 5000})
            
            # Only 2 units but value = 2 * 5000 = 10000 → above value threshold
            recs = [
                {'product_name': 'ExpensiveItem', 'supplier_name': 'LuxCo',
                 'recommended_quantity': 2, 'selling_price': 5000},
            ]
            
            result = util.apply_minimum_order_gate(recs)
            self.assertEqual(len(result['po_recs']), 1)
            self.assertEqual(len(result['transfer_recs']), 0)

    def test_mot_thresholds_defaults(self):
        """Default thresholds should include MOT keys."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as ME, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MC:
            ME.return_value = MagicMock()
            ME.return_value.load_local_databases.return_value = None
            MC.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"))
            self.assertIn('min_order_units', util.thresholds)
            self.assertIn('min_order_value_kes', util.thresholds)
            self.assertEqual(util.thresholds['min_order_units'], 10)
            self.assertEqual(util.thresholds['min_order_value_kes'], 5000)

    def test_mot_supplier_summary(self):
        """Result should include supplier_summary with status and counts."""
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        with patch("oasis.logic.simulation_bridge.OrderEngine") as ME, \
             patch("oasis.logic.simulation_bridge.SupplierCalendar") as MC:
            ME.return_value = MagicMock()
            ME.return_value.load_local_databases.return_value = None
            MC.return_value = MagicMock()
            
            util = SimulationOrderUtil(os.path.join(os.getcwd(), "data"),
                                        thresholds={'min_order_units': 10, 'min_order_value_kes': 5000})
            
            recs = [
                {'product_name': 'P1', 'supplier_name': 'A', 'recommended_quantity': 15, 'selling_price': 100},
                {'product_name': 'P2', 'supplier_name': 'B', 'recommended_quantity': 3, 'selling_price': 50},
            ]
            
            result = util.apply_minimum_order_gate(recs)
            summary = result['supplier_summary']
            self.assertIn('A', summary)
            self.assertIn('B', summary)
            self.assertEqual(summary['A']['status'], 'PO')
            self.assertEqual(summary['B']['status'], 'TRANSFER')


class TestG8DynamicDonorRatio(unittest.TestCase):
    """G8: Donor excess ratio should be velocity-dependent."""

    def test_find_donors_has_dynamic_ratio(self):
        """find_donors should accept use_dynamic_ratio parameter."""
        from oasis.logic.fulfillment_decider import NetworkAvailabilityMap
        import inspect
        sig = inspect.signature(NetworkAvailabilityMap.find_donors)
        self.assertIn("use_dynamic_ratio", sig.parameters)

    def test_dynamic_ratio_in_source(self):
        """Source should reference velocity-based ratio logic."""
        from oasis.logic.fulfillment_decider import NetworkAvailabilityMap
        import inspect
        source = inspect.getsource(NetworkAvailabilityMap.find_donors)
        self.assertIn("G8 Fix", source)
        self.assertIn("1.5", source, "Fast movers should use 1.5× ratio")
        self.assertIn("2.5", source, "Slow movers should use 2.5× ratio")
        self.assertIn("avg_daily_sales", source)


class TestG9DistanceBasedCost(unittest.TestCase):
    """G9: Transfer cost should include distance component."""

    def test_per_km_rate_constant(self):
        """Module should define DEFAULT_PER_KM_RATE."""
        from oasis.logic import fulfillment_decider as fd
        self.assertTrue(hasattr(fd, "DEFAULT_PER_KM_RATE"))
        self.assertEqual(fd.DEFAULT_PER_KM_RATE, 50.0)

    def test_decider_has_per_km_rate(self):
        """FulfillmentDecider should accept per_km_rate parameter."""
        from oasis.logic.fulfillment_decider import FulfillmentDecider
        import inspect
        sig = inspect.signature(FulfillmentDecider.__init__)
        self.assertIn("per_km_rate", sig.parameters)

    def test_distance_cost_in_decide(self):
        """decide() should calculate distance-based transfer cost."""
        from oasis.logic.fulfillment_decider import FulfillmentDecider
        import inspect
        source = inspect.getsource(FulfillmentDecider.decide)
        self.assertIn("G9 Fix", source)
        self.assertIn("per_km_rate", source)
        self.assertIn("donor_distance", source)


class TestG17PODedup(unittest.TestCase):
    """G17: Dashboard should check for existing POs before generating."""

    def test_dedup_check_in_dashboard(self):
        """Dashboard should contain PO dedup check logic."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("Duplicate PO Alert", content)
        self.assertIn("fetch_pending_pos", content)


class TestSettingsMOTControls(unittest.TestCase):
    """Settings tab should have MOT threshold controls."""

    def test_mot_controls_in_settings(self):
        """Settings should have Min Order Units and Min Order Value inputs."""
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        self.assertIn("Min Order Units", content)
        self.assertIn("Min Order Value", content)
        self.assertIn("Minimum Order Threshold (MOT)", content)
        self.assertIn("min_order_units", content)
        self.assertIn("min_order_value_kes", content)


if __name__ == "__main__":
    unittest.main()
