"""
Transfer Gap-Plug & Warehouse Hub Tests
========================================
Tests for: Gap-plug transfer philosophy, warehouse hub priority,
dead-stock donor bonus, proactive transfer thresholds.
"""
import os
import sys
import json
import unittest
import inspect

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from oasis.logic.fulfillment_decider import (
    FulfillmentDecider,
    FulfillmentDecision,
    NetworkAvailabilityMap,
    StoreSkuState,
)


class TestGapPlugNoTransferWhenCovered(unittest.TestCase):
    """If stock covers until replenishment, decision should be ORDER only."""

    def test_no_gap_means_order_only(self):
        nmap = NetworkAvailabilityMap()
        # Donor has excess
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Warehouse", itm_cd="ITM1",
            product_name="Widget A", current_stock=500, avg_daily_sales=2,
            safety_stock=4, excess=496,
        ))
        # Recipient has enough stock to cover lead time
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Widget A",
            recipient_org="027", shortfall_qty=10,
            network_map=nmap,
            lead_time_days=3.0,
            current_stock=30,      # 30 / 10 = 3 days of stock
            avg_daily_sales=10.0,  # days_of_stock = 30/10 = 3d, gap = 3-3 = 0
        )
        self.assertEqual(d.decision, "ORDER")
        self.assertIn("No stockout gap", d.reasoning)


class TestGapPlugTransferQtyMatchesGap(unittest.TestCase):
    """Transfer qty should approximately equal gap_days × ADS, not full shortfall."""

    def test_transfer_covers_gap_not_full_shortfall(self):
        nmap = NetworkAvailabilityMap()
        # Donor with large excess
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Warehouse", itm_cd="ITM1",
            product_name="Widget A", current_stock=200, avg_daily_sales=1,
            safety_stock=2, excess=198,
        ))
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Widget A",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap,
            lead_time_days=5.0,
            current_stock=20,       # days_of_stock = 20/10 = 2d
            avg_daily_sales=10.0,   # gap = 5 - 2 = 3d, gap_qty = 30
        )
        self.assertIn(d.decision, ("BOTH", "TRANSFER"))
        # Transfer qty should be ~30 (gap_qty), not 50 (full shortfall)
        # Capped by max_donor_drain (99 = 198*0.5), so should be ~30
        self.assertLessEqual(d.transfer_qty, 35)
        self.assertGreater(d.transfer_qty, 0)
        # Full order should still be kept for replenishment
        if d.decision == "BOTH":
            self.assertEqual(d.order_qty, 50.0)


class TestWarehouseHubPriority(unittest.TestCase):
    """Org 016 (warehouse) should rank higher than equidistant branch."""

    def test_warehouse_hub_gets_priority(self):
        nmap = NetworkAvailabilityMap()
        # Branch donor with excess
        nmap.add(StoreSkuState(
            org_cd="005", org_name="ABC Branch", itm_cd="ITM1",
            product_name="Widget A", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        # Warehouse donor with same excess
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Warehouse Baba Dogo", itm_cd="ITM1",
            product_name="Widget A", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        # Find donors with warehouse hub priority
        donors = nmap.find_donors(
            "ITM1", recipient_org="027",
            warehouse_hubs=["016"],
        )
        self.assertTrue(len(donors) >= 2)
        # Warehouse should be first due to 3× score boost
        self.assertEqual(donors[0].org_cd, "016")

    def test_find_donors_accepts_warehouse_hubs_param(self):
        sig = inspect.signature(NetworkAvailabilityMap.find_donors)
        self.assertIn("warehouse_hubs", sig.parameters)


class TestDeadStockDonorBonus(unittest.TestCase):
    """Aged, slow-moving stock should be prioritized as donor."""

    def test_dead_stock_scores_higher(self):
        nmap = NetworkAvailabilityMap()
        # Regular donor
        nmap.add(StoreSkuState(
            org_cd="005", org_name="ABC Branch", itm_cd="ITM1",
            product_name="Widget A", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=10, velocity_ratio=0.5,
        ))
        # Dead-stock donor (aged, slow-moving)
        nmap.add(StoreSkuState(
            org_cd="009", org_name="Diamond Plaza", itm_cd="ITM1",
            product_name="Widget A", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=60, velocity_ratio=0.02,
        ))
        donors = nmap.find_donors("ITM1", recipient_org="027")
        self.assertTrue(len(donors) >= 2)
        # Dead-stock donor should rank first due to 2× bonus
        self.assertEqual(donors[0].org_cd, "009")


class TestProactiveTransferRelaxedThresholds(unittest.TestCase):
    """Dead stock should be identified with relaxed criteria (45d, 0.05 velocity)."""

    def test_relaxed_thresholds_in_source(self):
        from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
        source = inspect.getsource(ConsolidatedTransferService._identify_proactive_transfers)
        # Check relaxed thresholds are present
        self.assertIn("45", source, "Days threshold should be 45")
        self.assertIn("0.05", source, "Velocity threshold should be 0.05")
        self.assertIn("50.0", source, "Max move should be 50 units")
        self.assertIn("0.5", source, "Max move ratio should be 50%")

    def test_warehouse_as_recipient_in_source(self):
        from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
        source = inspect.getsource(ConsolidatedTransferService._identify_proactive_transfers)
        self.assertIn("warehouse_hubs", source, "Should reference warehouse hubs as recipients")
        self.assertIn("is_warehouse_hub", source, "Should check is_warehouse_hub flag")


class TestDeciderAcceptsWarehouseHubs(unittest.TestCase):
    """FulfillmentDecider should accept warehouse_hubs parameter."""

    def test_init_accepts_warehouse_hubs(self):
        sig = inspect.signature(FulfillmentDecider.__init__)
        self.assertIn("warehouse_hubs", sig.parameters)

    def test_decider_stores_warehouse_hubs(self):
        decider = FulfillmentDecider(warehouse_hubs=["016", "001"])
        self.assertEqual(decider.warehouse_hubs, ["016", "001"])


class TestGapPlugReasoning(unittest.TestCase):
    """Gap-plug reasoning should mention gap days and warehouse hub."""

    def test_gap_plug_reasoning_content(self):
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Warehouse Baba Dogo", itm_cd="ITM1",
            product_name="Widget A", current_stock=200, avg_daily_sales=1,
            safety_stock=2, excess=198,
        ))
        decider = FulfillmentDecider(warehouse_hubs=["016"])
        d = decider.decide(
            itm_cd="ITM1", product_name="Widget A",
            recipient_org="027", shortfall_qty=30,
            network_map=nmap,
            lead_time_days=5.0,
            current_stock=10,       # days_of_stock = 10/5 = 2d
            avg_daily_sales=5.0,    # gap = 5 - 2 = 3d
        )
        if d.decision in ("BOTH", "TRANSFER"):
            self.assertIn("GAP-PLUG", d.reasoning)
            self.assertIn("WAREHOUSE HUB", d.reasoning)


class TestStoreCoords(unittest.TestCase):
    """store_coords.json should have Baba Dogo warehouse and real org codes."""

    def test_store_coords_has_warehouse(self):
        path = os.path.join(os.getcwd(), "store_coords.json")
        with open(path, 'r') as f:
            coords = json.load(f)
        self.assertIn("016", coords)
        self.assertEqual(coords["016"]["name"], "Warehouse Baba Dogo")
        self.assertTrue(coords["016"]["is_warehouse_hub"])
        self.assertAlmostEqual(coords["016"]["lat"], -1.2653, places=3)
        self.assertAlmostEqual(coords["016"]["lon"], 36.8872, places=3)

    def test_store_coords_has_real_org_codes(self):
        path = os.path.join(os.getcwd(), "store_coords.json")
        with open(path, 'r') as f:
            coords = json.load(f)
        # Should have both real DB org codes and legacy CFP-* keys
        self.assertIn("027", coords)  # Rhapta Road
        self.assertIn("001", coords)  # Head Office
        self.assertIn("CFP-003", coords)  # Legacy key


class TestLoadAllStocksNoArgs(unittest.TestCase):
    """load_all_stocks in ops_dashboard should take 0 args."""

    def test_no_args_in_source(self):
        dashboard_path = os.path.join(os.getcwd(), "ops_dashboard.py")
        with open(dashboard_path, "r", encoding="utf-8") as f:
            content = f.read()
        # The erroneous call with args should be gone
        self.assertNotIn("load_all_stocks([", content,
                         "load_all_stocks should not be called with list arg")
        # The correct calls (no args) should remain
        self.assertIn("load_all_stocks()", content)


if __name__ == "__main__":
    unittest.main()
