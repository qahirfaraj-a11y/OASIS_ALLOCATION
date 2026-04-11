"""
Rigorous Mathematical Validation Tests
=======================================
These tests use HAND-CALCULATED expected values to validate every core formula
in the transfer/order decision pipeline. No source-code string checks.

FORMULAS BEING VALIDATED:
  1. days_of_stock = current_stock / avg_daily_sales
  2. gap_days = effective_lead_days - days_of_stock
  3. gap_qty = gap_days × avg_daily_sales  (clamped ≥ 0)
  4. transfer_target = min(gap_qty, shortfall_qty)
  5. max_transferable = min(donor_excess × max_donor_drain, transfer_target)
  6. transfer_cost = base_cost + (distance_km × per_km_rate)
  7. order_cost = shortfall_qty × unit_cost
  8. donor_score = excess / (distance + 0.1) [× hub_mult] [× dead_stock_mult]
  9. Haversine distance between known coordinates
 10. safety_stock = ads × 2.0
 11. excess = current_stock - safety_stock
 12. ConsolidatedTransferService network pipeline (end-to-end)
"""
import os
import sys
import math
import unittest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from oasis.logic.fulfillment_decider import (
    FulfillmentDecider,
    FulfillmentDecision,
    NetworkAvailabilityMap,
    StoreSkuState,
    DEFAULT_TRANSFER_COST_KES,
    DEFAULT_PER_KM_RATE,
    MAX_DONOR_DRAIN,
)
from oasis.logic.consolidated_transfer_service import (
    ConsolidatedTransferService,
    NetworkPlan,
)


# ─────────────────────────────────────────────────────────────────────
# HELPER: Independent Haversine implementation for cross-validation
# ─────────────────────────────────────────────────────────────────────
def reference_haversine(lat1, lon1, lat2, lon2):
    """Independent Haversine implementation (not from the codebase)."""
    R = 6371.0
    dlat = math.radians(lat2 - lat1)
    dlon = math.radians(lon2 - lon1)
    a = (math.sin(dlat / 2)**2 +
         math.cos(math.radians(lat1)) *
         math.cos(math.radians(lat2)) *
         math.sin(dlon / 2)**2)
    c = 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))
    return R * c


# ═════════════════════════════════════════════════════════════════════
# TEST 1: GAP-DAYS FORMULA
# ═════════════════════════════════════════════════════════════════════
class TestGapDaysFormula(unittest.TestCase):
    """
    Formula: days_of_stock = current_stock / ADS
             gap_days = lead_time - days_of_stock
    """

    def _make_network_with_donor(self, excess=200):
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Warehouse", itm_cd="ITM1",
            product_name="Test Item", current_stock=excess + 4,
            avg_daily_sales=2, safety_stock=4, excess=excess,
        ))
        return nmap

    def test_positive_gap(self):
        """
        current_stock=20, ADS=10 → days_of_stock=2.0
        lead_time=5 → gap_days=3.0, gap_qty=30.0
        Expected: BOTH (transfer to plug 3-day gap)
        """
        nmap = self._make_network_with_donor(200)
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Test Item",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=20.0, avg_daily_sales=10.0,
        )
        # Hand calc: gap_days = 5 - (20/10) = 3.0
        # gap_qty = 3 * 10 = 30, transfer_target = min(30, 50) = 30
        # max_transferable = min(200*0.5, 30) = 30
        self.assertEqual(d.decision, "BOTH")
        self.assertAlmostEqual(d.transfer_qty, 30.0, places=0)
        self.assertAlmostEqual(d.order_qty, 50.0, places=0)  # full order kept

    def test_zero_gap(self):
        """
        current_stock=50, ADS=10 → days_of_stock=5.0
        lead_time=5 → gap_days=0.0
        Expected: ORDER (no gap to plug)
        """
        nmap = self._make_network_with_donor(200)
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Test Item",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=50.0, avg_daily_sales=10.0,
        )
        # Hand calc: days_of_stock = 50/10 = 5.0, gap = 5-5 = 0 → ORDER
        self.assertEqual(d.decision, "ORDER")
        self.assertEqual(d.transfer_qty, 0.0)

    def test_negative_gap(self):
        """
        current_stock=100, ADS=10 → days_of_stock=10.0
        lead_time=5 → gap_days=-5.0
        Expected: ORDER (stock outlasts lead time)
        """
        nmap = self._make_network_with_donor(200)
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Test Item",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=100.0, avg_daily_sales=10.0,
        )
        self.assertEqual(d.decision, "ORDER")
        self.assertEqual(d.transfer_qty, 0.0)

    def test_fractional_gap(self):
        """
        current_stock=15, ADS=10 → days_of_stock=1.5
        lead_time=4 → gap_days=2.5, gap_qty=25.0
        """
        nmap = self._make_network_with_donor(200)
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Test Item",
            recipient_org="027", shortfall_qty=40,
            network_map=nmap, lead_time_days=4.0,
            current_stock=15.0, avg_daily_sales=10.0,
        )
        # gap_days = 4 - 1.5 = 2.5, gap_qty = 25
        # transfer_target = min(25, 40) = 25
        # max_transferable = min(100, 25) = 25
        self.assertEqual(d.decision, "BOTH")
        self.assertAlmostEqual(d.transfer_qty, 25.0, places=0)

    def test_zero_ads_means_no_transfer(self):
        """
        ADS=0 → days_of_stock=0, gap_days=lead_time=5
        But gap_qty = 5 × 0 = 0 (no units needed even though gap exists in time)
        Expected: ORDER (no items to transfer when nothing sells)
        """
        nmap = self._make_network_with_donor(200)
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="ITM1", product_name="Test Item",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=0.0, avg_daily_sales=0.0,
        )
        # ADS=0 → gap_qty=0 → transfer_target=0 → max_transferable=0 < 1.0 → ORDER
        self.assertEqual(d.decision, "ORDER")
        self.assertEqual(d.transfer_qty, 0.0)


# ═════════════════════════════════════════════════════════════════════
# TEST 2: TRANSFER_TARGET & MAX_TRANSFERABLE CAPPING
# ═════════════════════════════════════════════════════════════════════
class TestTransferCapping(unittest.TestCase):
    """
    Formula: transfer_target = min(gap_qty, shortfall_qty)
             max_transferable = min(donor_excess * MAX_DONOR_DRAIN, transfer_target)
    """

    def test_gap_qty_less_than_shortfall(self):
        """gap_qty=15, shortfall=50 → transfer_target=15."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=20.0, avg_daily_sales=5.0,  # days=4, gap=1, gap_qty=5
        )
        # gap_days = 5 - 4 = 1, gap_qty = 5, transfer_target = min(5, 50) = 5
        # max_transferable = min(248, 5) = 5
        self.assertEqual(d.decision, "BOTH")
        self.assertAlmostEqual(d.transfer_qty, 5.0, places=0)

    def test_donor_drain_caps_transfer(self):
        """Donor excess=10, drain=0.5 → max 5 transferable."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=20,
            avg_daily_sales=2, safety_stock=4, excess=10,  # small excess
        ))
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=5.0,
            current_stock=10.0, avg_daily_sales=10.0,
        )
        # gap_days=5-1=4, gap_qty=40, transfer_target=min(40,50)=40
        # max_transferable = min(10*0.5, 40) = 5
        self.assertEqual(d.decision, "BOTH")
        self.assertAlmostEqual(d.transfer_qty, 5.0, places=0)

    def test_shortfall_caps_transfer(self):
        """shortfall=3, gap_qty=30 → transfer_target=3."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=3,
            network_map=nmap, lead_time_days=5.0,
            current_stock=10.0, avg_daily_sales=10.0,
        )
        # gap_days=5-1=4, gap_qty=40, transfer_target=min(40,3)=3
        self.assertIn(d.decision, ("BOTH", "TRANSFER"))
        self.assertAlmostEqual(d.transfer_qty, 3.0, places=0)


# ═════════════════════════════════════════════════════════════════════
# TEST 3: HAVERSINE DISTANCE — cross-validated against known values
# ═════════════════════════════════════════════════════════════════════
class TestHaversineDistance(unittest.TestCase):
    """Validate Haversine against independently calculated distances."""

    def test_baba_dogo_to_rhapta_road(self):
        """
        Baba Dogo:   -1.2653, 36.8872
        Rhapta Road: -1.2641, 36.7865
        Google Maps ≈ ~11 km. Let's verify Haversine gives reasonable result.
        """
        dist_map = {
            "016": {"lat": -1.2653, "lon": 36.8872},
            "027": {"lat": -1.2641, "lon": 36.7865},
        }
        decider = FulfillmentDecider(distance_map=dist_map)
        d = decider._calculate_distance_km("016", "027")
        # Independent Haversine
        ref = reference_haversine(-1.2653, 36.8872, -1.2641, 36.7865)
        self.assertAlmostEqual(d, ref, places=2)
        # Sanity: should be ~11 km (Nairobi intra-city)
        self.assertGreater(d, 8.0)
        self.assertLess(d, 15.0)

    def test_nairobi_to_mombasa(self):
        """
        Nairobi (Baba Dogo): -1.2653, 36.8872
        Mombasa (Nyali):     -4.0435, 39.6682
        Real distance ≈ ~440 km. Haversine should be in ballpark.
        """
        dist_map = {
            "016": {"lat": -1.2653, "lon": 36.8872},
            "019": {"lat": -4.0435, "lon": 39.6682},
        }
        decider = FulfillmentDecider(distance_map=dist_map)
        d = decider._calculate_distance_km("016", "019")
        ref = reference_haversine(-1.2653, 36.8872, -4.0435, 39.6682)
        self.assertAlmostEqual(d, ref, places=2)
        # Should be roughly 400-500 km
        self.assertGreater(d, 380.0)
        self.assertLess(d, 500.0)

    def test_same_location_is_zero(self):
        """Distance from a point to itself should be ~0."""
        dist_map = {
            "A": {"lat": -1.2653, "lon": 36.8872},
            "B": {"lat": -1.2653, "lon": 36.8872},
        }
        decider = FulfillmentDecider(distance_map=dist_map)
        d = decider._calculate_distance_km("A", "B")
        self.assertAlmostEqual(d, 0.0, places=2)

    def test_missing_org_returns_default(self):
        """If org not in distance_map, returns default 10.0 km."""
        decider = FulfillmentDecider(distance_map={"016": {"lat": -1.26, "lon": 36.88}})
        d = decider._calculate_distance_km("016", "MISSING")
        self.assertEqual(d, 10.0)


# ═════════════════════════════════════════════════════════════════════
# TEST 4: TRANSFER COST FORMULA
# ═════════════════════════════════════════════════════════════════════
class TestTransferCostFormula(unittest.TestCase):
    """
    Formula: transfer_cost = base_cost_kes + (distance_km × per_km_rate)
    Defaults: base=500, per_km=50
    """

    def test_default_cost_at_10km(self):
        """10 km: 500 + 10*50 = 1000 KES."""
        # Distance default is 10km when no coords
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        decider = FulfillmentDecider()  # no distance_map → donor_distance=10
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=10,
            network_map=nmap, lead_time_days=5.0,
            current_stock=0.0, avg_daily_sales=5.0,
        )
        # 500 + 50*10 = 1000
        # But we need to check the decision has the right transfer cost
        # The estimated_transfer_cost should be set regardless of decision outcome
        expected_cost = DEFAULT_TRANSFER_COST_KES + (10.0 * DEFAULT_PER_KM_RATE)
        self.assertAlmostEqual(d.estimated_transfer_cost, expected_cost, places=0)
        self.assertEqual(expected_cost, 1000.0)

    def test_custom_cost_rate(self):
        """Custom: base=200, rate=30, dist=10 → 200+300=500."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        decider = FulfillmentDecider(transfer_cost_kes=200, per_km_rate=30)
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=10,
            network_map=nmap, lead_time_days=5.0,
            current_stock=0.0, avg_daily_sales=5.0,
        )
        expected_cost = 200 + (10.0 * 30)  # 500
        self.assertAlmostEqual(d.estimated_transfer_cost, expected_cost, places=0)

    def test_order_cost_formula(self):
        """order_cost = shortfall_qty × unit_cost."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        decider = FulfillmentDecider()
        d = decider.decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=20,
            network_map=nmap, lead_time_days=5.0,
            current_stock=0.0, avg_daily_sales=5.0,
            unit_cost=150.0,
        )
        # 20 * 150 = 3000
        self.assertAlmostEqual(d.estimated_order_cost, 3000.0, places=0)


# ═════════════════════════════════════════════════════════════════════
# TEST 5: DONOR SCORING — warehouse hub + dead-stock multipliers
# ═════════════════════════════════════════════════════════════════════
class TestDonorScoring(unittest.TestCase):
    """
    Formula: base_score = excess / (distance + 0.1)
             if hub: score × 3.0
             if dead_stock (age>45d, velocity<0.05): score × 2.0
    """

    def test_base_score_calculation(self):
        """Two donors, same excess, different distance. Closer wins."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Near", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        nmap.add(StoreSkuState(
            org_cd="B", org_name="Far", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        # No distance_calc → default distance 50 for both
        donors = nmap.find_donors("ITM1", recipient_org="C")
        # Both have score = 98 / (50 + 0.1) = 1.956
        self.assertEqual(len(donors), 2)
        s1 = donors[0].last_search_score
        s2 = donors[1].last_search_score
        # Both should be equal (same excess, same default distance)
        self.assertAlmostEqual(s1, s2, places=2)
        expected_score = 98.0 / 50.1
        self.assertAlmostEqual(s1, expected_score, places=2)

    def test_hub_multiplier_is_3x(self):
        """Hub donor should have exactly 3× the base score."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Hub", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        nmap.add(StoreSkuState(
            org_cd="005", org_name="Branch", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
        ))
        donors = nmap.find_donors("ITM1", "C", warehouse_hubs=["016"])
        hub = [d for d in donors if d.org_cd == "016"][0]
        branch = [d for d in donors if d.org_cd == "005"][0]
        # Both have same base score but hub gets 3×
        self.assertAlmostEqual(hub.last_search_score, branch.last_search_score * 3, places=2)

    def test_dead_stock_multiplier_is_2x(self):
        """Dead-stock donor (age>45, velocity<0.05) gets 2× score."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Dead", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=60, velocity_ratio=0.01,
        ))
        nmap.add(StoreSkuState(
            org_cd="B", org_name="Active", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=10, velocity_ratio=0.5,
        ))
        donors = nmap.find_donors("ITM1", "C")
        dead = [d for d in donors if d.org_cd == "A"][0]
        active = [d for d in donors if d.org_cd == "B"][0]
        self.assertAlmostEqual(dead.last_search_score, active.last_search_score * 2, places=2)

    def test_hub_plus_dead_stock_is_6x(self):
        """A hub with dead stock should get 3 × 2 = 6× base score."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="Dead Hub", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=60, velocity_ratio=0.01,
        ))
        nmap.add(StoreSkuState(
            org_cd="005", org_name="Normal Branch", itm_cd="ITM1",
            product_name="X", current_stock=100, avg_daily_sales=1,
            safety_stock=2, excess=98,
            days_since_delivery=10, velocity_ratio=0.5,
        ))
        donors = nmap.find_donors("ITM1", "C", warehouse_hubs=["016"])
        hub = [d for d in donors if d.org_cd == "016"][0]
        branch = [d for d in donors if d.org_cd == "005"][0]
        self.assertAlmostEqual(hub.last_search_score, branch.last_search_score * 6, places=2)


# ═════════════════════════════════════════════════════════════════════
# TEST 6: PENDING ORDER INTERACTION
# ═════════════════════════════════════════════════════════════════════
class TestPendingOrderInteraction(unittest.TestCase):
    """Test that pending orders correctly suppress or reduce transfers."""

    def _make_network(self):
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="ITM1",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        return nmap

    def test_imminent_delivery_suppresses_transfer(self):
        """
        Pending order: 30 units, ETA 0.5 days, covers ≥50% of shortfall=50.
        Stock=5, ADS=10 → days_of_stock=0.5, effective_lead=min(5, 0.5)=0.5
        gap_days = 0.5 - 0.5 = 0 → no-gap rule fires → ORDER
        This proves the system correctly avoids transfer when delivery is imminent.
        """
        d = FulfillmentDecider().decide(
            itm_cd="ITM1", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=self._make_network(), lead_time_days=5.0,
            current_stock=5.0, avg_daily_sales=10.0,
            pending_order_qty=30.0, pending_order_eta_days=0.5,
        )
        # pending ETA=0.5 < lead=5 → effective_lead=0.5
        # days_of_stock = 5/10 = 0.5, gap = 0.5-0.5 = 0 → ORDER
        self.assertEqual(d.decision, "ORDER")
        self.assertEqual(d.transfer_qty, 0.0)

    def test_pending_uses_shorter_eta(self):
        """
        lead_time=7, pending_eta=2 → effective_lead=2
        current_stock=5, ADS=5 → days_of_stock=1
        gap_days = 2 - 1 = 1 (not 7-1=6)
        """
        d = FulfillmentDecider().decide(
            itm_cd="ITM1", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=self._make_network(), lead_time_days=7.0,
            current_stock=5.0, avg_daily_sales=5.0,
            pending_order_qty=10.0, pending_order_eta_days=2.0,
        )
        # Pending order covers gap: gap_qty = 1*5=5, reduced by pending 10 → 0
        self.assertEqual(d.decision, "ORDER")
        self.assertIn("covers the gap", d.reasoning)

    def test_critical_stockout_overrides_pending(self):
        """
        stock=0.5, ADS=10 → hours_to_stockout=1.2h (<4h)
        Pending order exists but critical stockout bypasses it.
        """
        d = FulfillmentDecider().decide(
            itm_cd="ITM1", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=self._make_network(), lead_time_days=5.0,
            current_stock=0.5, avg_daily_sales=10.0,
            pending_order_qty=40.0, pending_order_eta_days=2.0,
        )
        # Critical: 0.5/10*24 = 1.2h
        self.assertIn(d.decision, ("BOTH", "TRANSFER"))
        self.assertIn("CRITICAL", d.reasoning)


# ═════════════════════════════════════════════════════════════════════
# TEST 7: SAFETY_STOCK & EXCESS CALCULATIONS in network map
# ═════════════════════════════════════════════════════════════════════
class TestSafetyStockAndExcess(unittest.TestCase):
    """
    Formula (in ConsolidatedTransferService._build_network_map):
      safety_stock = ADS × 2.0
      excess = current_stock - safety_stock
    """

    def test_network_map_excess_calculation(self):
        """Verify excess = current - (ADS × 2) via ConsolidatedTransferService."""
        stock_data = {
            "027": [
                {"itm_cd": "A", "product_name": "Widget",
                 "current_stocks": 100, "avg_daily_sales": 10,
                 "selling_price": 500},
            ],
        }
        service = ConsolidatedTransferService(
            org_names={"027": "Rhapta"},
            stock_data=stock_data,
        )
        state = service.network_map.get_store_state("027", "A")
        self.assertIsNotNone(state)
        # safety = 10 × 2 = 20, excess = 100 - 20 = 80
        self.assertAlmostEqual(state.safety_stock, 20.0, places=1)
        self.assertAlmostEqual(state.excess, 80.0, places=1)

    def test_negative_excess_when_understocked(self):
        """If stock < safety → excess is negative → not a donor."""
        stock_data = {
            "027": [
                {"itm_cd": "A", "product_name": "Widget",
                 "current_stocks": 5, "avg_daily_sales": 10,
                 "selling_price": 500},
            ],
        }
        service = ConsolidatedTransferService(
            org_names={"027": "Rhapta"},
            stock_data=stock_data,
        )
        state = service.network_map.get_store_state("027", "A")
        # safety = 20, excess = 5 - 20 = -15
        self.assertAlmostEqual(state.excess, -15.0, places=1)
        # Should NOT appear as donor
        donors = service.network_map.find_donors("A", recipient_org="016")
        donor_orgs = [d.org_cd for d in donors]
        self.assertNotIn("027", donor_orgs)


# ═════════════════════════════════════════════════════════════════════
# TEST 8: END-TO-END NETWORK PIPELINE
# ═════════════════════════════════════════════════════════════════════
class TestEndToEndNetworkPipeline(unittest.TestCase):
    """Run the full ConsolidatedTransferService.optimize_network pipeline with
    known inputs and verify the outputs match hand-calculated expectations."""

    def test_transfer_reduces_order(self):
        """
        Store A needs 50 widgets. Warehouse has 200 excess.
        Scenario: stock=0, ADS=10, lead_time=5d
        gap_days = 5, gap_qty=50, transfer_target=50
        max_transferable = min(200*0.5, 50) = 50
        Expected: BOTH (transfer 50, order 50 for replenishment)
        """
        org_names = {"016": "Warehouse", "027": "Rhapta"}
        stock_data = {
            "016": [{"itm_cd": "W1", "product_name": "Widget",
                      "current_stocks": 400, "avg_daily_sales": 5,
                      "selling_price": 100}],
            "027": [{"itm_cd": "W1", "product_name": "Widget",
                      "current_stocks": 0, "avg_daily_sales": 10,
                      "selling_price": 100}],
        }
        store_orders = {
            "027": [{
                "product_name": "Widget", "itm_cd": "W1",
                "recommended_quantity": 50, "avg_daily_sales": 10,
                "current_stocks": 0, "selling_price": 100,
                "estimated_delivery_days": 5, "supplier_name": "SupplierX",
            }],
        }
        service = ConsolidatedTransferService(org_names, stock_data)
        plan = service.optimize_network(store_orders)

        # Should generate transfers
        self.assertGreater(len(plan.transfers), 0)
        self.assertGreater(plan.total_units_transferred, 0)

        # The transfer should be from warehouse to Rhapta
        transfer = [t for t in plan.transfers if t.from_org == "016" and t.to_org == "027"]
        self.assertTrue(len(transfer) > 0, "Expected transfer from 016 to 027")

    def test_no_transfer_when_fully_stocked(self):
        """Store with plenty of stock shouldn't trigger transfers."""
        org_names = {"016": "Warehouse", "027": "Rhapta"}
        stock_data = {
            "016": [{"itm_cd": "W1", "product_name": "Widget",
                      "current_stocks": 400, "avg_daily_sales": 5,
                      "selling_price": 100}],
            "027": [{"itm_cd": "W1", "product_name": "Widget",
                      "current_stocks": 200, "avg_daily_sales": 10,
                      "selling_price": 100}],
        }
        store_orders = {
            "027": [{
                "product_name": "Widget", "itm_cd": "W1",
                "recommended_quantity": 50, "avg_daily_sales": 10,
                "current_stocks": 200, "selling_price": 100,
                "estimated_delivery_days": 3, "supplier_name": "SupplierX",
            }],
        }
        service = ConsolidatedTransferService(org_names, stock_data)
        plan = service.optimize_network(store_orders)

        # Decision for W1 at 027 should be ORDER (stock=200, ADS=10, days=20 > lead=3)
        w1_decisions = [d for d in plan.decisions if d.itm_cd == "W1"]
        self.assertTrue(len(w1_decisions) > 0)
        self.assertEqual(w1_decisions[0].decision, "ORDER")


# ═════════════════════════════════════════════════════════════════════
# TEST 9: DYNAMIC DONOR EXCESS RATIO (G8)
# ═════════════════════════════════════════════════════════════════════
class TestDynamicDonorExcessRatio(unittest.TestCase):
    """
    G8 formula:
      ADS > 5 → ratio = 1.5 (fast movers: lower bar)
      ADS ≤ 1 → ratio = 2.5 (slow movers: higher bar)
      else → ratio = 2.0 (default)

    Donor qualifies if: excess > 0 AND current_stock >= safety × ratio
    """

    def test_fast_mover_lower_bar(self):
        """ADS=10 (fast) → ratio=1.5. stock=30, safety=20 → 30 >= 20*1.5=30 → qualifies."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Store", itm_cd="ITM1",
            product_name="Fast Widget", current_stock=30, avg_daily_sales=10,
            safety_stock=20, excess=10,  # 30-20=10
        ))
        donors = nmap.find_donors("ITM1", "B", use_dynamic_ratio=True)
        # ratio=1.5, need stock >= 20*1.5=30. Stock=30 → QUALIFIES
        self.assertEqual(len(donors), 1)

    def test_fast_mover_fails_at_default_ratio(self):
        """Same store but with fixed ratio 2.0: 30 >= 20*2=40 → FAILS."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Store", itm_cd="ITM1",
            product_name="Fast Widget", current_stock=30, avg_daily_sales=10,
            safety_stock=20, excess=10,
        ))
        donors = nmap.find_donors("ITM1", "B", use_dynamic_ratio=False, min_excess_ratio=2.0)
        # ratio=2.0, need stock >= 20*2=40. Stock=30 → FAILS
        self.assertEqual(len(donors), 0)

    def test_slow_mover_higher_bar(self):
        """ADS=0.5 (slow) → ratio=2.5. stock=10, safety=1 → 10 >= 1*2.5=2.5 → qualifies."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Store", itm_cd="ITM1",
            product_name="Slow Widget", current_stock=10, avg_daily_sales=0.5,
            safety_stock=1, excess=9,
        ))
        donors = nmap.find_donors("ITM1", "B", use_dynamic_ratio=True)
        self.assertEqual(len(donors), 1)

    def test_slow_mover_rejected_at_boundary(self):
        """ADS=0.5 → ratio=2.5. stock=2, safety=1 → 2 < 1*2.5=2.5 → FAILS."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="A", org_name="Store", itm_cd="ITM1",
            product_name="Slow Widget", current_stock=2, avg_daily_sales=0.5,
            safety_stock=1, excess=1,
        ))
        donors = nmap.find_donors("ITM1", "B", use_dynamic_ratio=True)
        self.assertEqual(len(donors), 0)


# ═════════════════════════════════════════════════════════════════════
# TEST 10: GNN RISK-AWARE OVERRIDE
# ═════════════════════════════════════════════════════════════════════
class TestGNNRiskOverride(unittest.TestCase):
    """High risk score (>0.6) should force BOTH even without gap."""

    def test_high_risk_forces_both(self):
        """risk=0.8 should force BOTH including transfer, even if no gap."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        d = FulfillmentDecider(risk_threshold=0.6).decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=3.0,
            current_stock=50.0, avg_daily_sales=10.0,
            risk_score=0.8,  # > 0.6 threshold
        )
        # days_of_stock=5, gap=-2 (negative), BUT high risk overrides
        self.assertEqual(d.decision, "BOTH")
        self.assertIn("HIGH RISK", d.reasoning)

    def test_low_risk_allows_no_transfer(self):
        """risk=0.3, no gap → ORDER."""
        nmap = NetworkAvailabilityMap()
        nmap.add(StoreSkuState(
            org_cd="016", org_name="WH", itm_cd="A",
            product_name="Widget", current_stock=500,
            avg_daily_sales=2, safety_stock=4, excess=496,
        ))
        d = FulfillmentDecider(risk_threshold=0.6).decide(
            itm_cd="A", product_name="Widget",
            recipient_org="027", shortfall_qty=50,
            network_map=nmap, lead_time_days=3.0,
            current_stock=50.0, avg_daily_sales=10.0,
            risk_score=0.3,
        )
        self.assertEqual(d.decision, "ORDER")


if __name__ == "__main__":
    unittest.main()
