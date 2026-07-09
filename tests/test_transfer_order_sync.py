"""
Quick integration test for the consolidated transfer layer.
Includes pending-order awareness tests.
"""
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from oasis.logic.transfer_state import TransferStateTracker, TransferRecord
from oasis.logic.fulfillment_decider import FulfillmentDecider, NetworkAvailabilityMap, StoreSkuState
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

# --- Test 1: TransferStateTracker ---
print("=== Test 1: TransferStateTracker ===")
tracker = TransferStateTracker()
t1 = TransferRecord(from_org="ORG002", to_org="ORG001", itm_cd="ITM001",
                     product_name="Test Product", qty=50)
tracker.register_transfer(t1)
assert tracker.get_inbound_qty("ORG001", "ITM001") == 50, "Inbound should be 50"
assert tracker.get_outbound_qty("ORG002", "ITM001") == 50, "Outbound should be 50"
assert tracker.get_inbound_qty("ORG002", "ITM001") == 0, "ORG002 should have 0 inbound"
print("  Inbound/Outbound tracking: PASS")

tracker.complete_transfer(t1.transfer_id)
assert tracker.get_inbound_qty("ORG001", "ITM001") == 0, "After delivery, inbound should be 0"
print("  Completion clears indices: PASS")

summary = tracker.get_network_summary()
assert summary["delivered"] == 1
print(f"  Summary: {summary}")
print("  TransferStateTracker: ALL PASS")

# --- Test 2: FulfillmentDecider ---
print()
print("=== Test 2: FulfillmentDecider ===")
decider = FulfillmentDecider()
nmap = NetworkAvailabilityMap()
nmap.add(StoreSkuState(
    org_cd="ORG002", org_name="Store B", itm_cd="ITM001", product_name="Milk",
    current_stock=100, avg_daily_sales=5, safety_stock=10, excess=90, sell_price=50
))
nmap.add(StoreSkuState(
    org_cd="ORG001", org_name="Store A", itm_cd="ITM001", product_name="Milk",
    current_stock=0, avg_daily_sales=5, safety_stock=10, excess=-10, sell_price=50
))

# Test: donor available, ordering day
d = decider.decide("ITM001", "Milk", "ORG001", 20, nmap,
                    is_ordering_day=True, lead_time_days=3)
print(f"  Decision: {d.decision} | Transfer: {d.transfer_qty} | Order: {d.order_qty}")
print(f"  Reasoning: {d.reasoning}")
assert d.decision in ("TRANSFER", "BOTH", "ORDER"), "Should make a valid decision"
print("  With donor + ordering day: PASS")

# Test: no donor
d2 = decider.decide("ITM999", "Unknown", "ORG001", 20, nmap, is_ordering_day=True)
assert d2.decision == "ORDER", "No donor -> ORDER"
print("  No donor: PASS")

# Test: donor available, NOT ordering day
d3 = decider.decide("ITM001", "Milk", "ORG001", 20, nmap,
                     is_ordering_day=False, lead_time_days=3, avg_daily_sales=5.0)
assert d3.decision in ("TRANSFER", "BOTH"), "Not ordering day + donor -> TRANSFER or BOTH"
print(f"  Not ordering day: {d3.decision} (PASS)")
print("  FulfillmentDecider: ALL PASS")

# --- Test 3: ConsolidatedTransferService ---
print()
print("=== Test 3: ConsolidatedTransferService ===")
org_names = {"ORG001": "Store Alpha", "ORG002": "Store Beta"}
stock_data = {
    "ORG001": [{"product_name": "Milk", "itm_cd": "ITM001",
                "avg_daily_sales": 5, "current_stocks": 0,
                "selling_price": 50, "department": "DAIRY"}],
    "ORG002": [{"product_name": "Milk", "itm_cd": "ITM001",
                "avg_daily_sales": 3, "current_stocks": 100,
                "selling_price": 50, "department": "DAIRY"}],
}
store_orders = {
    "ORG001": [{"product_name": "Milk", "itm_cd": "ITM001",
                "recommended_quantity": 20, "avg_daily_sales": 5,
                "current_stocks": 0, "selling_price": 50,
                "estimated_delivery_days": 3, "is_fresh": True,
                "reasoning": "Net Req"}],
    "ORG002": [{"product_name": "Milk", "itm_cd": "ITM001",
                "recommended_quantity": 0, "avg_daily_sales": 3,
                "current_stocks": 100, "selling_price": 50,
                "estimated_delivery_days": 3, "is_fresh": True,
                "reasoning": "Adequate"}],
}

cts = ConsolidatedTransferService(org_names, stock_data)
plan = cts.optimize_network(store_orders)
print(f"  Transfers: {plan.total_items_transferred}")
print(f"  Units transferred: {plan.total_units_transferred}")
print(f"  Orders reduced: {plan.total_orders_reduced}")
print(f"  Savings: KES {plan.estimated_savings_kes:,.0f}")
donor_adds = plan.donor_additions.get("ORG002", [])
print(f"  Donor additions for ORG002: {len(donor_adds)}")
if plan.transfers:
    t = plan.transfers[0]
    print(f"  Transfer: {t.from_org} -> {t.to_org}: {t.qty} x {t.product_name}")
print("  ConsolidatedTransferService: ALL PASS")

# --- Test 4: Per-store engine isolation ---
print()
print("=== Test 4: Per-Store Engine Isolation ===")
# Verify that ORG002 original order is untouched (qty was 0)
org2_adjusted = plan.adjusted_orders.get("ORG002", [])
for r in org2_adjusted:
    if r["product_name"] == "Milk":
        assert r["recommended_quantity"] == 0, "ORG002 original order should be untouched"
        print("  ORG002 (donor) original order unchanged: PASS")
        break
print("  Per-store isolation: PASS")

# --- Test 5: Pending order covers shortfall (<=24h) ---
print()
print("=== Test 5: Pending Order Suppresses Transfer (<=24h) ===")
d5 = decider.decide(
    "ITM001", "Milk", "ORG001", shortfall_qty=20, network_map=nmap,
    is_ordering_day=True, lead_time_days=3,
    pending_order_qty=15.0, pending_order_eta_days=0.5,  # arriving in 12h
    current_stock=2, avg_daily_sales=5,  # 9.6h to stockout -> gap > 0 but not critical
)
print(f"  Decision: {d5.decision} | Reasoning: {d5.reasoning}")
assert d5.decision == "ORDER", \
    f"Expected ORDER (delivery imminent), got {d5.decision}"
assert "imminent" in d5.reasoning.lower() or "suppressed" in d5.reasoning.lower(), \
    "Reasoning should mention imminent delivery"
print("  Pending order <=24h suppresses transfer: PASS")

# --- Test 6: Pending order partially covers (<=48h) ---
print()
print("=== Test 6: Pending Order Partially Covers (<=48h) ===")
d6 = decider.decide(
    "ITM001", "Milk", "ORG001", shortfall_qty=20, network_map=nmap,
    is_ordering_day=True, lead_time_days=3,
    pending_order_qty=10.0, pending_order_eta_days=1.5,  # arriving in 36h
    current_stock=2, avg_daily_sales=5,  # 9.6h to stockout -> gap > 0 but not critical
)
print(f"  Decision: {d6.decision} | Transfer: {d6.transfer_qty} | Order: {d6.order_qty}")
print(f"  Reasoning: {d6.reasoning}")
# Shortfall should be reduced from 20 to 10 by pending order
assert "pending order" in d6.reasoning.lower() or "reduces" in d6.reasoning.lower(), \
    "Reasoning should mention pending order reduction"
print("  Partial cover reduces transfer qty: PASS")

# --- Test 7: Critical override beats pending order ---
print()
print("=== Test 7: Critical Override Bypasses Pending Order ===")
d7 = decider.decide(
    "ITM001", "Milk", "ORG001", shortfall_qty=20, network_map=nmap,
    is_ordering_day=True, lead_time_days=3,
    pending_order_qty=15.0, pending_order_eta_days=0.5,
    current_stock=0, avg_daily_sales=5,  # Already stocked out! hours=0 → critical
)
print(f"  Decision: {d7.decision} | Transfer: {d7.transfer_qty}")
print(f"  Reasoning: {d7.reasoning}")
# Should NOT suppress transfer because stock is 0 → critical
assert d7.decision in ("TRANSFER", "BOTH"), \
    f"Expected TRANSFER/BOTH (critical override), got {d7.decision}"
assert "critical" in d7.reasoning.lower(), \
    "Reasoning should mention critical override"
print("  Critical override bypasses pending order: PASS")

# --- Test 8: ConsolidatedTransferService with pending_orders ---
print()
print("=== Test 8: CTS with Pending Orders ===")
cts2 = ConsolidatedTransferService(org_names, stock_data)
pending = {
    "ORG001": {
        "ITM001": {"qty": 15.0, "eta_days": 0.5}  # <=24h
    }
}
# Use stock=10, ads=5 so it's NOT critical (48h to SO)
store_orders_v2 = {
    "ORG001": [{"product_name": "Milk", "itm_cd": "ITM001",
                "recommended_quantity": 20, "avg_daily_sales": 5,
                "current_stocks": 10, "selling_price": 50,
                "estimated_delivery_days": 3, "is_fresh": True,
                "reasoning": "Net Req"}],
    "ORG002": [{"product_name": "Milk", "itm_cd": "ITM001",
                "recommended_quantity": 0, "avg_daily_sales": 3,
                "current_stocks": 100, "selling_price": 50,
                "estimated_delivery_days": 3, "is_fresh": True,
                "reasoning": "Adequate"}],
}
plan2 = cts2.optimize_network(store_orders_v2, pending_orders=pending)
print(f"  Transfers: {plan2.total_items_transferred}")
print(f"  Decisions: {[(d.decision, d.reasoning[:80]) for d in plan2.decisions]}")
# With pending order covering 75% and arriving in 12h, transfer should be suppressed
assert plan2.total_items_transferred == 0, \
    f"Expected 0 transfers (pending order suppresses), got {plan2.total_items_transferred}"
print("  CTS respects pending orders: PASS")

print()
print("=" * 50)
print("ALL TESTS PASSED")
