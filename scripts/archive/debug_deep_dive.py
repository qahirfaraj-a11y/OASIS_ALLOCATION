
import logging
import os
import sys
import pandas as pd
from typing import List, Dict

# Setup Path
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))

from oasis.logic.order_engine import OrderEngine
from oasis.simulation.simulation_engine import SalesSimulator, InventoryTracker, RiskModel, ReplenishmentLogic
from oasis.simulation.data_loader import HistoricalDataLoader

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("DeepDive")

def run_deep_dive():
    """
    Simulates a single item (Milk) with mismatch logic to prove the crash.
    """
    budget_override = 7000000.0 # 7M (Crash Zone)
    target_month = "FEB" 
    
    logger.info(f"--- DEEP DIVE: Analyzing Supply Chain Logic (Budget: ${budget_override:,.0f}) ---")
    
    scratch_dir = "C:/Users/iLink/.gemini/antigravity/scratch"
    engine = OrderEngine(scratch_dir)
    engine.load_local_databases()
    
    # 1. Manually Construct a "Fresh Milk" Record
    milk_rec = {
        'product_name': 'FRESH MILK 500ML',
        'item_code': 'MILK001',
        'product_category': 'DAIRY',
        'supplier_name': 'DAIRY CO',
        'avg_daily_sales': 100.0,  # High Velocity
        'selling_price': 50.0,
        'current_stock': 0,
        'is_staple': True,
        'lead_time_days': 1.0, # CRITICAL: Lead Time = 1
        'margin_pct': 10.0
    }
    
    # Run Allocation (Pass 1)
    # We want to see what buffer it gets.
    # Launch Buffer = LeadTime(1) + 2 = 3 Days Coverage.
    # Quantity = 100 * 3 = 300 units.
    
    logger.info("Step 1: Allocation Logic")
    # Mocking the engine call for a single item context
    # But let's use the real engine input list to assert internal logic
    res = engine.apply_greenfield_allocation([milk_rec], total_budget=budget_override)
    allocated_rec = res['recommendations'][0]
    
    qty = allocated_rec['recommended_quantity']
    logger.info(f"Item: {milk_rec['product_name']}")
    logger.info(f"Actual Lead Time: {milk_rec['lead_time_days']} Days")
    logger.info(f"Allocated Qty: {qty} units")
    logger.info(f"Implied Coverage: {qty / 100.0:.1f} Days")
    
    # 2. Simulate the Crash
    logger.info("\nStep 2: Simulating 'Hardcoded 3-Day Delivery' Crash...")
    tracker = InventoryTracker()
    tracker.initialize_stock([allocated_rec])
    
    sales_sim = SalesSimulator(seed=42)
    replenisher = ReplenishmentLogic(check_frequency_days=1)
    month_factor = 1.0
    
    # Trace outcomes
    for day in range(1, 8):
        # Morning Receive
        received = tracker.receive_stock(day)
        if received:
             logger.info(f"   [MORNING] Received Stock! New Level: {tracker.inventory['FRESH MILK 500ML']['current_stock']}")
        
        # Check Orders (Evening)
        # We check BEFORE sales to mimic 'Morning/Day' ordering flow or vice versa?
        # Sim engine usually does Sales THEN Reorder.
        
        # Sales
        stats = tracker.process_daily_sales(sales_sim, day, month_factor=month_factor)
        current_stock = tracker.inventory['FRESH MILK 500ML']['current_stock']
        logger.info(f"Day {day}: Sold {stats['units_sold']}, Stockouts {stats['stockouts']}, EndStock {current_stock}")
        
        # Reorder
        draft_orders = replenisher.check_for_reorder(tracker.inventory, day_index=day, month_factor=month_factor)
        if draft_orders:
            for do in draft_orders:
                # REPRODUCING THE BUG: Hardcoded +3
                arrival = day + 3 
                logger.info(f"   [ORDER] Triggered {do['qty']} units. Arriving Day {arrival} (Fixed lag)")
                tracker.pending_orders.append({
                    'sku': do['sku'],
                    'qty': do['qty'],
                    'arrival_day': arrival
                })

    logger.info("\nAnalysis: If EndStock hit 0 before Day 4 Arrival, the 'Gap' is confirmed.")

if __name__ == "__main__":
    run_deep_dive()
