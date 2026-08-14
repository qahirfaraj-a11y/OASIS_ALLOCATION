"""
Run Seed Simulations Across All Tiers
======================================
Runs a 30-day simulation for each store tier to identify gaps and stockout patterns.
Generated reports will help identify systematic issues across different scales.
"""

import sys
import os
import pandas as pd
import time
from datetime import datetime

# Ensure app path. The old two-step dance (own dir, then parent "if oasis is
# not found") pointed at the repo root only by accident of living there; from
# devkit/ the parent step reached ABOVE the repo. One explicit hop instead.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # devkit/ -> repo root

from oasis.simulation.simulation_engine import SalesSimulator, InventoryTracker, ReplenishmentLogic
from oasis.logic.order_engine import OrderEngine

# Configuration
TIERS = {
    "Micro_100k": 100_000,
    "Small_200k": 200_000,
    "Medium_1M": 1_000_000,
    "Large_10M": 10_000_000,
    "Mega_100M": 100_000_000,
}

DAYS_TO_SIMULATE = 30 
OUTPUT_DIR = "simulation_reports"
DATA_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # repo root
SCORECARD_FILE = os.path.join(DATA_DIR, "Full_Product_Allocation_Scorecard_v3.csv")

if not os.path.exists(OUTPUT_DIR):
    os.makedirs(OUTPUT_DIR)

print("=" * 80)
print("MULTI-TIER SEED SIMULATION RUNNER")
print("=" * 80)

# Load Scorecard Data
print(f"Loading scorecard from: {SCORECARD_FILE}")
if not os.path.exists(SCORECARD_FILE):
    print("❌ Scorecard file not found!")
    sys.exit(1)

df_scorecard = pd.read_csv(SCORECARD_FILE)
print(f"Loaded {len(df_scorecard)} products.")

# Prepare Recommendations Lists
recommendations = []
for _, row in df_scorecard.iterrows():
    rec = {
        'product_name': row.get('Product'),
        'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
        'product_category': row.get('Department', 'GENERAL'),
        'pack_size': 1,
        'moq_floor': 0,
        'historical_order_count': 0,
        'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
        'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
        'recommended_quantity': 0,
        'reasoning': ''
    }
    recommendations.append(rec)

results_summary = []

# Initialize Engine
engine = OrderEngine(DATA_DIR)

for tier_name, budget in TIERS.items():
    print(f"\nRunning Tier: {tier_name} (Budget: KES {budget:,.0f})")
    start_time = time.time()
    
    # 1. Allocation
    print("  > Generating Allocation...")
    # Make a fresh copy of recommendations to avoid carry-over
    tier_recs = [r.copy() for r in recommendations]
    
    allocation_res = engine.apply_greenfield_allocation(tier_recs, budget)
    
    # Extract recommendations
    allocation = allocation_res # It returns a list of dicts directly in current version logic
    if isinstance(allocation_res, dict) and 'recommendations' in allocation_res:
         allocation = allocation_res['recommendations']
    
    # Filter for approved items (qty > 0)
    approved_allocation = [item for item in allocation if item['recommended_quantity'] > 0]
    sku_count = len(approved_allocation)
    print(f"  > Allocated {sku_count} SKUs")
    
    if sku_count == 0:
        print("  ⚠️ No items allocated! Skipping simulation.")
        results_summary.append({
            "Tier": tier_name,
            "Budget": budget,
            "SKUs": 0,
            "Fill Rate": 0,
            "Stockouts": 0,
            "Revenue": 0,
            "Status": "Failed - No Allocation"
        })
        continue

    # 2. Simulation
    print(f"  > Running {DAYS_TO_SIMULATE}-Day Simulation...")
    simulator = SalesSimulator(seed=42)
    inventory = InventoryTracker()
    replenishment = ReplenishmentLogic(check_frequency_days=1)
    
    # Initialize
    inventory.initialize_stock(approved_allocation)
    
    # Queue for orders
    if not hasattr(inventory, 'order_queue'):
        inventory.order_queue = []
    
    daily_stats = []
    
    # Run Days
    for day in range(1, DAYS_TO_SIMULATE + 1):
        month_factor = 1.0 
        
        # Process Sales
        daily_sum = inventory.process_daily_sales(simulator, day, month_factor)
        
        # Replenishment
        orders = replenishment.check_for_reorder(inventory.inventory, day, month_factor, simulator)
        
        # Add orders to queue
        for order in orders:
            sku = order['sku']
            p_data = inventory.inventory.get(sku, {})
            lead_time = p_data.get('lead_time_days', 3)
            arrival_day = day + lead_time
            
            inventory.order_queue.append({
                'sku': sku,
                'qty': order['qty'],
                'arrival_day': arrival_day
            })
            
        # Process Arrivals
        arrived_orders = [o for o in inventory.order_queue if o['arrival_day'] <= day]
        inventory.order_queue = [o for o in inventory.order_queue if o['arrival_day'] > day]
        
        for arr in arrived_orders:
            if arr['sku'] in inventory.inventory:
                inventory.inventory[arr['sku']]['current_stock'] += arr['qty']

        # Stats
        daily_stats.append(daily_sum)

    # 3. Analysis
    df_daily = pd.DataFrame(daily_stats)
    total_rev = df_daily['revenue'].sum()
    total_lost = df_daily['lost_revenue'].sum()
    avg_fill = (1 - (total_lost / (total_rev + total_lost))) * 100 if (total_rev + total_lost) > 0 else 100.0
    total_stockouts = df_daily['stockouts'].sum()
    
    # Save Report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M")
    report_file = os.path.join(OUTPUT_DIR, f"{tier_name}_seed_{timestamp}.csv")
    df_daily.to_csv(report_file, index=False)
    
    elapsed = time.time() - start_time
    print(f"  Tier Complete ({elapsed:.1f}s)")
    print(f"    Fill Rate: {avg_fill:.1f}% | Stockouts: {total_stockouts} | Revenue: KES {total_rev:,.0f}")
    
    results_summary.append({
        "Tier": tier_name,
        "Budget": budget,
        "SKUs": sku_count,
        "Fill Rate": avg_fill,
        "Stockouts": total_stockouts,
        "Revenue": total_rev,
        "Status": "Success",
        "Report": report_file
    })

# Final Summary
print("\n" + "=" * 80)
print("MULTI-TIER RUN SUMMARY")
print("=" * 80)
df_summary = pd.DataFrame(results_summary)
print(df_summary.to_string(index=False))

# Identify Gaps
print("\nGAP ANALYSIS:")
low_fill_tiers = df_summary[df_summary['Fill Rate'] < 95]
if not low_fill_tiers.empty:
    print("Tiers with Fill Rate < 95%:")
    print(low_fill_tiers[['Tier', 'Fill Rate', 'Stockouts']].to_string(index=False))
else:
    print("All tiers achieved > 95% fill rate!")

print("\nDone.")
