"""
Quick Simulation: Test Precision Allocation Fix
================================================
Runs small 14-day simulation to verify fill rates improved.
"""

import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine
from oasis.data.loader import DatabaseLoader
from oasis.simulation.simulation_engine import SalesSimulator, InventoryTracker, ReplenishmentLogic
import pandas as pd

print("=" * 80)
print("PRECISION ALLOCATION - VERIFICATION TEST")
print("=" * 80)

# Load data
print("\nLoading data...")
db_loader = DatabaseLoader()
databases = db_loader.load_all_databases()

# Initialize engine
engine = OrderEngine(databases)

# Get scorecard
df_scorecard = databases['scorecard']
candidates = df_scorecard.to_dict('records')

# Run allocation with Medium_1M budget
budget = 1_000_000
print(f"\nRunning allocation with budget: {budget:,.0f} KES")
print("Using NEW precision formula...")

allocation_results = engine.apply_greenfield_allocation(
    recommendations=candidates,
    total_budget=budget
)

print(f"Allocated {len(allocation_results)} SKUs")

# Quick depth check
print("\n" + "=" * 80)
print("ALLOCATION DEPTH CHECK")
print("=" * 80)

depths = []
fresh_depths = []
longlife_depths = []

for item in allocation_results:
    coverage = item.get('target_coverage_days', 0)
    depths.append(coverage)
    
    if item.get('is_fresh'):
        fresh_depths.append(coverage)
    
    name_upper = item.get('product_name', '').upper()
    if 'UHT' in name_upper or 'ESL' in name_upper or 'LONG LIFE' in name_upper:
        longlife_depths.append(coverage)

print(f"\nOverall allocation depths:")
print(f"  Average: {sum(depths)/len(depths):.1f} days (was ~5 days)")
print(f"  Median: {sorted(depths)[len(depths)//2]:.1f} days")
print(f"  Min: {min(depths):.1f} days")
print(f"  Max: {max(depths):.1f} days")

if fresh_depths:
    print(f"\nFresh item depths:")
    print(f"  Average: {sum(fresh_depths)/len(fresh_depths):.1f} days")
    print(f"  Max: {max(fresh_depths):.1f} days (should be ≤5)")

if longlife_depths:
    print(f"\nLong-life item depths:")
    print(f"  Average: {sum(longlife_depths)/len(longlife_depths):.1f} days")
    print(f"  Max: {max(longlife_depths):.1f} days (should be ≤7)")

# Run quick 14-day simulation
print("\n" + "=" * 80)
print("RUNNING 14-DAY SIMULATION")
print("=" * 80)

simulator = SalesSimulator(seed=42)
inventory = InventoryTracker()
replenishment = ReplenishmentLogic(check_frequency_days=1)

inventory.initialize_stock(allocation_results)

daily_results = []

for day in range(1, 15):
    month_factor = 1.0
    
    # Process sales
    daily_summary = inventory.process_daily_sales(
        simulator=simulator,
        day_index=day,
        month_factor=month_factor,
        store_scale_factor=1.0
    )
    
    # Check for reorders
    orders = replenishment.check_for_reorder(
        inventory=inventory.inventory,
        day_index=day,
        month_factor=month_factor,
        sales_simulator=simulator
    )
    
    # Receive stock (assume 0 lead time for quick test)
    for order in orders:
        sku = order['sku']
        qty = order['qty']
        if sku in inventory.inventory:
            inventory.inventory[sku]['current_stock'] += qty
    
    # Calculate fill rate
    fill_rate = (1 - daily_summary['lost_revenue'] / (daily_summary['revenue'] + daily_summary['lost_revenue'])) * 100 if (daily_summary['revenue'] + daily_summary['lost_revenue']) > 0 else 100
    
    daily_results.append({
        'Day': day,
        'Fill Rate %': fill_rate,
        'Stockouts': daily_summary['stockouts']
    })
    
    print(f"Day {day:2d}: Fill Rate {fill_rate:5.1f}%, Stockouts: {daily_summary['stockouts']:3d}")

# Summary
print("\n" + "=" * 80)
print("RESULTS")
print("=" * 80)

df_results = pd.DataFrame(daily_results)
avg_fill_rate = df_results['Fill Rate %'].mean()
worst_day = df_results['Fill Rate %'].min()
total_stockouts = df_results['Stockouts'].sum()

print(f"\nAverage Fill Rate: {avg_fill_rate:.1f}% (target: >95%)")
print(f"Worst Day Fill Rate: {worst_day:.1f}%")
print(f"Total Stockouts (14 days): {total_stockouts}")

if avg_fill_rate >= 95:
    print("\n✓ SUCCESS: Fill rate target achieved!")
else:
    print(f"\n⚠ Still needs tuning. Gap: {95 - avg_fill_rate:.1f}pp")

print("\n" + "=" * 80)
print("NEXT STEP: Run full 30-day simulation with your standard config")
print("=" * 80)
