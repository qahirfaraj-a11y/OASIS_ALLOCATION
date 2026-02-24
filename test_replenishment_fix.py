"""
Quick Test: Verify Replenishment Fixes
========================================
Runs a short 10-day simulation to verify stockout fixes are working.
"""

import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine
from oasis.data.loader import DatabaseLoader
from oasis.simulation.simulation_engine import SimulationEngine, SalesSimulator, InventoryTracker, ReplenishmentLogic
import pandas as pd

print("=" * 80)
print("REPLENISHMENT FIX VERIFICATION TEST")
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

# Run allocation
budget = 1_000_000  # 1M budget (same as successful verification test)
print(f"\nRunning allocation with budget: {budget:,.0f} KES")

allocation_results = engine.apply_greenfield_allocation(
    recommendations=candidates,
    total_budget=budget
)

print(f"Allocated {len(allocation_results)} SKUs")

# Run 10-day simulation
print("\n" + "=" * 80)
print("RUNNING 10-DAY SIMULATION")
print("=" * 80)

simulator = SalesSimulator(seed=42)
inventory = InventoryTracker()
replenishment = ReplenishmentLogic(check_frequency_days=1)

# Initialize inventory
inventory.initialize_stock(allocation_results)

daily_results = []

for day in range(1, 11):  # Days 1-10
    # Month factor (January = 1.0)
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
    
    # Receive stock (simple: assume 0 lead time for test)
    for order in orders:
        sku = order['sku']
        qty = order['qty']
        if sku in inventory.inventory:
            inventory.inventory[sku]['current_stock'] += qty
    
    # Record
    fill_rate = (1 - daily_summary['lost_revenue'] / (daily_summary['revenue'] + daily_summary['lost_revenue'])) * 100 if (daily_summary['revenue'] + daily_summary['lost_revenue']) > 0 else 100
    
    daily_results.append({
        'Day': day,
        'Fill Rate %': fill_rate,
        'Stockouts': daily_summary['stockouts'],
        'Orders Placed': len(orders)
    })
    
    print(f"Day {day:2d}: Fill Rate {fill_rate:5.1f}%, Stockouts: {daily_summary['stockouts']:3d}, Orders: {len(orders):3d}")

# Summary
print("\n" + "=" * 80)
print("RESULTS SUMMARY")
print("=" * 80)

df_results = pd.DataFrame(daily_results)

avg_fill_rate = df_results['Fill Rate %'].mean()
total_stockouts = df_results['Stockouts'].sum()
worst_day = df_results['Fill Rate %'].min()

print(f"\nAverage Fill Rate: {avg_fill_rate:.1f}%")
print(f"Worst Day Fill Rate: {worst_day:.1f}%")
print(f"Total Stockouts (10 days): {total_stockouts}")

if avg_fill_rate >= 95 and worst_day >= 90:
    print("\n✓ SUCCESS: Replenishment fixes working!")
else:
    print(f"\n⚠ WARNING:Still need tuning. Target: 95%+ average, 90%+ worst day")
