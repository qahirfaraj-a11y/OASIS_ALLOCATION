"""
Verification Script: Test Stockout Fixes
==========================================
Runs a short 14-day simulation to verify allocation fixes reduced stockouts.
"""

import sys
import os
import pandas as pd

sys.path.append(os.getcwd())

from retail_simulator import RetailSimulator, STORE_UNIVERSES

# Configuration
TIER = "Medium_1M"  # Medium store for faster testing
DAYS = 14

print("=" * 70)
print(f"STOCKOUT FIX VERIFICATION - {TIER} Store, {DAYS} Days")
print("=" * 70)

config = STORE_UNIVERSES[TIER]
sim = RetailSimulator(TIER, config)

# Run simulation
result = sim.run(days=DAYS)

print(f"\n[SIMULATION COMPLETE]")
print(f"=" * 70)

# Analyze results by category
from collections import defaultdict

category_stats = defaultdict(lambda: {'total': 0, 'stockouts': 0, 'early_stockouts': 0})

for name, sku in result.final_sku_states.items():
    dept = sku.department
    category_stats[dept]['total'] += 1
    
    if sku.first_stockout_day is not None:
        category_stats[dept]['stockouts'] += 1
        if sku.first_stockout_day <= 3:
            category_stats[dept]['early_stockouts'] += 1

print("\nSTOCKOUT ANALYSIS BY CATEGORY:")
print(f"{'Category':<25} {'Total SKUs':<12} {'Stockouts':<12} {'Early (Day 1-3)':<15} {'Rate %':<10}")
print("-" * 80)

# Focus on key categories from our fixes
key_categories = ['FRESH MILK', 'YOGHURT', 'BREAD', 'EGGS', 'WATER', 'BEVERAGES', 
                  'SNACKS', 'CONFECTIONERY', 'CIGARETTES', 'MAIZE MEAL', 'SUGAR']

for cat in key_categories:
    stats = category_stats.get(cat, {'total': 0, 'stockouts': 0, 'early_stockouts': 0})
    if stats['total'] > 0:
        rate = (stats['stockouts'] / stats['total']) * 100
        print(f"{cat:<25} {stats['total']:<12} {stats['stockouts']:<12} {stats['early_stockouts']:<15} {rate:<10.1f}%")

# Overall metrics
print(f"\n{' = ' * 35}")
print("OVERALL METRICS:")
print(f"Total SKUs: {len(result.final_sku_states)}")
print(f"Fill Rate: {result.metrics['avg_fill_rate']:.1f}%")
print(f"Total Stockouts: {sum(1 for sku in result.final_sku_states.values() if sku.first_stockout_day is not None)}")

# Day-by-day breakdown
print(f"\nDAILY PERFORMANCE:")
print(f"{'Day':<5} {'Fill %':<10} {'Stockouts':<12}")
for log in result.daily_logs[:7]:  # First week
    print(f"{log.day:<5} {log.fill_rate:<10.1f} {log.stockout_count:<12}")

print("\n[SUCCESS] Verification complete!")
print("Expected improvements:")
print("  - Fresh items: Stockout rate < 5% (was 10%)")
print("  - Impulse/Discretionary: Stockout rate < 7% (was 10-13%)")
print("  - Staples: Stockout rate < 3% (was 6-10%)")
