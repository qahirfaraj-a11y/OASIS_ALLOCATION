"""
Root Cause Analysis - Corrected with Actual Columns
====================================================
"""

import pandas as pd
import numpy as np
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"

print("=" * 80)
print("ROOT CAUSE INVESTIGATION - CORRECTED")
print("="* 80)

# Load SKU data
df_sku = pd.read_excel(sim_file, sheet_name='SKU_Final_State')

# Analyze early stockouts
early_stockouts = df_sku[(df_sku['first_stockout_day'].notna()) & (df_sku['first_stockout_day'] <= 7)].copy()

print(f"\n{len(early_stockouts)} SKUs with Day 1-7 stockouts (out of {len(df_sku)} total)")

# Hypothesis 1: Demand Scaling Mismatch
print("\n" + "=" * 80)
print("HYPOTHESIS 1: Demand Scaling Mismatch")
print("=" * 80)

# Calculate implied daily demand from 30-day total
early_stockouts['implied_daily_demand'] = early_stockouts['total_demand'] / 30
early_stockouts['demand_vs_allocated'] = early_stockouts['implied_daily_demand'] / early_stockouts['avg_daily_sales']

avg_demand_ratio = early_stockouts['demand_vs_allocated'].median()
print(f"\nDemand Analysis (30-day simulation):")
print(f"  Median ratio of Actual Demand / Allocated ADS: {avg_demand_ratio:.2f}x")
print(f"  Mean ratio: {early_stockouts['demand_vs_allocated'].mean():.2f}x")

if avg_demand_ratio > 1.1:
    print(f"\n🚨 CRITICAL FINDING: Actual demand is {avg_demand_ratio:.2f}x what was allocated for!")
    print(f"   This means allocation used lower ADS than simulation demand")
    print(f"   Gap: {(avg_demand_ratio - 1) * 100:.0f}% higher demand than expected")
else:
    print(f"\n✓ Demand scaling appears OK")

# Show examples
print(f"\nWorst 10 examples (highest demand vs allocated):")
worst = early_stockouts.nlargest(10, 'demand_vs_allocated')[['product_name', 'avg_daily_sales', 'implied_daily_demand', 'demand_vs_allocated', 'first_stockout_day']]
print(worst.to_string(index=False))

# Hypothesis 2: Allocation Formula Inadequacy
print("\n" + "=" * 80)
print("HYPOTHESIS 2: Allocated Quantity Too Low")
print("=" * 80)

# Calculate what initial stock SHOULD have been
# Initial stock = total_sales + lost_sales (before first stockout)
early_stockouts['cumulative_demand_at_stockout'] = early_stockouts['avg_daily_sales'] * early_stockouts['first_stockout_day']

# Estimate initial stock based on total sales
# Assumption: most sales happened before stockout
early_stockouts['estimated_initial_stock'] = early_stockouts['total_sales'] / 30 * early_stockouts['first_stockout_day']

# Days of coverage (rough estimate)
early_stockouts['estimated_days_coverage'] = early_stockouts['estimated_initial_stock'] / early_stockouts['avg_daily_sales']

avg_coverage = early_stockouts['estimated_days_coverage'].median()
print(f"\nEstimated Initial Allocation Coverage:")
print(f"  Median: {avg_coverage:.1f} days")
print(f"  Mean: {early_stockouts['estimated_days_coverage'].mean():.1f} days")

if avg_coverage < 7:
    print(f"\n🚨 CRITICAL: Allocated <7 days of stock!")
    print(f"   With lead times of 2-7 days, items run out before replenishment arrives")

# Hypothesis 3: High-Velocity Distribution
print("\n" + "=" * 80)
print("HYPOTHESIS 3: Velocity Distribution")
print("=" * 80)

early_stockouts['velocity_category'] = pd.cut(
    early_stockouts['avg_daily_sales'],
    bins=[0, 1, 2, 5, 10, 1000],
    labels=['Low (<1)', 'Medium (1-2)', 'Med-High (2-5)', 'High (5-10)', 'Very High (>10)']
)

print("\nStockout distribution by velocity:")
for cat in ['Low (<1)', 'Medium (1-2)', 'Med-High (2-5)', 'High (5-10)', 'Very High (>10)']:
    count = (early_stockouts['velocity_category'] == cat).sum()
    pct = count / len(early_stockouts) * 100
    print(f"  {cat}: {count} SKUs ({pct:.1f}%)")

high_vel = early_stockouts[early_stockouts['avg_daily_sales'] > 5.0]
print(f"\nHigh-velocity (>5 ADS) stockouts: {len(high_vel)} ({len(high_vel)/len(early_stockouts)*100:.1f}%)")

# Hypothesis 4: Fresh Items
print("\n" + "=" * 80)
print("HYPOTHESIS 4: Fresh vs Non-Fresh")
print("=" * 80)

fresh_stockouts = early_stockouts[early_stockouts['is_fresh'] == True]
non_fresh_stockouts = early_stockouts[early_stockouts['is_fresh'] == False]

print(f"\nFresh item stockouts: {len(fresh_stockouts)} ({len(fresh_stockouts)/len(early_stockouts)*100:.1f}%)")
print(f"Non-fresh stockouts: {len(non_fresh_stockouts)} ({len(non_fresh_stockouts)/len(early_stockouts)*100:.1f}%)")

if len(fresh_stockouts) > 0:
    fresh_avg_day = fresh_stockouts['first_stockout_day'].mean()
    print(f"  Fresh avg stockout day: {fresh_avg_day:.1f}")
    
if len(non_fresh_stockouts) > 0:
    non_fresh_avg_day = non_fresh_stockouts['first_stockout_day'].mean()
    print(f"  Non-fresh avg stockout day: {non_fresh_avg_day:.1f}")

# FINAL DIAGNOSIS
print("\n" + "=" * 80)
print("DIAGNOSIS & RECOMMENDED FIX")
print("=" * 80)

if avg_demand_ratio > 1.15:
    print("\n🎯 PRIMARY ISSUE: DEMAND SCALING MISMATCH")
    print(f"   Simulation demand is {avg_demand_ratio:.2f}x allocation ADS")
    print(f"\n   ROOT CAUSE:")
    print(f"   - Allocation uses 'avg_daily_sales' from scorecard")
    print(f"   - Simulation applies month_factor or scale_factor on top")
    print(f"   - Result: {(avg_demand_ratio-1)*100:.0f}% more demand than allocated for")
    print(f"\n   FIX: Reduce simulation demand scaling OR increase allocation depths by {avg_demand_ratio:.2f}x")
elif avg_coverage < 7:
    print("\n🎯 PRIMARY ISSUE: SHALLOW ALLOCATION")
    print(f"   Items allocated only ~{avg_coverage:.1f} days of stock")
    print(f"   With 2-7 day lead times, they stock out before replenishment")
    print(f"\n   FIX: Increase base allocation depth to minimum 14 days")
else:
    print("\n🎯 UNCLEAR - Multiple factors")

print("=" * 80)
