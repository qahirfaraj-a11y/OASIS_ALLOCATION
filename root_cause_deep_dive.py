"""
Root Cause Investigation: Why 67% of SKUs Stock Out Days 1-7
==============================================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"

print("=" * 80)
print("ROOT CAUSE INVESTIGATION")
print("=" * 80)

# Load data
df_sku = pd.read_excel(sim_file, sheet_name='SKU_Final_State')

# Find columns
stockout_col = [c for c in df_sku.columns if 'first' in c.lower() and 'stockout' in c.lower()][0]

# Analyze early stockouts
early_stockouts = df_sku[(df_sku[stockout_col].notna()) & (df_sku[stockout_col] <= 7)]

print(f"\n{len(early_stockouts)} SKUs with Day 1-7 stockouts")
print("\nHYPOTHESIS TESTING:")

# Hypothesis 1: Initial allocation too shallow
print("\n" + "=" * 80)
print("HYPOTHESIS 1: Allocation Depths Too Shallow")
print("=" * 80)

if 'Initial Stock' in df_sku.columns and 'ADS' in df_sku.columns:
    early_stockouts['days_coverage'] = early_stockouts['Initial Stock'] / early_stockouts['ADS']
    
    avg_coverage = early_stockouts['days_coverage'].mean()
    median_coverage = early_stockouts['days_coverage'].median()
    
    print(f"\nStockout SKUs - Initial Coverage:")
    print(f"  Average: {avg_coverage:.1f} days")
    print(f"  Median: {median_coverage:.1f} days")
    
    # Compare to non-stockout SKUs
    no_stockouts = df_sku[df_sku[stockout_col].isna()]
    if len(no_stockouts) > 0:
        no_stockouts['days_coverage'] = no_stockouts['Initial Stock'] / no_stockouts['ADS']
        no_stockout_avg = no_stockouts['days_coverage'].mean()
        
        print(f"\nNon-Stockout SKUs - Initial Coverage:")
        print(f"  Average: {no_stockout_avg:.1f} days")
        
        print(f"\nDifference: {no_stockout_avg - avg_coverage:.1f} days")
        
        if avg_coverage < 7:
            print(f"\n🚨 FINDING: Stockout SKUs allocated <7 days coverage!")
            print(f"   For lead time of 2-7 days, this is too shallow")

# Hypothesis 2: Demand scaling mismatch
print("\n" + "=" * 80)
print("HYPOTHESIS 2: Demand Scaling Mismatch")
print("=" * 80)

if 'Total Demand' in df_sku.columns and 'ADS' in df_sku.columns:
    # Calculate implied daily demand from total
    early_stockouts['implied_daily_demand'] = early_stockouts['Total Demand'] / 30
    early_stockouts['demand_ratio'] = early_stockouts['implied_daily_demand'] / early_stockouts['ADS']
    
    avg_ratio = early_stockouts['demand_ratio'].mean()
    
    print(f"\nDemand Scaling Analysis (30-day simulation):")
    print(f"  Allocated ADS vs Actual Demand ratio: {avg_ratio:.2f}x")
    
    if avg_ratio > 1.2:
        print(f"\n🚨 FINDING: Actual demand {avg_ratio:.2f}x higher than allocated for!")
        print(f"   Allocation used ADS, but simulation applied higher factor")
    elif avg_ratio < 0.8:
        print(f"\n✓ Demand scaling OK (actually lower than allocated)")

# Hypothesis 3: Velocity distribution
print("\n" + "=" * 80)
print("HYPOTHESIS 3: High-Velocity Item Distribution")
print("=" * 80)

if 'ADS' in df_sku.columns:
    # Velocity brackets
    early_stockouts['velocity_bracket'] = pd.cut(
        early_stockouts['ADS'],
        bins=[0, 1, 2, 5, 10, 100],
        labels=['<1', '1-2', '2-5', '5-10', '>10']
    )
    
    print("\nStockout Distribution by Velocity:")
    velocity_dist = early_stockouts['velocity_bracket'].value_counts().sort_index()
    for bracket, count in velocity_dist.items():
        pct = count / len(early_stockouts) * 100
        print(f"  {bracket} ADS: {count} SKUs ({pct:.1f}%)")
    
    high_velocity = early_stockouts[early_stockouts['ADS'] > 5.0]
    print(f"\nHigh-velocity items (>5 ADS): {len(high_velocity)} ({len(high_velocity)/len(early_stockouts)*100:.1f}%)")

# Hypothesis 4: Fresh vs Non-Fresh
print("\n" + "=" * 80)
print("HYPOTHESIS 4: Fresh Item Constraints")
print("=" * 80)

if 'Department' in df_sku.columns:
    fresh_depts = ['FRESH MILK', 'BREAD', 'YOGHURT', 'EGGS']
    early_stockouts['is_fresh_dept'] = early_stockouts['Department'].isin(fresh_depts)
    
    fresh_stockouts = early_stockouts[early_stockouts['is_fresh_dept']]
    print(f"\nFresh department stockouts: {len(fresh_stockouts)} ({len(fresh_stockouts)/len(early_stockouts)*100:.1f}%)")
    
    if 'days_coverage' in early_stockouts.columns:
        fresh_coverage = fresh_stockouts['days_coverage'].mean()
        non_fresh_coverage = early_stockouts[~early_stockouts['is_fresh_dept']]['days_coverage'].mean()
        
        print(f"  Fresh item coverage: {fresh_coverage:.1f} days")
        print(f"  Non-fresh coverage: {non_fresh_coverage:.1f} days")

# Summary
print("\n" + "=" * 80)
print("DIAGNOSTIC SUMMARY")
print("=" * 80)
print("\nPriority issues to address:")
print("1. Check coverage analysis above")
print("2. Check demand scaling ratio")
print("3. Review velocity distribution")
print("=" * 80)
