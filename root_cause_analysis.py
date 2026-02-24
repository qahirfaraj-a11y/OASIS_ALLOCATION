"""
Deep Dive: Identify Root Cause of Stockouts
=============================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

file_path = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_180448.xlsx"

print("=" * 80)
print("ROOT CAUSE ANALYSIS")
print("=" * 80)

df_sku = pd.read_excel(file_path, sheet_name='SKU_Final_State')

print(f"\nAnalyzing {len(df_sku)} SKUs...\n")

# Find stockout column
stockout_day_col = [c for c in df_sku.columns if 'first' in c.lower() and 'stockout' in c.lower()]
if stockout_day_col:
    stockout_day_col = stockout_day_col[0]
    
    # Early stockouts (Day 1-7)
    early_stockouts = df_sku[df_sku[stockout_day_col].notna() & (df_sku[stockout_day_col] <= 7)]
    
    print(f"SKUs with Day 1-7 Stockouts: {len(early_stockouts)}")
    print(f"Percentage: {len(early_stockouts)/len(df_sku)*100:.1f}%\n")
    
    # Check if orders were placed
    orders_col = [c for c in df_sku.columns if 'order' in c.lower() and 'place' in c.lower()]
    if orders_col:
        orders_col = orders_col[0]
        
        print("=" * 80)
        print("REPLENISHMENT CHECK")
        print("=" * 80)
        
        # SKUs that stocked out but had NO orders
        no_orders_stockout = early_stockouts[early_stockouts[orders_col] == 0]
        print(f"\nSKUs with stockouts but ZERO orders placed: {len(no_orders_stockout)}")
        print(f"  → This suggests replenishment logic NOT executing\n")
        
        # SKUs that stocked out despite orders
        had_orders_stockout = early_stockouts[early_stockouts[orders_col] > 0]
        print(f"SKUs with stockouts despite orders: {len(had_orders_stockout)}")
        print(f"  → This suggests orders too late or insufficient quantity\n")
    
    # Department breakdown
    if 'Department' in early_stockouts.columns:
        print("=" * 80)
        print("CATEGORY BREAKDOWN")
        print("=" * 80)
        
        dept_counts = early_stockouts['Department'].value_counts()
        print(f"\nTop 15 Affected Departments:")
        for dept, count in dept_counts.head(15).items():
            pct = count/len(early_stockouts)*100
            print(f"  {dept:<30} {count:>4} SKUs ({pct:>5.1f}%)")
    
    # Stockout timing
    print("\n" + "=" * 80)
    print("STOCKOUT TIMING")
    print("=" * 80)
    
    day_counts = early_stockouts[stockout_day_col].value_counts().sort_index()
    print(f"\nWhen did stockouts first occur?")
    for day, count in day_counts.items():
        print(f"  Day {int(day)}: {count} SKUs")
    
    # Top 25 problem SKUs
    print("\n" + "=" * 80)
    print("TOP 25 PROBLEM SKUs (Early Stockouts)")
    print("=" * 80)
    
    display_cols = []
    for col_name in ['Product', 'Department', 'ADS', stockout_day_col]:
        if col_name in early_stockouts.columns:
            display_cols.append(col_name)
    if orders_col:
        display_cols.append(orders_col)
    
    print(early_stockouts[display_cols].sort_values(stockout_day_col).head(25).to_string(index=False))

print("\n" + "=" * 80)
