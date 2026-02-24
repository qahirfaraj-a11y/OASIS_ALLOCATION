"""
Analyze User Simulation File (19:49 PM) - High Velocity Focus
===========================================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_194917.xlsx"

print("=" * 80)
print(f"ANALYZING: {sim_file}")
print("=" * 80)

try:
    # 1. Summary Sheet
    df_summary = pd.read_excel(sim_file, sheet_name='Summary')
    print("\nSummary Metrics:")
    print(df_summary)
    
    # 2. Daily Performance
    df_daily = pd.read_excel(sim_file, sheet_name='Daily_Performance')
    
    print("\nDaily Breakdown (First 10 Days):")
    print(f"{'Day':<5} {'Fill Rate':<10} {'Stockouts':<10} {'Lost Rev':<15}")
    print("-" * 50)
    
    for _, row in df_daily.head(10).iterrows():
        print(f"{int(row['day']):<5} {row['fill_rate']:<10.1f} {int(row['stockout_count']):<10} KES {row['lost_sales']:<15,.0f}")

    # 3. Stockout Analysis (Deep Dive)
    df_sku = pd.read_excel(sim_file, sheet_name='SKU_Final_State')
    
    # Filter for Stockouts
    stockouts = df_sku[df_sku['stockout_days'] > 0].copy()
    
    # Filter for Day 1 Stockouts (implied by high lost sales on Day 1?)
    # We don't have per-day SKU log in Excel, but 'stockout_days' > 0 and 'lost_sales' high suggests it.
    
    print(f"\nTotal SKUs with Stockouts: {len(stockouts)}")
    
    print("\nTop 10 High Value Stockouts:")
    print(stockouts[['product_name', 'department', 'unit_price', 'total_demand', 'lost_sales', 'stockout_days']].nlargest(10, 'lost_sales'))
    
    # Specific Check for Brookside
    print("\nSpecific Check: 'BROOKSIDE'")
    brookside = df_sku[df_sku['product_name'].str.contains("BROOKSIDE", case=False, na=False)]
    if not brookside.empty:
        print(brookside[['product_name', 'department', 'current_stock', 'total_demand', 'lost_sales', 'stockout_days', 'orders_placed']])
    else:
        print("No Brookside items found in SKU list.")

except Exception as e:
    print(f"Error reading file: {e}")
