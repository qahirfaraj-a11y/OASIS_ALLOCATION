"""
Analyze User Simulation File (19:45 PM)
=======================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_194505.xlsx"

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

    # Check Day 3 Pinch Point
    d3 = df_daily[df_daily['day'] == 3]
    if not d3.empty:
        print(f"\nDay 3 Performance: {d3.iloc[0]['fill_rate']:.1f}% Fill Rate, {d3.iloc[0]['stockout_count']} Stockouts")

    # 3. Stockout Analysis
    df_sku = pd.read_excel(sim_file, sheet_name='SKU_Final_State')
    stockouts = df_sku[df_sku['stockout_days'] > 0]
    
    print(f"\nTotal SKUs with Stockouts: {len(stockouts)}")
    if len(stockouts) > 0:
        print("\nTop 5 Stockout Items:")
        print(stockouts[['product_name', 'department', 'stockout_days', 'lost_sales']].nlargest(5, 'lost_sales'))

except Exception as e:
    print(f"Error reading file: {e}")
