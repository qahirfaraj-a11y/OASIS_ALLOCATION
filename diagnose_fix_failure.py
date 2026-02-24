"""
Deep Diagnostic: Why Did the Fix Fail?
=======================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

file_path = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"

print("=" * 80)
print("INVESTIGATING FIX FAILURE")
print("=" * 80)

df_sku = pd.read_excel(file_path, sheet_name='SKU_Final_State')

# Get column names
stockout_day_col = [c for c in df_sku.columns if 'first' in c.lower() and 'stockout' in c.lower()][0]
orders_col = [c for c in df_sku.columns if 'order' in c.lower() and 'place' in c.lower()][0]

early_stockouts = df_sku[(df_sku[stockout_day_col].notna()) & (df_sku[stockout_day_col] <= 7)]

print(f"\nAnalyzing {len(early_stockouts)} early stockout SKUs...")

# Check if order behavior changed
print("\n" + "=" * 80)
print("ORDER BEHAVIOR CHECK")
print("=" * 80)

avg_orders = early_stockouts[orders_col].mean()
max_orders = early_stockouts[orders_col].max()
min_orders = early_stockouts[orders_col].min()

print(f"\nOrders placed for stockout SKUs:")
print(f"  Average: {avg_orders:.1f}")
print(f"  Range: {int(min_orders)}-{int(max_orders)}")
print(f"\n🔍 BEFORE FIX: 26-30 orders")
print(f"🔍 AFTER FIX:  {int(min_orders)}-{int(max_orders)} orders")

if abs(avg_orders - 28) < 2:
    print(f"\n⚠️  ORDER COUNT UNCHANGED! Fix may not be applying.")
else:
    print(f"\n✓ Order count changed")

# Check specific SKU details
print("\n" + "=" * 80)
print("SAMPLE SKU DEEP DIVE (First 10 stockouts)")
print("=" * 80)

# Get relevant columns
display_cols = []
for col in ['Product', 'Department', 'ADS', 'Lead Time', stockout_day_col, orders_col, 'Total Demand', 'Total Sales', 'Lost Sales']:
    if col in df_sku.columns:
        display_cols.append(col)

print(early_stockouts[display_cols].head(10).to_string(index=False))

# Calculate some key ratios
print("\n" + "=" * 80)
print("KEY RATIOS")
print("=" * 80)

if 'Total Demand' in df_sku.columns and 'Total Sales' in df_sku.columns:
    early_stockouts['fill_rate'] = early_stockouts['Total Sales'] / early_stockouts['Total Demand'] * 100
    avg_fill = early_stockouts['fill_rate'].mean()
    
    print(f"\nAverage fill rate for stockout SKUs: {avg_fill:.1f}%")
    print(f"This suggests orders are {100-avg_fill:.1f}% short on average")

# Hypothesis testing
print("\n" + "=" * 80)
print("HYPOTHESIS TESTING")
print("=" * 80)

print("\nPossible root causes:")
print("  1. Simulation not using updated code")
print("  2. Lead times too long (orders arrive after stockout)")
print("  3. Demand scale factor too high")
print("  4. Initial allocation too shallow")
print("  5. Order frequency too low (only daily checks)")

# Check lead time distribution
if 'Lead Time' in early_stockouts.columns:
    avg_lead = early_stockouts['Lead Time'].mean()
    print(f"\n  Average Lead Time: {avg_lead:.1f} days")
    print(f"  If lead time > stockout day, orders can't help")
    
    # Check if lead time explains stockouts
    early_stockouts['lead_vs_stockout'] = early_stockouts['Lead Time'] >= early_stockouts[stockout_day_col]
    lead_too_long = early_stockouts['lead_vs_stockout'].sum()
    
    print(f"  SKUs where lead_time >= first_stockout_day: {lead_too_long} ({lead_too_long/len(early_stockouts)*100:.1f}%)")
    
    if lead_too_long > len(early_stockouts) * 0.5:
        print(f"\n🚨 CRITICAL: Lead times are too long!")
        print(f"   Solution: Need to increase INITIAL allocation depth, not just replenishment")

print("\n" + "=" * 80)
