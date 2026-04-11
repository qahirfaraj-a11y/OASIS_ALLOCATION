"""
Deep Dive: Missing Fresh Milk Items & Oversupply Analysis
==========================================================
Focused analysis on:
  1. Fresh Milk items in scorecard but NOT stocked at Rhapta
  2. Oversupply: items with excessive days-of-cover relative to velocity
"""
import sys, io
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')

import pandas as pd
import os

BASE_DIR = os.path.dirname(__file__)

# ── Load data ───────────────────────────────────────────────────────────────
df_sc = pd.read_csv(os.path.join(BASE_DIR, "Full_Product_Allocation_Scorecard_v7.csv"))
df_working = pd.read_csv(os.path.join(BASE_DIR, "rhapta_working_assortment.csv"))
df_comparison = pd.read_csv(os.path.join(BASE_DIR, "rhapta_scorecard_comparison.csv"))

df_sc['Product_upper'] = df_sc['Product'].str.strip().str.upper()
df_working['ITM_NAME_upper'] = df_working['ITM_NAME'].str.strip().str.upper()
working_names = set(df_working['ITM_NAME_upper'])

# Also load full stock data (including zero-stock) to check if items exist at all
df_stock_all = pd.read_csv(os.path.join(BASE_DIR, "rhapta_working_assortment.csv"))
# We need the full dept data for zero-stock items too - let's use the scorecard comparison
# to identify what's truly missing vs. what's at zero stock

# ==============================================================================
#  PART 1: MISSING FRESH MILK ITEMS
# ==============================================================================
print("=" * 90)
print("  PART 1: FRESH MILK ITEMS — SCORECARD vs. RHAPTA REALITY")
print("=" * 90)

# All fresh milk items in scorecard
fresh_depts = ['FRESH MILK', 'FRESH GOURMET', 'FRESH YOGHURT']
fresh_milk_sc = df_sc[df_sc['Department'].str.upper().isin([d.upper() for d in fresh_depts])].copy()
fresh_milk_only = df_sc[df_sc['Department'].str.upper() == 'FRESH MILK'].copy()

print(f"\n  Scorecard 'FRESH MILK' items:  {len(fresh_milk_only):>6}")
print(f"  Scorecard all fresh items:     {len(fresh_milk_sc):>6}")

# Which are stocked?
fresh_milk_only['In_Stock'] = fresh_milk_only['Product_upper'].isin(working_names)
stocked = fresh_milk_only['In_Stock'].sum()
missing = (~fresh_milk_only['In_Stock']).sum()

print(f"\n  Stocked at Rhapta:             {stocked:>6}")
print(f"  NOT stocked at Rhapta:         {missing:>6}")
print()

# Missing fresh milk - detailed breakdown
missing_milk = fresh_milk_only[~fresh_milk_only['In_Stock']].copy()
missing_milk = missing_milk.sort_values('Total_Revenue', ascending=False)

total_missing_revenue = missing_milk['Total_Revenue'].sum()
total_missing_ads = missing_milk['Avg_Daily_Sales'].sum()

print(f"  MISSING FRESH MILK - IMPACT:")
print(f"  ─────────────────────────────")
print(f"  Total missing daily demand:    {total_missing_ads:>10.1f} units/day")
print(f"  Total missing revenue:         KES {total_missing_revenue:>12,.0f}")
print()

# By priority
print(f"  Missing Fresh Milk by Priority:")
priority_grp = missing_milk.groupby('Priority_Label').agg(
    Count=('Product', 'count'),
    Total_ADS=('Avg_Daily_Sales', 'sum'),
    Total_Revenue=('Total_Revenue', 'sum'),
).sort_values('Total_Revenue', ascending=False)
for p, row in priority_grp.iterrows():
    print(f"    {p:<12} {row['Count']:>4} items | ADS: {row['Total_ADS']:>8.1f} | Rev: KES {row['Total_Revenue']:>12,.0f}")

# By supplier
print(f"\n  Missing Fresh Milk by Supplier:")
supplier_grp = missing_milk.groupby('Supplier').agg(
    Count=('Product', 'count'),
    Total_ADS=('Avg_Daily_Sales', 'sum'),
    Total_Revenue=('Total_Revenue', 'sum'),
).sort_values('Total_Revenue', ascending=False)
for s, row in supplier_grp.iterrows():
    print(f"    {s[:40]:<42} {row['Count']:>3} items | ADS: {row['Total_ADS']:>7.1f} | Rev: KES {row['Total_Revenue']:>10,.0f}")

# Top 30 missing items
print(f"\n  {'─'*90}")
print(f"  TOP 30 MISSING FRESH MILK ITEMS (by Revenue):")
print(f"  {'─'*90}")
print(f"  {'Product':<50} {'Supplier':<25} {'ADS':>6} {'Revenue':>12} {'Priority':>10}")
print(f"  {'─'*90}")
for _, r in missing_milk.head(30).iterrows():
    print(f"  {str(r['Product'])[:49]:<50} {str(r['Supplier'])[:24]:<25} {r['Avg_Daily_Sales']:>6.1f} {r['Total_Revenue']:>12,.0f} {str(r.get('Priority_Label','?')):>10}")

# Items currently stocked in Fresh Milk
stocked_milk = fresh_milk_only[fresh_milk_only['In_Stock']].copy()
stocked_milk_merged = stocked_milk.merge(
    df_working, left_on='Product_upper', right_on='ITM_NAME_upper', how='left'
)

print(f"\n\n  {'─'*90}")
print(f"  STOCKED FRESH MILK ITEMS ({stocked} items):")
print(f"  {'─'*90}")
if len(stocked_milk_merged) > 0:
    stocked_milk_merged['DaysCover'] = (
        stocked_milk_merged['STOCK'] / stocked_milk_merged['Avg_Daily_Sales'].replace(0, float('nan'))
    ).round(1)
    stocked_milk_merged = stocked_milk_merged.sort_values('Total_Revenue', ascending=False)
    print(f"  {'Product':<45} {'Stock':>6} {'ADS':>6} {'Days':>6} {'Sell Val':>10} {'Priority':>10}")
    print(f"  {'─'*90}")
    for _, r in stocked_milk_merged.head(20).iterrows():
        print(f"  {str(r['Product'])[:44]:<45} {r['STOCK']:>6,.0f} {r['Avg_Daily_Sales']:>6.1f} {r.get('DaysCover', 0):>6.1f} {r.get('StockValue', 0):>10,.0f} {str(r.get('Priority_Label','?')):>10}")


# ==============================================================================
#  PART 2: OVERSUPPLY ANALYSIS
# ==============================================================================
print()
print()
print("=" * 90)
print("  PART 2: OVERSUPPLY ANALYSIS — ITEMS WITH EXCESSIVE DAYS OF COVER")
print("=" * 90)

df_comp = df_comparison.copy()
df_comp['SC_ADS'] = pd.to_numeric(df_comp['SC_ADS'], errors='coerce').fillna(0)
df_comp['STOCK'] = pd.to_numeric(df_comp['STOCK'], errors='coerce').fillna(0)
df_comp['DaysOfCover'] = pd.to_numeric(df_comp['DaysOfCover'], errors='coerce')
df_comp['StockValue'] = pd.to_numeric(df_comp['StockValue'], errors='coerce').fillna(0)
df_comp['CostValue'] = pd.to_numeric(df_comp['CostValue'], errors='coerce').fillna(0)
df_comp['StockGap'] = pd.to_numeric(df_comp['StockGap'], errors='coerce').fillna(0)

# Define oversupply thresholds
# A (Staple): > 30 days is oversupply
# B (Core):   > 45 days is oversupply
# C (Filler): > 60 days is oversupply
# D (Risk):   > 90 days is oversupply (these are already slow movers)
thresholds = {
    'A (Staple)': 30,
    'B (Core)': 45,
    'C (Filler)': 60,
    'D (Risk)': 90,
}

print(f"\n  Oversupply Thresholds (days of cover):")
for tier, days in thresholds.items():
    print(f"    {tier}: > {days} days")

# Tag oversupply
df_comp['Oversupply_Threshold'] = df_comp['SC_Velocity'].map(thresholds)
df_comp['Is_Oversupplied'] = (
    df_comp['DaysOfCover'] > df_comp['Oversupply_Threshold']
) & (df_comp['SC_ADS'] > 0)

oversupplied = df_comp[df_comp['Is_Oversupplied']].copy()
not_oversupplied = df_comp[~df_comp['Is_Oversupplied'] & (df_comp['SC_ADS'] > 0)]

print(f"\n  Overall Oversupply Summary:")
print(f"    Items with measurable ADS:   {(df_comp['SC_ADS'] > 0).sum():>8,}")
print(f"    Oversupplied items:          {len(oversupplied):>8,} ({len(oversupplied)/(df_comp['SC_ADS'] > 0).sum()*100:.1f}%)")
print(f"    Adequately stocked items:    {len(not_oversupplied):>8,}")
print()

# Capital tied up in oversupply
oversupplied['ExcessStock'] = oversupplied['STOCK'] - (
    oversupplied['SC_ADS'] * oversupplied['Oversupply_Threshold']
)
oversupplied['ExcessStock'] = oversupplied['ExcessStock'].clip(lower=0)
oversupplied['ExcessCostValue'] = oversupplied['ExcessStock'] * oversupplied['CostPrice_Final']
oversupplied['ExcessSellValue'] = oversupplied['ExcessStock'] * oversupplied['SellPrice']

total_excess_cost = oversupplied['ExcessCostValue'].sum()
total_excess_sell = oversupplied['ExcessSellValue'].sum()

print(f"  CAPITAL TIED UP IN OVERSUPPLY:")
print(f"  ──────────────────────────────")
print(f"    Total oversupply units:      {oversupplied['ExcessStock'].sum():>12,.0f}")
print(f"    Excess at cost:              KES {total_excess_cost:>12,.0f}")
print(f"    Excess at sell:              KES {total_excess_sell:>12,.0f}")
print()

# Oversupply by velocity tier
print(f"  Oversupply by Velocity Tier:")
print(f"  {'Tier':<15} {'Items':>6} {'Avg Days':>9} {'Excess Units':>13} {'Excess Cost':>14} {'Excess Sell':>14}")
print(f"  {'─'*75}")
for tier in ['A (Staple)', 'B (Core)', 'C (Filler)', 'D (Risk)']:
    subset = oversupplied[oversupplied['SC_Velocity'] == tier]
    if len(subset) > 0:
        print(f"  {tier:<15} {len(subset):>6} {subset['DaysOfCover'].mean():>8.0f}d {subset['ExcessStock'].sum():>13,.0f} {subset['ExcessCostValue'].sum():>13,.0f} {subset['ExcessSellValue'].sum():>13,.0f}")

# Oversupply by department (top 15)
print(f"\n  Oversupply by Department (Top 15 by Excess Cost):")
dept_excess = oversupplied.groupby('DEPARTMENT').agg(
    Items=('BARCODE', 'count'),
    AvgDaysCover=('DaysOfCover', 'mean'),
    ExcessUnits=('ExcessStock', 'sum'),
    ExcessCost=('ExcessCostValue', 'sum'),
    ExcessSell=('ExcessSellValue', 'sum'),
).sort_values('ExcessCost', ascending=False)

print(f"  {'Department':<25} {'Items':>5} {'Avg Days':>9} {'Excess Units':>12} {'Excess Cost':>13} {'Excess Sell':>13}")
print(f"  {'─'*80}")
for dept, row in dept_excess.head(15).iterrows():
    print(f"  {dept[:24]:<25} {row['Items']:>5} {row['AvgDaysCover']:>8.0f}d {row['ExcessUnits']:>12,.0f} {row['ExcessCost']:>13,.0f} {row['ExcessSell']:>13,.0f}")

# Top 30 most oversupplied individual items by excess cost value
print(f"\n  {'─'*110}")
print(f"  TOP 30 MOST OVERSUPPLIED ITEMS (by Excess Cost Value):")
print(f"  {'─'*110}")
top_oversupplied = oversupplied.nlargest(30, 'ExcessCostValue')
print(f"  {'Item':<42} {'Dept':<18} {'Stock':>6} {'ADS':>5} {'Days':>6} {'Threshold':>5} {'Excess Cost':>12} {'Velocity':>10}")
print(f"  {'─'*110}")
for _, r in top_oversupplied.iterrows():
    print(f"  {str(r['ITM_NAME'])[:41]:<42} {str(r['DEPARTMENT'])[:17]:<18} {r['STOCK']:>6,.0f} {r['SC_ADS']:>5.1f} {r['DaysOfCover']:>5.0f}d {r['Oversupply_Threshold']:>4.0f}d {r['ExcessCostValue']:>12,.0f} {str(r['SC_Velocity']):>10}")

# ── PART 2b: Extremely slow movers (ADS < 0.1 but stock > 5) ───────────────
print(f"\n\n  {'─'*90}")
print(f"  NEAR-DEAD STOCK: Items with ADS < 0.1 and Stock > 5 units")
print(f"  {'─'*90}")

near_dead = df_comp[(df_comp['SC_ADS'] < 0.1) & (df_comp['SC_ADS'] > 0) & (df_comp['STOCK'] > 5)].copy()
near_dead = near_dead.sort_values('CostValue', ascending=False)

print(f"  Total near-dead items:  {len(near_dead):>6}")
print(f"  Capital locked (cost):  KES {near_dead['CostValue'].sum():>12,.0f}")
print(f"  Capital locked (sell):  KES {near_dead['StockValue'].sum():>12,.0f}")
print()

# By department
dead_dept = near_dead.groupby('DEPARTMENT').agg(
    Items=('BARCODE', 'count'),
    TotalCost=('CostValue', 'sum'),
).sort_values('TotalCost', ascending=False)

print(f"  Near-Dead Stock by Department (Top 10):")
for dept, row in dead_dept.head(10).iterrows():
    print(f"    {dept[:30]:<32} {row['Items']:>4} items | KES {row['TotalCost']:>10,.0f}")

print(f"\n  Sample Near-Dead Items:")
print(f"  {'Item':<45} {'Dept':<18} {'Stock':>6} {'ADS':>5} {'Cost Val':>10}")
print(f"  {'─'*90}")
for _, r in near_dead.head(15).iterrows():
    print(f"  {str(r['ITM_NAME'])[:44]:<45} {str(r['DEPARTMENT'])[:17]:<18} {r['STOCK']:>6,.0f} {r['SC_ADS']:>5.2f} {r['CostValue']:>10,.0f}")

print()
print("  DONE.")
