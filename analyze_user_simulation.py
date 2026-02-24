"""
Analyze Simulation Results from User's Run
===========================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

file_path = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_180448.xlsx"

print("=" * 80)
print("SIMULATION RESULTS ANALYSIS")
print("=" * 80)
print(f"\nFile: {file_path}")
print(f"Run time: 2026-02-08 18:04:48\n")

# Load all sheets
xl = pd.ExcelFile(file_path)
print(f"Available sheets: {xl.sheet_names}\n")

# 1. Summary
print("=" * 80)
print("OVERALL METRICS (Summary)")
print("=" * 80)

df_summary = pd.read_excel(file_path, sheet_name='Summary')
for col in df_summary.columns:
    val = df_summary[col].iloc[0]
    print(f"  {col}: {val}")

# 2. Daily Performance
print("\n" + "=" * 80)
print("DAILY PERFORMANCE")
print("=" * 80)

df_daily = pd.read_excel(file_path, sheet_name='Daily_Performance')
print(f"\n{'Day':<5} {'Fill Rate %':<12} {'Stockouts':<12} {'Lost Sales':<15}")
print("-" * 50)
for _, row in df_daily.iterrows():
    fill_rate_col = [c for c in df_daily.columns if 'fill' in c.lower()][0]
    stockout_col = [c for c in df_daily.columns if 'stockout' in c.lower()][0]
    lost_col = [c for c in df_daily.columns if 'lost' in c.lower()][0]
    day_col = [c for c in df_daily.columns if 'day' in c.lower()][0]
    
    print(f"{int(row[day_col]):<5} {row[fill_rate_col]:<12.1f} {int(row[stockout_col]):<12} {row[lost_col]:<15,.0f}")

# 3. SKU Final State - Stockout Analysis
print("\n" + "=" * 80)
print("STOCKOUT ANALYSIS (SKU Final State)")
print("=" * 80)

df_sku = pd.read_excel(file_path, sheet_name='SKU_Final_State')

print(f"\nTotal SKUs: {len(df_sku)}")

if 'First Stockout Day' in df_sku.columns:
    stockouts = df_sku[df_sku['First Stockout Day'].notna()]
    early = stockouts[stockouts['First Stockout Day'] <= 10]
    
    print(f"SKUs with Stockouts: {len(stockouts)} ({len(stockouts)/len(df_sku)*100:.1f}%)")
    print(f"Early Stockouts (Day 1-10): {len(early)} ({len(early)/len(df_sku)*100:.1f}%)")
    
    if len(early) > 0:
        print("\n" + "-" * 80)
        print("EARLY STOCKOUT BREAKDOWN BY DEPARTMENT")
        print("-" * 80)
        
        if 'Department' in early.columns:
            dept_counts = early['Department'].value_counts()
            for dept, count in dept_counts.items():
                pct_of_early = count/len(early)*100
                pct_of_total = count/len(df_sku)*100
                print(f"  {dept:<25} {count:>4} SKUs ({pct_of_early:>5.1f}% of early, {pct_of_total:>5.1f}% of total)")
        
        print("\n" + "-" * 80)
        print("STOCKOUT DAY DISTRIBUTION")
        print("-" * 80)
        
        day_counts = early['First Stockout Day'].value_counts().sort_index()
        for day, count in day_counts.items():
            print(f"  Day {int(day):>2}: {count:>3} SKUs")
        
        print("\n" + "-" * 80)
        print("TOP 20 EARLY STOCKOUT SKUs")
        print("-" * 80)
        
        display_cols = ['Product', 'Department', 'ADS', 'First Stockout Day', 'Lost Sales']
        display_cols = [col for col in display_cols if col in early.columns]
        
        print(early[display_cols].sort_values('First Stockout Day').head(20).to_string(index=False))
        
        # Analyze by category type
        print("\n" + "-" * 80)
        print("CATEGORY ANALYSIS")
        print("-" * 80)
        
        fresh_depts = ['fresh', 'FRESH MILK', 'BREAD', 'YOGHURT', 'EGGS', 'DAIRY']
        discretionary_depts = ['WATER', 'BEVERAGES', 'SNACKS', 'CONFECTIONERY', 'CIGARETTES']
        staples_depts = ['SUGAR', 'MAIZE MEAL', 'RICE', 'FLOUR', 'SALT']
        
        fresh_stockouts = early[early['Department'].str.upper().isin([d.upper() for d in fresh_depts])]
        discretionary_stockouts = early[early['Department'].str.upper().isin([d.upper() for d in discretionary_depts])]
        staples_stockouts = early[early['Department'].str.upper().isin([d.upper() for d in staples_depts])]
        
        print(f"  Fresh Items: {len(fresh_stockouts)} stockouts")
        print(f"  Discretionary: {len(discretionary_stockouts)} stockouts")
        print(f"  Staples: {len(staples_stockouts)} stockouts")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
