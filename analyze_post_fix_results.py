"""
Analyze Post-Fix Simulation Results
====================================
Check if replenishment fix resolved the stockout crisis.
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

file_path = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"

print("=" * 80)
print("POST-FIX SIMULATION ANALYSIS")
print("=" * 80)
print(f"\nFile: {file_path}")
print(f"Run time: 2026-02-08 18:24:18")
print("This is AFTER replenishment fix (18:18)\n")

# Load sheets
xl = pd.ExcelFile(file_path)

# 1. Summary metrics
print("=" * 80)
print("OVERALL METRICS")
print("=" * 80)

df_summary = pd.read_excel(file_path, sheet_name='Summary')
for col in df_summary.columns:
    val = df_summary[col].iloc[0]
    print(f"  {col}: {val}")

# 2. Daily performance (focus on Days 1-7)
print("\n" + "=" * 80)
print("DAILY PERFORMANCE (Days 1-10)")
print("=" * 80)

df_daily = pd.read_excel(file_path, sheet_name='Daily_Performance')

print(f"\n{'Day':<5} {'Fill Rate %':<12} {'Stockouts':<12} {'Lost Sales':<15}")
print("-" * 50)

fill_rate_col = [c for c in df_daily.columns if 'fill' in c.lower()][0]
stockout_col = [c for c in df_daily.columns if 'stockout' in c.lower()][0]
lost_col = [c for c in df_daily.columns if 'lost' in c.lower()][0]
day_col = [c for c in df_daily.columns if 'day' in c.lower()][0]

for _, row in df_daily.head(10).iterrows():
    print(f"{int(row[day_col]):<5} {row[fill_rate_col]:<12.1f} {int(row[stockout_col]):<12} {row[lost_col]:<15,.0f}")

# 3. Stockout analysis
print("\n" + "=" * 80)
print("STOCKOUT ANALYSIS")
print("=" * 80)

df_sku = pd.read_excel(file_path, sheet_name='SKU_Final_State')

stockout_day_col = [c for c in df_sku.columns if 'first' in c.lower() and 'stockout' in c.lower()][0]

stockouts = df_sku[df_sku[stockout_day_col].notna()]
early_stockouts = stockouts[stockouts[stockout_day_col] <= 7]

print(f"\nTotal SKUs: {len(df_sku)}")
print(f"SKUs with Stockouts: {len(stockouts)} ({len(stockouts)/len(df_sku)*100:.1f}%)")
print(f"Early Stockouts (Day 1-7): {len(early_stockouts)} ({len(early_stockouts)/len(df_sku)*100:.1f}%)")

# 4. Comparison with pre-fix
print("\n" + "=" * 80)
print("BEFORE vs AFTER COMPARISON")
print("=" * 80)

print("\n                          BEFORE FIX    AFTER FIX    IMPROVEMENT")
print("-" * 65)

# Days 1-7 metrics
days_1_7 = df_daily.head(7)
avg_fill_1_7 = days_1_7[fill_rate_col].mean()
min_fill_1_7 = days_1_7[fill_rate_col].min()

print(f"Days 1-7 Avg Fill Rate:    57.6%        {avg_fill_1_7:5.1f}%       {avg_fill_1_7-57.6:+5.1f}pp")
print(f"Days 1-7 Worst Day:        32.4%        {min_fill_1_7:5.1f}%       {min_fill_1_7-32.4:+5.1f}pp")
print(f"Day 1-7 Stockout Rate:     72.6%        {len(early_stockouts)/len(df_sku)*100:5.1f}%       {(len(early_stockouts)/len(df_sku)*100)-72.6:+5.1f}pp")

# 5. Success criteria
print("\n" + "=" * 80)
print("SUCCESS CRITERIA CHECK")
print("=" * 80)

criteria = [
    ("Days 1-7 Fill Rate > 95%", avg_fill_1_7 >= 95, f"{avg_fill_1_7:.1f}%"),
    ("Worst Day Fill Rate > 90%", min_fill_1_7 >= 90, f"{min_fill_1_7:.1f}%"),
    ("Total Stockout Rate < 5%", (len(stockouts)/len(df_sku)*100) < 5, f"{len(stockouts)/len(df_sku)*100:.1f}%"),
    ("Early Stockout Rate < 5%", (len(early_stockouts)/len(df_sku)*100) < 5, f"{len(early_stockouts)/len(df_sku)*100:.1f}%")
]

for criterion, passed, value in criteria:
    status = "✓ PASS" if passed else "✗ FAIL"
    print(f"  {status} {criterion:<30} ({value})")

# Overall verdict
all_passed = all(p for _, p, _ in criteria)

print("\n" + "=" * 80)
if all_passed:
    print("🎉 SUCCESS! All criteria met. Replenishment fix working!")
else:
    print("⚠ PARTIAL SUCCESS. Some criteria not met yet.")
print("=" * 80)
