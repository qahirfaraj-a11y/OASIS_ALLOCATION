"""
Generate simulation_feedback.json from recent stockout results
================================================================
This bootstraps the feedback loop using your actual stockout data.
"""

import pandas as pd
import json
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

# Load your latest simulation
sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"
df_sku = pd.read_excel(sim_file, sheet_name='SKU_Final_State')

print("=" * 80)
print("GENERATING SIMULATION FEEDBACK")
print("=" * 80)

# Find stockout column
stockout_col = [c for c in df_sku.columns if 'first' in c.lower() and 'stockout' in c.lower()][0]

# Build feedback
sku_feedback = {}

stockout_skus = df_sku[df_sku[stockout_col].notna()]

for _, row in stockout_skus.iterrows():
    product = row.get('Product', row.get('product_name', ''))
    first_stockout = row[stockout_col]
    
    # Calculate frequency (assume 30 day sim)
    # If stocked out early, mark as high frequency
    if first_stockout <= 7:
        frequency = 1.0  # 100% - always stocks out
    elif first_stockout <= 14:
        frequency = 0.5
    else:
        frequency = 0.3
    
    sku_feedback[product] = {
        'stockout_frequency': frequency,
        'avg_first_stockout_day': float(first_stockout),
        'lost_sales': 0,  # Could calculate from data
        'stockout_days': 30 - int(first_stockout)
    }

feedback_data = {
    'simulation_count': 1,
    'sku_feedback': sku_feedback
}

# Save
output_path = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\simulation_feedback.json"
with open(output_path, 'w') as f:
    json.dump(feedback_data, f, indent=2)

print(f"\nGenerated feedback for {len(sku_feedback)} SKUs with stockouts")
print(f"Saved to: {output_path}")

# Summary
critical = sum(1 for v in sku_feedback.values() if v['avg_first_stockout_day'] < 7)
print(f"\nCritical (Day 1-7 stockouts): {critical} SKUs")
print(f"  → These will get +14 days depth in next allocation")

medium = sum(1 for v in sku_feedback.values() if 7 <= v['avg_first_stockout_day'] < 10)
print(f"Medium (Day 7-10 stockouts): {medium} SKUs")
print(f"  → These will get +7 days depth in next allocation")

print("\n✓ Run your simulation again - targeted fix will now apply!")
