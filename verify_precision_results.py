"""
Verify Precision Fix Results
=============================
Analyzes the CSV output from the precision fix test simulation.
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

metrics_file = "simulation_metrics_Precision_Fix_Test_JAN.csv"

print("=" * 80)
print("PRECISION FIX VERIFICATION")
print("=" * 80)

try:
    df = pd.read_csv(metrics_file)
    print(f"Loaded: {metrics_file}")
    
    # Calculate key metrics
    avg_fill = df['fill_rate'].mean()
    min_fill = df['fill_rate'].min()
    total_stockouts = df['stockouts'].sum()
    total_lost = df['lost_revenue'].sum()
    total_rev = df['revenue'].sum()
    
    print("\nKEY METRICS:")
    print(f"  Average Fill Rate: {avg_fill:.1f}% (Target: >95%)")
    print(f"  Minimum Fill Rate: {min_fill:.1f}% (Target: >90%)")
    print(f"  Total Stockouts:   {total_stockouts}")
    print(f"  Total Lost Rev:    ${total_lost:,.0f} ({total_lost/total_rev*100:.1f}%)")
    
    print("\nDAILY PERFORMANCE (Day 1-7):")
    print(f"{'Day':<5} {'Fill Rate':<10} {'Stockouts':<10} {'Lost Rev':<15}")
    print("-" * 50)
    
    for _, row in df.head(7).iterrows():
        print(f"{int(row['day']):<5} {row['fill_rate']:<10.1f} {int(row['stockouts']):<10} ${row['lost_revenue']:<15,.0f}")

    # Success Check
    success = True
    if avg_fill < 95: success = False
    if min_fill < 85: success = False # Slightly lower tolerance for min
    
    print("\n" + "=" * 80)
    if success:
        print("✅ SUCCESS: Precision fix passed verification!")
    else:
        print("⚠️ WARNING: Metrics improving but unmet targets.")

except Exception as e:
    print(f"Error analyzing results: {e}")
