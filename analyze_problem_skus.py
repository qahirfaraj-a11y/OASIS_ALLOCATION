"""
Detailed analysis of problem SKUs from simulation feedback.
"""
import json
from pathlib import Path

feedback_path = Path(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\simulation_feedback.json")

with open(feedback_path) as f:
    data = json.load(f)

print("=" * 80)
print("PROBLEM SKUs DETAILED ANALYSIS")
print("=" * 80)
print(f"Total Simulations: {data['simulation_count']}")
print(f"Total SKUs Tracked: {len(data['sku_feedback'])}")

# Get problem SKUs
problem_skus = [
    (name, info) 
    for name, info in data['sku_feedback'].items() 
    if info.get('stockout_frequency', 0) > 0.5
]

print(f"Problem SKUs (>50% stockout freq): {len(problem_skus)}")

# Sort by frequency
sorted_problems = sorted(problem_skus, key=lambda x: x[1].get('stockout_frequency', 0), reverse=True)

print("\n" + "=" * 80)
print("COMPLETE LIST OF PROBLEM SKUs")
print("=" * 80)
print(f"{'#':>3} {'SKU Name':<55} {'Freq':>6} {'Day':>6} {'Boost':>6}")
print("-" * 80)

for i, (name, info) in enumerate(sorted_problems, 1):
    freq = info.get('stockout_frequency', 0)
    day = info.get('avg_first_stockout_day', 0)
    boost = info.get('recommended_coverage_boost', 1.0)
    print(f"{i:3}. {name[:53]:<55} {freq:>5.0%} {day:>6.1f} {boost:>5.1f}x")

print("\n" + "=" * 80)
print("ROOT CAUSE ANALYSIS")
print("=" * 80)

# Analyze patterns
early_stockouts = [p for p in sorted_problems if p[1].get('avg_first_stockout_day', 99) < 7]
mid_stockouts = [p for p in sorted_problems if 7 <= p[1].get('avg_first_stockout_day', 0) < 10]
late_stockouts = [p for p in sorted_problems if p[1].get('avg_first_stockout_day', 0) >= 10]

print(f"\n1. EARLY STOCKOUTS (Day 1-6) - {len(early_stockouts)} SKUs")
print("   Root Cause: Insufficient initial allocation depth")
for name, info in early_stockouts[:5]:
    print(f"   - {name[:50]}: Day {info.get('avg_first_stockout_day', 0):.1f}")

print(f"\n2. MID-PERIOD STOCKOUTS (Day 7-9) - {len(mid_stockouts)} SKUs")
print("   Root Cause: Replenishment timing or weekend demand spike")
for name, info in mid_stockouts[:5]:
    print(f"   - {name[:50]}: Day {info.get('avg_first_stockout_day', 0):.1f}")

print(f"\n3. LATE STOCKOUTS (Day 10+) - {len(late_stockouts)} SKUs")
print("   Root Cause: Lead time gaps or sporadic demand")
for name, info in late_stockouts[:5]:
    print(f"   - {name[:50]}: Day {info.get('avg_first_stockout_day', 0):.1f}")

print("\n" + "=" * 80)
print("WHY STOCKOUTS STILL OCCUR")
print("=" * 80)
print("""
STRUCTURAL REASONS:

1. DEMAND VARIABILITY
   - Simulation uses random normal distribution for demand
   - Even with coverage boosts, demand spikes can exceed safety stock
   - Fresh items have 30% higher variability (cv * 1.3)

2. PERISHABLE CONSTRAINTS
   - Fresh milk, bread have short shelf life
   - Can't overstock without waste risk
   - Trade-off between stockout and spoilage

3. SUPPLY-SIDE GAPS
   - Some suppliers only deliver on specific days
   - Lead time between order and delivery
   - MOV (Minimum Order Value) batching delays small orders

4. DATA QUALITY ISSUES
   - Historical avg_daily_sales may be inaccurate
   - Some SKUs have unusually high demand_cv
   - New products lack sales history

5. BUDGET CONSTRAINTS
   - Limited capital spread across 2500+ SKUs
   - Priority given to higher-margin items
   - Low-priority items may be underfunded
""")
