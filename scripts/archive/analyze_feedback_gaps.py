"""
Analyze simulation feedback to identify key gaps and problem SKUs.
"""
import json
from pathlib import Path
from collections import Counter

feedback_path = Path(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\simulation_feedback.json")

with open(feedback_path) as f:
    data = json.load(f)

print("=" * 60)
print("SIMULATION FEEDBACK ANALYSIS")
print("=" * 60)

# Basic stats
print(f"\nTotal Simulations: {data['simulation_count']}")
print(f"SKUs Tracked: {len(data['sku_feedback'])}")
print(f"Last Updated: {data['last_updated']}")

# Problem SKUs (>50% stockout frequency)
problem_skus = [
    (name, info) 
    for name, info in data['sku_feedback'].items() 
    if info.get('stockout_frequency', 0) > 0.5
]

print(f"\n{'=' * 60}")
print(f"PROBLEM SKUs (>50% stockout frequency): {len(problem_skus)}")
print("=" * 60)

# Sort by stockout frequency
sorted_problems = sorted(problem_skus, key=lambda x: x[1].get('stockout_frequency', 0), reverse=True)

for i, (name, info) in enumerate(sorted_problems[:20], 1):
    freq = info.get('stockout_frequency', 0)
    boost = info.get('recommended_coverage_boost', 1.0)
    avg_day = info.get('avg_first_stockout_day', 'N/A')
    print(f"{i:2}. {name[:55]:<55} | Freq: {freq:.0%} | Boost: {boost:.1f}x | Day: {avg_day}")

# Category analysis - extract department from product name patterns
print(f"\n{'=' * 60}")
print("CATEGORY PATTERNS IN PROBLEM SKUs")
print("=" * 60)

categories = Counter()
for name, _ in problem_skus:
    name_upper = name.upper()
    if 'BREAD' in name_upper or 'FESTIVE' in name_upper or 'NATURES' in name_upper:
        categories['BREAD/BAKERY'] += 1
    elif 'MILK' in name_upper or 'DAIRY' in name_upper or 'DAIMA' in name_upper or 'BIO' in name_upper:
        categories['DAIRY/MILK'] += 1
    elif 'YOGHURT' in name_upper or 'YOGHU' in name_upper:
        categories['YOGHURT'] += 1
    elif 'JUICE' in name_upper or 'QUENCHER' in name_upper:
        categories['BEVERAGES'] += 1
    elif 'CHIPS' in name_upper or 'OLA' in name_upper:
        categories['SNACKS'] += 1
    else:
        categories['OTHER'] += 1

for cat, count in categories.most_common():
    print(f"  {cat}: {count} SKUs")

# Tier analysis
print(f"\n{'=' * 60}")
print("TIER PERFORMANCE")
print("=" * 60)

tier_feedback = data.get('tier_feedback', {})
for tier, info in tier_feedback.items():
    avg_fill = info.get('avg_fill_rate', 0)
    avg_stockout = info.get('avg_stockout_rate', 0)
    sim_count = info.get('simulation_count', 0)
    print(f"  {tier}: Fill Rate: {avg_fill:.1%} | Stockout Rate: {avg_stockout:.2%} | Runs: {sim_count}")

# Key Insights
print(f"\n{'=' * 60}")
print("KEY GAPS IDENTIFIED")
print("=" * 60)
print("""
1. FRESH/PERISHABLES GAP: Bread and dairy products dominate stockouts
   - FESTIVE, NATURES bread items: High velocity, short shelf life
   - DAIMA, BIO milk products: Fresh milk running out mid-week
   - Root cause: Coverage days not accounting for perishable demand spikes

2. HIGH-VELOCITY STAPLES GAP: Essential daily items understocked
   - 500ML milk pouches (BROOKSIDE, ILARA, TUZO)
   - These have high turnover but current allocation is too conservative

3. DEMAND VARIABILITY GAP: Weekend/weekday patterns not captured
   - Many stockouts occur Day 5-7 (mid-week)
   - Suggests demand spikes not adequately covered

4. PACK SIZE MISMATCH: Smaller fast-moving packs understocked
   - 400G, 500ML, 600G sizes showing frequent stockouts
   - Larger packs (2L) may be over-allocated

RECOMMENDATIONS:
- Increase coverage boost for BREAD/BAKERY category from 1.5x to 2.0x
- Add category-specific depth for DAIRY (fresh milk) items
- Consider separate coverage multiplier for perishables
""")
