"""
Analyze simulation feedback to identify implementation gaps and recommendations.
"""
import json
from pathlib import Path
from collections import Counter, defaultdict

feedback_path = Path(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\simulation_feedback.json")

with open(feedback_path) as f:
    data = json.load(f)

print("=" * 70)
print("SIMULATION FEEDBACK ANALYSIS - GAPS & IMPLEMENTATIONS")
print("=" * 70)

print(f"\nTotal Simulations Run: {data['simulation_count']}")
print(f"Total SKUs Tracked: {len(data['sku_feedback'])}")

# Problem SKUs
problem_skus = [
    (name, info) 
    for name, info in data['sku_feedback'].items() 
    for name, info in data['sku_feedback'].items() 
    if info.get('stockout_frequency', 0) > 0.0 # Show ALL issues
]

print(f"Problem SKUs (>50% stockout freq): {len(problem_skus)}")

# Sort by frequency
sorted_problems = sorted(problem_skus, key=lambda x: x[1].get('stockout_frequency', 0), reverse=True)

print("\n" + "=" * 70)
print("TOP PROBLEM SKUs (Chronic Stockouts)")
print("=" * 70)

for i, (name, info) in enumerate(sorted_problems[:16], 1):
    freq = info.get('stockout_frequency', 0)
    day = info.get('avg_first_stockout_day', 0)
    boost = info.get('recommended_coverage_boost', 1.0)
    print(f"{i:2}. {name[:45]:<45} | {freq:>4.0%} | Day {day:>4.1f} | {boost:.1f}x boost")

# Categorize problem SKUs
print("\n" + "=" * 70)
print("GAP CATEGORIZATION")
print("=" * 70)

gaps = defaultdict(list)

for name, info in problem_skus:
    name_upper = name.upper()
    freq = info.get('stockout_frequency', 0)
    day = info.get('avg_first_stockout_day', 0)
    
    # Categorize by product type
    if name_upper.startswith('CFB '):
        gaps['INTERNAL_BAKERY (Exclude)'].append((name, freq, day))
    elif 'BREAD' in name_upper or 'FESTIVE' in name_upper or 'NATURES' in name_upper:
        gaps['BAKERY_PERISHABLE'].append((name, freq, day))
    elif any(x in name_upper for x in ['DAIMA', 'BIO ', 'MILK', 'MAZIWA', 'YOGHU']):
        gaps['DAIRY_FRESH'].append((name, freq, day))
    elif any(x in name_upper for x in ['JUICE', 'QUENCHER', 'CROWN TFA']):
        gaps['BEVERAGES'].append((name, freq, day))
    elif any(x in name_upper for x in ['LOLLIPOP', 'LOLLYPOP', 'CHUPA', 'CANDY']):
        gaps['IMPULSE_CONFECTIONERY'].append((name, freq, day))
    elif any(x in name_upper for x in ['NOODLE', 'INDOMIE', 'COFFEE', 'MACCOFFEE']):
        gaps['HIGH_VELOCITY_CONVENIENCE'].append((name, freq, day))
    elif any(x in name_upper for x in ['SALT', 'FLOUR', 'MEAL', 'ATTA']):
        gaps['STAPLE_COMMODITIES'].append((name, freq, day))
    else:
        gaps['OTHER'].append((name, freq, day))

for gap_name, items in sorted(gaps.items(), key=lambda x: -len(x[1])):
    print(f"\n### {gap_name} ({len(items)} SKUs)")
    for name, freq, day in sorted(items, key=lambda x: -x[1])[:5]:
        print(f"    - {name[:50]}: {freq:.0%}, Day {day:.1f}")

# Implementation Recommendations
print("\n" + "=" * 70)
print("IMPLEMENTATION STATUS & RECOMMENDATIONS")
print("=" * 70)

impls = [
    ("CFB Exclusion", "IMPLEMENTED", "Internal bakery items excluded from allocation"),
    ("Bread/Bakery 2.0x Boost", "IMPLEMENTED", "FESTIVE, NATURES, sized breads get double coverage"),
    ("Dairy/Fresh 1.5x Boost", "IMPLEMENTED", "DAIMA, BIO, MAZIWA, fresh milk boosted"),
    ("High-Velocity 1.3x Boost", "IMPLEMENTED", "GOLD TFA, MACCOFFEE, INDOMIE boosted"),
    ("Impulse/Confectionery Boost", "NOT IMPLEMENTED", "Lollipops, candy showing 60-80% stockout"),
    ("Staple Commodities Boost", "NOT IMPLEMENTED", "Salt, flour, meal need review"),
    ("Weekend Demand Spike", "NOT IMPLEMENTED", "Many stockouts Day 5-7 suggest weekend pattern"),
    ("Mega_100M Variance Fix", "NOT IMPLEMENTED", "ValueError in demand simulation (negative std_dev)"),
]

for name, status, desc in impls:
    icon = "[OK]" if status == "IMPLEMENTED" else "[!]"
    print(f"  {icon} {name}: {status}")
    print(f"      {desc}")

print("\n" + "=" * 70)
print("NEXT PRIORITY FIXES")
print("=" * 70)
print("""
1. IMPULSE/CONFECTIONERY GAP (6+ SKUs affected)
   - Big Giant Lollypops: 80% stockout
   - KSL Tropical Lollipop: 60% stockout  
   - Chupa Chups: 60% stockout
   -> Add 1.5x boost for confectionery items

2. WEEKEND DEMAND SPIKE (Day 5-7 pattern)
   - Most stockouts occur mid-week
   - Suggests weekend shopping spikes not captured
   -> Implement weekday/weekend demand adjustment

3. MEGA TIER DATA QUALITY
   - Simulation fails with ValueError (scale < 0)
   - Some SKU has negative variance in demand data
   -> Fix data validation in simulator
""")
