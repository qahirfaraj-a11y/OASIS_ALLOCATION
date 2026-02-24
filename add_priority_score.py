"""
Implement Composite Priority Score (GAP D Fix)
==============================================

Combines ABC_Class (revenue) and Velocity_Tier (frequency) into unified priority system.
"""
import pandas as pd
import numpy as np

# Load scorecard
df = pd.read_csv('Full_Product_Allocation_Scorecard_v6.csv')

print("=" * 70)
print("IMPLEMENTING COMPOSITE PRIORITY SCORE")
print("=" * 70)
print()

# ============================================================================
# PRIORITY SCORE CALCULATION
# ============================================================================

def calculate_priority_score(row):
    """
    Unified priority score combining revenue (ABC) and velocity
    
    Priority 1 = Critical (must allocate, high depth)
    Priority 2 = Important (should allocate, moderate depth)
    Priority 3 = Filler (allocate if budget allows, low depth)
    Priority 4 = Review (careful consideration, skip for small stores)
    """
    abc_class = row['ABC_Class']
    velocity_tier = row['Velocity_Tier']
    
    # Map ABC to numeric (revenue contribution)
    abc_map = {'A': 3, 'B': 2, 'C': 1}
    abc_value = abc_map.get(abc_class, 1)
    
    # Map Velocity to numeric (ordering frequency)
    vel_map = {
        'A (Staple)': 3,
        'B (Core)': 2,
        'C (Filler)': 1,
        'D (Risk)': 0
    }
    vel_value = vel_map.get(velocity_tier, 0)
    
    # Weighted composite: Velocity 60%, ABC 40%
    # Velocity weighted higher because frequency of sale matters more for small stores
    composite = (abc_value * 0.4) + (vel_value * 0.6)
    
    # Convert to priority tiers
    if composite >= 2.4:  # A+A, A+B, B+A
        priority = 1
        label = "Critical"
    elif composite >= 1.8:  # A+C, B+B, C+A
        priority = 2
        label = "Important"
    elif composite >= 1.0:  # B+C, C+B, A+D
        priority = 3
        label = "Filler"
    else:  # C+C, C+D, B+D, D+anything
        priority = 4
        label = "Review"
    
    return priority, label, composite

# Apply calculation
results = df.apply(calculate_priority_score, axis=1, result_type='expand')
df['Priority_Score'] = results[0].astype(int)
df['Priority_Label'] = results[1]
df['Priority_Composite'] = results[2].round(2)

print("Priority Score System:")
print("  1 = Critical  (must stock, high depth)")
print("  2 = Important (should stock, moderate depth)")
print("  3 = Filler    (stock if budget allows, low depth)")
print("  4 = Review    (careful consideration)")
print()

# ============================================================================
# DISTRIBUTION ANALYSIS
# ============================================================================
print("=" * 70)
print("PRIORITY DISTRIBUTION")
print("=" * 70)
print()

priority_dist = df['Priority_Label'].value_counts().sort_index()
for label in ['Critical', 'Important', 'Filler', 'Review']:
    count = priority_dist.get(label, 0)
    pct = count / len(df) * 100
    print(f"  {label:12} {count:6} ({pct:5.1f}%)")
print()

print(f"Total items: {len(df)}")
print()

# ============================================================================
# EXAMPLES BY SCENARIO
# ============================================================================
print("=" * 70)
print("EXAMPLE ITEMS BY PRIORITY")
print("=" * 70)
print()

def show_examples(priority, n=5):
    items = df[df['Priority_Score'] == priority].nlargest(n, 'Total_Revenue')
    print(f"\nPriority {priority} - {items.iloc[0]['Priority_Label']} (Top {n} by revenue):")
    print("-" * 70)
    for idx, row in items.iterrows():
        print(f"  {row['Product'][:45]:45}")
        print(f"    ABC:{row['ABC_Class']} | Vel:{row['Velocity_Tier']:12} | Composite:{row['Priority_Composite']:.2f}")

for p in [1, 2, 3, 4]:
    show_examples(p, n=5)

# ============================================================================
# EDGE CASES VERIFICATION
# ============================================================================
print()
print("=" * 70)
print("EDGE CASE VERIFICATION")
print("=" * 70)
print()

# High revenue + low velocity (e.g., whisky)
high_rev_low_vel = df[(df['ABC_Class'] == 'A') & (df['Velocity_Tier'] == 'D (Risk)')]
print(f"High Revenue + Low Velocity (A + D): {len(high_rev_low_vel)} items")
if len(high_rev_low_vel) > 0:
    sample = high_rev_low_vel.head(3)
    for idx, row in sample.iterrows():
        print(f"  {row['Product'][:40]:40} -> Priority {row['Priority_Score']} ({row['Priority_Label']})")
print()

# Low revenue + high velocity (e.g., bread)
low_rev_high_vel = df[(df['ABC_Class'] == 'C') & (df['Velocity_Tier'] == 'A (Staple)')]
print(f"Low Revenue + High Velocity (C + A): {len(low_rev_high_vel)} items")
if len(low_rev_high_vel) > 0:
    sample = low_rev_high_vel.head(3)
    for idx, row in sample.iterrows():
        print(f"  {row['Product'][:40]:40} -> Priority {row['Priority_Score']} ({row['Priority_Label']})")
print()

# ============================================================================
# STAPLE DEPARTMENT VERIFICATION
# ============================================================================
print("=" * 70)
print("STAPLE DEPARTMENT PRIORITY CHECK")
print("=" * 70)
print()

staple_depts = ['FRESH MILK', 'BREAD', 'FLOUR', 'COOKING OIL', 'SUGAR', 'RICE']
staple_items = df[df['Department'].isin(staple_depts)]

print(f"Staple department items: {len(staple_items)}")
print()
print("Priority distribution for staples:")
staple_priority = staple_items['Priority_Label'].value_counts().sort_index()
for label in ['Critical', 'Important', 'Filler', 'Review']:
    count = staple_priority.get(label, 0)
    pct = count / len(staple_items) * 100 if len(staple_items) > 0 else 0
    print(f"  {label:12} {count:6} ({pct:5.1f}%)")
print()

# ============================================================================
# SAVE RESULTS
# ============================================================================
df.to_csv('Full_Product_Allocation_Scorecard_v7.csv', index=False)

print("=" * 70)
print("SAVED: Full_Product_Allocation_Scorecard_v7.csv")
print("=" * 70)
print()
print("New columns added:")
print("  - Priority_Score (1-4)")
print("  - Priority_Label (Critical/Important/Filler/Review)")
print("  - Priority_Composite (raw weighted score)")
