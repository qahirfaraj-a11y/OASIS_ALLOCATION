"""
Fix GAP A, B, C: Velocity Tier Recalculation + Dead Stock Filter Update
========================================================================

GAP A: Hybrid velocity tier (GRN frequency OR sales velocity)
GAP B: Essential department bypass in dead stock filter  
GAP C: Sales-based GRN fallback
"""
import pandas as pd
import numpy as np

# Load scorecard
df = pd.read_csv('Full_Product_Allocation_Scorecard_v5.csv')

print("=" * 70)
print("FIXING GAPS A, B, C")
print("=" * 70)
print()

# ============================================================================
# GAP A: HYBRID VELOCITY TIER CALCULATION
# ============================================================================
print("GAP A: Recalculating Velocity Tiers with Hybrid Logic")
print("-" * 70)

# Save original for comparison
df['Velocity_Tier_Original'] = df['Velocity_Tier'].copy()

# Hybrid velocity tier: GRN frequency OR sales velocity
def calculate_hybrid_velocity_tier(row):
    grn_freq = row['GRN_Frequency']
    ads = row['Avg_Daily_Sales']
    
    # Tier A (Staple): High ordering frequency OR high sales
    if grn_freq > 0.8 or ads > 2.0:
        return "A (Staple)"
    
    # Tier B (Core): Medium ordering OR medium sales
    elif grn_freq > 0.5 or ads > 1.0:
        return "B (Core)"
    
    # Tier C (Filler): Low ordering OR moderate sales
    elif grn_freq > 0.2 or ads > 0.5:
        return "C (Filler)"
    
    # Tier D (Risk): Very low ordering AND low sales
    else:
        return "D (Risk)"

df['Velocity_Tier'] = df.apply(calculate_hybrid_velocity_tier, axis=1)

# Report changes
changes = df[df['Velocity_Tier'] != df['Velocity_Tier_Original']]
print(f"Velocity tier changed for {len(changes)} items")
print()

# Show tier distribution
print("Tier Distribution:")
print("Before:")
print(df['Velocity_Tier_Original'].value_counts().to_string())
print()
print("After:")
print(df['Velocity_Tier'].value_counts().to_string())
print()

# Show examples of promoted items
promoted = changes[changes['Velocity_Tier_Original'] == 'D (Risk)']
print(f"Items promoted from D-tier: {len(promoted)}")
if len(promoted) > 0:
    print()
    print("Sample promotions:")
    sample = promoted.nlargest(10, 'Avg_Daily_Sales')[['Product', 'Department', 'Avg_Daily_Sales', 'GRN_Frequency', 'Velocity_Tier_Original', 'Velocity_Tier']]
    for idx, row in sample.iterrows():
        print(f"  {row['Product'][:40]:40} {row['Velocity_Tier_Original']:12} -> {row['Velocity_Tier']:12} (ADS:{row['Avg_Daily_Sales']:.2f})")
print()

# ============================================================================
# GAP C: SALES-BASED GRN FALLBACK
# ============================================================================
print("=" * 70)
print("GAP C: Filling Missing GRN Frequencies")
print("-" * 70)

# For items with GRN_Frequency = 0 but positive sales, estimate GRN frequency
zero_grn = df[(df['GRN_Frequency'] == 0) & (df['Avg_Daily_Sales'] > 0)]
print(f"Items with GRN=0 but ADS>0: {len(zero_grn)}")

# Calculate synthetic GRN frequency based on sales velocity
# Logic: If they're selling, they must be getting ordered (data gap)
# Estimate based on sales tier
def estimate_grn_from_sales(ads):
    if ads > 2.0:
        return 0.85  # High sales -> likely high ordering
    elif ads > 1.0:
        return 0.60  # Medium sales -> medium ordering
    elif ads > 0.5:
        return 0.30  # Moderate sales -> some ordering
    else:
        return 0.10  # Low sales -> rare ordering

for idx in zero_grn.index:
    ads = df.at[idx, 'Avg_Daily_Sales']
    df.at[idx, 'GRN_Frequency'] = estimate_grn_from_sales(ads)

filled = len(zero_grn)
print(f"Filled {filled} missing GRN frequencies based on sales velocity")
print()

# ============================================================================
# UPDATE IS_ELIGIBLE FLAG
# ============================================================================
print("=" * 70)
print("Updating Eligibility Flags")
print("-" * 70)

# Recalculate eligibility with new tiers
# Items in A/B tiers are generally eligible, C needs review, D needs good reason
df['Is_Eligible_Basic_Old'] = df['Is_Eligible_Basic'].copy()

# More items should be eligible now
# Rule: A-tier and B-tier items are eligible, C-tier if in staple depts
staple_depts = ['FRESH MILK', 'BREAD', 'FLOUR', 'COOKING OIL', 'SUGAR', 'RICE', 'EGGS',
                'MINERAL WATER', 'SODA', 'TOILET ROLL', 'TISSUE PAPER', 'SALT',
                'BREAKFAST CEREALS', 'YOGHURT', 'BUTTER', 'TEA', 'COFFEE']

for idx, row in df.iterrows():
    tier = row['Velocity_Tier']
    dept = row['Department']
    
    if tier in ['A (Staple)', 'B (Core)']:
        df.at[idx, 'Is_Eligible_Basic'] = True
    elif tier == 'C (Filler)' and dept in staple_depts:
        df.at[idx, 'Is_Eligible_Basic'] = True
    # D-tier keeps existing eligibility (don't override good reasons)

newly_eligible = df[(df['Is_Eligible_Basic'] == True) & (df['Is_Eligible_Basic_Old'] == False)]
print(f"Newly eligible items: {len(newly_eligible)}")
print()

# ============================================================================
# SAVE RESULTS
# ============================================================================
# Drop temporary columns
df = df.drop(columns=['Velocity_Tier_Original', 'Is_Eligible_Basic_Old'])

# Save updated scorecard
df.to_csv('Full_Product_Allocation_Scorecard_v6.csv', index=False)
print("=" * 70)
print("SAVED: Full_Product_Allocation_Scorecard_v6.csv")
print("=" * 70)
print()

# ============================================================================
# VERIFICATION
# ============================================================================
print("VERIFICATION: Checking specific items")
print("-" * 70)

# Check the problem items we identified
problem_items = [
    'TUZO 450ML',
    'MUMIAS 2KG',
    'VELVEX EXTRA 10S',
    'KERINGET 18.9'
]

for item_name in problem_items:
    matches = df[df['Product'].str.contains(item_name, case=False, na=False)]
    if len(matches) > 0:
        row = matches.iloc[0]
        print(f"{row['Product'][:45]:45}")
        print(f"  ADS: {row['Avg_Daily_Sales']:.2f} | GRN: {row['GRN_Frequency']:.2f} | Tier: {row['Velocity_Tier']}")
        print(f"  Eligible: {row['Is_Eligible_Basic']}")
        print()

print("=" * 70)
print("SUMMARY")
print("=" * 70)
print(f"Total items: {len(df)}")
print(f"Velocity tier changes: {len(changes)}")
print(f"D-tier promotions: {len(promoted)}")
print(f"GRN frequencies filled: {filled}")
print(f"Newly eligible items: {len(newly_eligible)}")
