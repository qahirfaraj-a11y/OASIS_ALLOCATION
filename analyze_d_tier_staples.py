"""
Analyze D-Tier Staple Items + Logic Gaps
"""
import pandas as pd

df = pd.read_csv('Full_Product_Allocation_Scorecard_v5.csv')

# Staple departments
staple_depts = ['FRESH MILK', 'BREAD', 'FLOUR', 'COOKING OIL', 'SUGAR', 'RICE', 'EGGS', 
                'MINERAL WATER', 'SODA', 'TOILET ROLL', 'TISSUE PAPER', 'SALT', 
                'BREAKFAST CEREALS', 'YOGHURT', 'BUTTER', 'TEA', 'COFFEE']

# Find staple items in D-tier
staple_d_tier = df[(df['Department'].isin(staple_depts)) & (df['Velocity_Tier'] == 'D (Risk)')]

print('=' * 70)
print('STAPLE ITEMS IN D-TIER (LOW VELOCITY)')
print('=' * 70)
print()
print(f'Total staple D-tier items: {len(staple_d_tier)}')
print()

# Breakdown by department
print('BY DEPARTMENT:')
dept_counts = staple_d_tier['Department'].value_counts()
for dept, count in dept_counts.items():
    print(f'  {dept}: {count}')
print()

# Show high-value D-tier staples
print('TOP 25 D-TIER STAPLES:')
print('-' * 70)
top_d_tier = staple_d_tier.nlargest(25, 'Capital_Required')[['Product', 'Department', 'Avg_Daily_Sales', 'GRN_Frequency']]
for idx, row in top_d_tier.iterrows():
    product = row['Product'][:42]
    dept = row['Department'][:12]
    ads = row['Avg_Daily_Sales']
    grn = row['GRN_Frequency']
    print(f"{product:42} | {dept:12} | ADS:{ads:5.2f} | GRN:{grn:.2f}")

print()

# Why are they D-tier?
print('=' * 70)
print('WHY ARE THESE IN D-TIER?')
print('=' * 70)
print()
print('D-Tier Criteria: GRN_Frequency <= 0.2')
print()
print('GRN FREQUENCY STATS FOR D-TIER STAPLES:')
print(f'  Mean: {staple_d_tier["GRN_Frequency"].mean():.3f}')
print(f'  Max:  {staple_d_tier["GRN_Frequency"].max():.3f}')
print()

zero_grn = staple_d_tier[staple_d_tier['GRN_Frequency'] == 0]
print(f'Items with GRN_Frequency = 0: {len(zero_grn)}')
print()

# Check if any have positive sales despite D-tier
has_sales = staple_d_tier[staple_d_tier['Avg_Daily_Sales'] > 0.5]
print(f'D-tier staples with ADS > 0.5: {len(has_sales)}')
if len(has_sales) > 0:
    print('  These items sell but have low ordering frequency!')
    print('  Sample:')
    for idx, row in has_sales.head(5).iterrows():
        print(f'    {row["Product"][:40]} | ADS: {row["Avg_Daily_Sales"]:.2f}')

print()
print('=' * 70)
print('LOGIC GAPS IDENTIFIED')
print('=' * 70)
print('''
GAP A: VELOCITY TIER MISMATCH
  - Items with good sales (ADS > 0.5) but low GRN frequency are D-tier
  - This means they SELL but we don't ORDER them often
  - Problem: These could be blocked in small stores

GAP B: GRN FREQUENCY MAY BE STALE
  - GRN frequency is based on historical ordering patterns
  - New items or seasonally ordered items get penalized
  
GAP C: NO SALES-BASED OVERRIDE
  - Current tier logic: Based purely on GRN frequency
  - Should consider: If ADS > threshold, promote to higher tier
  
RECOMMENDATION:
  Modify velocity tier logic in generate_allocation_scorecard.py:
  
  if grn_freq > 0.8 or avg_daily_sales > 2.0:
      velocity_tier = "A (Staple)"
  elif grn_freq > 0.5 or avg_daily_sales > 1.0:
      velocity_tier = "B (Core)"
  elif grn_freq > 0.2 or avg_daily_sales > 0.5:
      velocity_tier = "C (Filler)"
  else:
      velocity_tier = "D (Risk)"
''')
