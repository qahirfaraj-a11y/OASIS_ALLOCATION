import pandas as pd
import numpy as np

FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"

def analyze_logic_gaps():
    print("Loading Scorecard...")
    df = pd.read_csv(FILE)
    
    # Analyze Small_200k Scenario
    df_small = df[df['Small_200k'] == True].copy()
    
    print("\n--- GAP ANALYSIS: Small_200k Scenario ---")
    
    # 1. CONCENTRATION RISK (The "Expensive Pack" Trap)
    # Check if any single SKU consumes > 5% of the TOTAL Budget ($200k -> $10k)
    # Or > 10% of its Department Budget
    
    # Recalculate estimated cost for the allocated items (MDQ or Calculated)
    # The CSV has 'Capital_Required', but that's the IDEAL. We need the actual allocated cost.
    # We will approximate this by checking the 'Recommended_Qty' logic trace or just using Capital_Required 
    # if it aligns with the logic. 
    # Better: Inspect 'Capital_Required'.
    
    total_budget = 200000
    df_small['Budget_Share'] = df_small['Capital_Required'] / total_budget
    
    risky_items = df_small[df_small['Budget_Share'] > 0.025] # > 2.5% of total store budget ($5000)
    
    print(f"\n1. CONCENTRATION RISK (Items > $5,000 cost): {len(risky_items)}")
    if len(risky_items) > 0:
        print(risky_items[['Product', 'Department', 'Is_Staple', 'Capital_Required']].sort_values(by='Capital_Required', ascending=False).to_string())
        
    # 2. CATEGORY DOMINANCE (The "Toilet Paper" Trap)
    # Check if any Non-Staple department consumes > 30% of the Discretionary Budget ($80k)
    
    non_staple = df_small[df_small['Is_Staple'] == False]
    disc_spend = non_staple.groupby('Department')['Capital_Required'].sum()
    total_disc_spend = disc_spend.sum()
    
    print(f"\n2. DISCRETIONARY BALANCE (Total Disc Spend: ${total_disc_spend:,.0f})")
    disc_share = (disc_spend / total_disc_spend * 100).sort_values(ascending=False)
    
    print("  Dept Share of Discretionary Pool:")
    print(disc_share.head(10).to_string())
    
    # Check for dominance (>25%)
    dominant = disc_share[disc_share > 25]
    if len(dominant) > 0:
        print(f"\n  WARNING: Departments dominating variety: {dominant.index.tolist()}")
    else:
        print("\n  PASS: Good balance (No dept > 25%)")

    # 3. THE "ORPHAN" GAP
    # Check for categories with exactly 1 or 2 items (Risk of poor customer choice)
    dept_counts = df_small['Department'].value_counts()
    orphans = dept_counts[dept_counts < 3]
    
    print(f"\n3. ASSORTMENT DEPTH RISKS (Depts with < 3 items): {len(orphans)}")
    if len(orphans) > 0:
        print(orphans.head(10).to_string())
        
if __name__ == "__main__":
    analyze_logic_gaps()
