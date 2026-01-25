import pandas as pd
import numpy as np

FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"

def analyze_allocation():
    print(f"Loading {FILE}...")
    try:
        df = pd.read_csv(FILE)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    scenarios = ["Small_200k", "Med_2.5M", "Large_10M"]
    
    print("\n--- HYBRID ALLOCATION HEALTH CHECK ---")
    
    for scen in scenarios:
        print(f"\n[{scen.upper()}]")
        allocated = df[df[scen] == True]
        
        # 1. Budget Stats
        total_spend = allocated['Capital_Required'].sum() 
        count = len(allocated)
        
        target = 0
        if "Small" in scen: target = 200000
        elif "Med" in scen: target = 2500000
        elif "Large" in scen: target = 10000000
        
        utilization = (total_spend / target) * 100 if target > 0 else 0
        
        print(f"  SKU Count: {count}")
        print(f"  Total Spend: ${total_spend:,.0f} / ${target:,.0f} ({utilization:.1f}%)")
        
        # 2. Composition (The "DNA" Test)
        proxy_count = allocated['Is_Proxy_Core'].sum()
        staple_count = allocated['Is_Staple'].sum()
        
        proxy_pct = (proxy_count / count) * 100 if count > 0 else 0
        staple_pct = (staple_count / count) * 100 if count > 0 else 0
        
        print(f"  SKU Count: {count}")
        print(f"  Proxy Alignment: {proxy_pct:.1f}% ({proxy_count} / {count})")
        print(f"  Staple Density:  {staple_pct:.1f}%")
        
        # 3. Department Balance (Are we over-indexed on Soda?)
        print("  Top 5 Departments (by Count):")
        print(allocated['Department'].value_counts().head(5).to_string())
        
        # 4. The "Missing Core" (What did we FAIL to buy?)
        # Proxy Core items that are NOT in the basket
        missed_core = df[(df['Is_Proxy_Core'] == True) & (df[scen] == False)]
        print(f"  Missed Core Opportunities: {len(missed_core)}")
        if len(missed_core) > 0:
            print("    Top Missed Depts:", missed_core['Department'].value_counts().head(3).to_dict())

if __name__ == "__main__":
    analyze_allocation()
