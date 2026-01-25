import pandas as pd
import json
import os

DATA_DIR = r'c:\Users\iLink\.gemini\antigravity\scratch'
SCORECARD_PATH = os.path.join(DATA_DIR, 'Full_Product_Allocation_Scorecard_v3.csv')
DEPT_RATIOS_PATH = os.path.join(DATA_DIR, 'app', 'data', 'supplier_dept_ratios.json')
# We also need the Scaling Ratios (Department Weights)
SCALING_RATIOS_PATH = os.path.join(DATA_DIR, 'app', 'data', 'department_scaling_ratios.csv')

if not os.path.exists(SCORECARD_PATH):
    print("Scorecard not found.")
    exit()

df = pd.read_csv(SCORECARD_PATH)

with open(DEPT_RATIOS_PATH, 'r') as f:
    sup_share_map = json.load(f)

scaling_df = pd.read_csv(SCALING_RATIOS_PATH)
dept_ratios = scaling_df.set_index('Department')['Capital_Weight'].to_dict()

def run_tier_analysis(tier_name, budget, ceiling_pct):
    print(f"\n=== ANALYSIS FOR {tier_name} (${budget:,.0f}) ===")
    
    tier_df = df[df[tier_name] == True].copy()
    total_spent = 0 
    # Since we don't have the per-tier cost in the CSV, we'll re-calculate approx
    # Actually, we can check the department concentration
    
    dept_stats = []
    for dept, weight in dept_ratios.items():
        dept_sku_count = tier_df[tier_df['Department'] == dept].shape[0]
        # Calculate Wallet
        wallet = budget * weight
        ceiling = wallet * ceiling_pct
        
        # Check for price ceiling violations in the excluded set
        # (This is hard to do without the full set, but we can look at the included set)
        if dept_sku_count > 0:
            dept_stats.append({
                "Dept": dept,
                "Count": dept_sku_count,
                "Wallet": wallet,
                "Ceiling": ceiling
            })
    
    stat_df = pd.DataFrame(dept_stats).sort_values("Wallet", ascending=False)
    print(stat_df.head(10).to_string(index=False))
    
    print(f"\nTotal SKUs included: {len(tier_df)}")
    if len(tier_df) > 0:
        # Check ADS concentration
        top_sku = tier_df.sort_values("Avg_Daily_Sales", ascending=False).iloc[0]
        bottom_sku = tier_df.sort_values("Avg_Daily_Sales", ascending=False).iloc[-1]
        print(f"Top ADS SKU: {top_sku['Product']} ({top_sku['Avg_Daily_Sales']:.2f})")
        print(f"Bottom ADS SKU: {bottom_sku['Product']} ({bottom_sku['Avg_Daily_Sales']:.2f})")

run_tier_analysis("Small_200k", 200000, 0.02)
run_tier_analysis("Mega_115M", 115000000, 1.0)
