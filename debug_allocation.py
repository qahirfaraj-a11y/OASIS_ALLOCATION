import pandas as pd
import json
import os
import numpy as np

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
SCORECARD_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"

def debug_allocation():
    df = pd.read_csv(SCORECARD_PATH)
    
    # Re-run a bit of the logic to see what's happening
    budget_name = "Med_2.5M"
    target_budget = 2500000
    
    staple_mask = (df['Is_Staple'] == True) & (df['Stocking_Notes'].str.contains("-> Eligible"))
    staples_sorted = df[staple_mask].sort_values(by="Score_Weighted", ascending=False)
    
    print(f"Total Eligible Staples: {len(staples_sorted)}")
    
    current_spent = 0
    picked_count = 0
    for idx in staples_sorted.index:
        cost = df.at[idx, 'Capital_Required']
        if current_spent + cost <= target_budget:
            picked_count += 1
            current_spent += cost
        else:
            # print(f"Rejected: {df.at[idx, 'Product']} | Cost: {cost} | Current: {current_spent}")
            pass
            
    print(f"Staples picked for {budget_name}: {picked_count}, Total Spent: ${current_spent:,.2f}")

if __name__ == "__main__":
    debug_allocation()
