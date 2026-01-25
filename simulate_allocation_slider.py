import pandas as pd
import numpy as np
import sys

# Configuration
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"

def load_data():
    print(f"Loading Scorecard from {SCORECARD_FILE}...")
    try:
        df = pd.read_csv(SCORECARD_FILE)
        return df
    except FileNotFoundError:
        print("Error: Scorecard file not found. Please run generate_allocation_scorecard.py first.")
        sys.exit(1)

def get_allocation_profile(budget):
    """
    Determines the retailer profile and strategy rules based on the slider (budget) value.
    """
    if budget < 500000:
        return {
            "name": "Small Retailer (Efficiency Mode)",
            "allowed_abc": ["A"], # Only Top Movers
            "allowed_roles": ["Cash Cow", "Star", "Profit Driver"], # No Dogs/Standard
            "min_gmroi": 5.0, # High efficiency required
            "dept_balance_strictness": "Loose" # Focus on ROI over perfect variety
        }
    elif budget < 5000000:
        return {
            "name": "Medium Retailer (Growth Mode)",
            "allowed_abc": ["A", "B"], # A and B items
            "allowed_roles": ["Cash Cow", "Star", "Profit Driver", "Standard"], 
            "min_gmroi": 1.0, # Moderate efficiency
            "dept_balance_strictness": "Moderate"
        }
    elif budget < 20000000:
        return {
            "name": "Large Retailer (Assortment Mode)",
            "allowed_abc": ["A", "B", "C", "D"], # Full Range
            "allowed_roles": ["Cash Cow", "Star", "Profit Driver", "Standard", "Dog"], # Long tail included
            "min_gmroi": 0.0,
            "dept_balance_strictness": "Strict" # Maintain department presence
        }
    else:
        return {
            "name": "Mega Retailer (Dominance Mode)",
            "allowed_abc": ["A", "B", "C", "D"], # Complete Everything
            "allowed_roles": ["Cash Cow", "Star", "Profit Driver", "Standard", "Dog"], 
            "min_gmroi": -99.0, # No efficiency floor - Stock EVERYTHING
            "dept_balance_strictness": "Perfect" # Maximum breadth
        }

def allocate_budget(df, total_budget):
    profile = get_allocation_profile(total_budget)
    print(f"\n--- Simulation: Budget ${total_budget:,.2f} ---")
    print(f"Profile: {profile['name']}")
    print(f"Rules: ABC={profile['allowed_abc']}, Min GMROI={profile['min_gmroi']}")

    # 1. Filter Candidates based on Profile Rules
    mask = (
        df["ABC_Class"].isin(profile["allowed_abc"]) &
        (df["Strategy_Role"].isin(profile["allowed_roles"])) &
        (df["GMROI"] >= profile["min_gmroi"]) &
        (df["Unit_Price"] > 0)
    )
    candidates = df[mask].copy()
    
    print(f"Eligible SKUs: {len(candidates)} / {len(df)}")

    # 2. Calculate Department Targets
    # Use the global department shares from the full dataset to guide allocation
    # (Or re-calculate based on candidates if we strictly want only efficient dept items)
    # Let's use global shares to ensure we try to represent all depts.
    dept_revenue = df.groupby("Department")["Total_Revenue"].sum()
    global_shares = dept_revenue / dept_revenue.sum()
    
    # 3. Allocation Loop
    selected_indices = []
    current_spend = 0
    
    # Sort candidates by Score (Efficiency)
    candidates = candidates.sort_values(by="Score_Weighted", ascending=False)
    
    # We will try to fill department buckets.
    # If a dept bucket is full or runs out of items, we move to the next best globally.
    
    # Init allocated spend per dept
    dept_spend = {dept: 0.0 for dept in global_shares.index}
    
    # First Pass: Fill proportionate buckets
    for dept, share in global_shares.items():
        target_spend = total_budget * share
        
        dept_items = candidates[candidates["Department"] == dept]
        
        for idx, row in dept_items.iterrows():
            cost = row["Capital_Required"]
            if dept_spend[dept] + cost <= target_spend:
                selected_indices.append(idx)
                dept_spend[dept] += cost
                current_spend += cost
            else:
                # Bucket full for this pass
                continue
                
    remaining_budget = total_budget - current_spend
    print(f"First Pass Allocated: ${current_spend:,.2f}. Remaining: ${remaining_budget:,.2f}")
    
    # Second Pass: Fill remaining budget with best available global items (ignoring strict dept caps)
    # Exclude already selected
    candidates_remaining = candidates.drop(selected_indices, errors='ignore')
    
    for idx, row in candidates_remaining.iterrows():
        cost = row["Capital_Required"]
        if current_spend + cost <= total_budget:
            selected_indices.append(idx)
            current_spend += cost
            dept_name = row["Department"]
            dept_spend[dept_name] = dept_spend.get(dept_name, 0) + cost
        
        if current_spend >= total_budget * 0.99: # 99% utilized
            break
            
    # Result Construction
    basket = df.loc[selected_indices].copy()
    
    # Summary Stats
    print(f"Final Spend: ${current_spend:,.2f} ({current_spend/total_budget:.1%})")
    print("\nBasket Composition (ABC):")
    print(basket["ABC_Class"].value_counts(normalize=True).mul(100).round(1).astype(str) + '%')
    
    print("\nBasket Composition (Strategy):")
    print(basket["Strategy_Role"].value_counts())
    
    print("\nDepartment Allocation:")
    dept_summary = basket.groupby("Department")["Capital_Required"].sum().sort_values(ascending=False)
    print(dept_summary.apply(lambda x: f"${x:,.0f}"))

    return basket

def main():
    if len(sys.argv) > 1:
        try:
            budget_input = float(sys.argv[1])
            budgets_to_test = [budget_input]
        except ValueError:
            print("Invalid budget input. Running defaults.")
            budgets_to_test = [300000, 1500000, 8000000]
    else:
        budgets_to_test = [300000, 1500000, 8000000]

    df = load_data()
    
    for b in budgets_to_test:
        allocate_budget(df, b)
        print("-" * 40)

if __name__ == "__main__":
    main()
