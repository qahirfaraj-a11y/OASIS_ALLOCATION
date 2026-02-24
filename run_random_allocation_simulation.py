import pandas as pd
import numpy as np
import random
import sys
# Add current directory to path just in case
sys.path.append('.')
from simulate_allocation_slider import load_data, allocate_budget

def run_simulation():
    try:
        df = load_data()
    except Exception as e:
        print(f"Failed to load data: {e}")
        return

    tiers = [
        ("Small - Efficiency", 100000, 450000),
        ("Medium - Growth", 600000, 4500000),
        ("Large - Assortment", 6000000, 18000000),
        ("Mega - Dominance", 25000000, 40000000)
    ]
    
    results = []
    
    print("\n" + "="*80)
    print("STARTING RANDOM CAPITAL ALLOCATION SIMULATION")
    print("="*80)
    
    for name, min_b, max_b in tiers:
        budget = random.uniform(min_b, max_b)
        print(f"\n>>> Simulating {name} Tier with Budget ${budget:,.2f}")
        
        try:
            # Capture the basket returned
            basket = allocate_budget(df, budget)
            
            # Calculate key metrics
            spend = basket["Capital_Required"].sum()
            sku_count = len(basket)
            
            # ABC Comp
            abc_counts = basket["ABC_Class"].value_counts(normalize=True)
            a_share = abc_counts.get("A", 0)
            b_share = abc_counts.get("B", 0)
            c_share = abc_counts.get("C", 0)
            
            # Strategy Comp
            roles = basket["Strategy_Role"].value_counts()
            cash_cows = roles.get("Cash Cow", 0)
            
            results.append({
                "Tier": name,
                "Budget": budget,
                "Actual Spend": spend,
                "Util %": spend / budget,
                "SKU Count": sku_count,
                "A %": a_share,
                "A+B %": a_share + b_share,
                "Cash Cows": cash_cows
            })
        except Exception as e:
            print(f"Error processing {name}: {e}")
            
    print("\n" + "="*80)
    print("FINAL RESULTS SUMMARY")
    print("="*80)
    
    if not results:
        print("No results generated.")
        return

    summary_df = pd.DataFrame(results)
    
    # Format for display
    # Keep raw numbers in DF for any potential further analysis, but print formatted
    print_df = summary_df.copy()
    print_df["Budget"] = print_df["Budget"].apply(lambda x: f"${x:,.0f}")
    print_df["Actual Spend"] = print_df["Actual Spend"].apply(lambda x: f"${x:,.0f}")
    print_df["Util %"] = print_df["Util %"].apply(lambda x: f"{x:.1%}")
    print_df["A %"] = print_df["A %"].apply(lambda x: f"{x:.1%}")
    print_df["A+B %"] = print_df["A+B %"].apply(lambda x: f"{x:.1%}")
    
    # Adjust column width for readability
    pd.set_option('display.max_columns', None)
    pd.set_option('display.width', 1000)
    pd.set_option('display.colheader_justify', 'center')

    print(print_df.to_string(index=False))
    print("\n" + "="*80)

if __name__ == "__main__":
    run_simulation()
