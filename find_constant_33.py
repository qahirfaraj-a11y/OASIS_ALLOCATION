import pandas as pd

try:
    df = pd.read_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv")
    
    scenarios = ["Small_200k", "Med_2.5M", "Large_10M", "Mega_115M"]
    total_revenue_all = df['Total_Revenue'].sum()
    total_capital_all = df['Capital_Required'].sum() # Based on Mega/Ideal
    
    print(f"Total Potential Revenue: {total_revenue_all:,.2f}")
    print(f"Total Potential Capital (Ideal): {total_capital_all:,.2f}")
    
    for sc in scenarios:
        print(f"\n--- {sc} ---")
        if sc not in df.columns:
            print("Column not found")
            continue
            
        subset = df[df[sc] == True]
        
        count = len(subset)
        rev_sum = subset['Total_Revenue'].sum()
        cap_sum_ideal = subset['Capital_Required'].sum() # This is NOT the actual capital for the scenario, but the ideal capital of selected items
        
        ratio_rev_total = rev_sum / total_revenue_all if total_revenue_all else 0
        ratio_count = count / len(df)
        
        print(f"Count: {count} ({ratio_count:.2%})")
        print(f"Sum Allocated Revenue (Monthly): {rev_sum:,.2f}")
        print(f"  % of Total Potential Rev: {ratio_rev_total:.2%}")
        
        # Check if 'Estimated Revenue' / Something == 33%
        # Maybe Revenue / Capital?
        # Note: Capital_Required in CSV is for Ideal scenario (Mega).
        # For Small, the actual capital is much lower.
        
except Exception as e:
    print(e)
