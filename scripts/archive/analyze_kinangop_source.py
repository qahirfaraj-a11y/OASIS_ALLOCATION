import pandas as pd

def analyze_supplier_source_data(supplier_name):
    file_path = "Full_Product_Allocation_Scorecard_v7.csv"
    print(f"Analyzing Source Data: {file_path}")
    
    try:
        df = pd.read_csv(file_path)
        
        # 1. Filter for Supplier
        # Normalize for case-insensitive match
        mask = df['Supplier'].astype(str).str.contains(supplier_name, case=False, na=False)
        sup_df = df[mask].copy()
        
        if sup_df.empty:
            print(f"No products found for '{supplier_name}'")
            return

        print(f"\nSUPPLIER PROFILE: {supplier_name.upper()}")
        print(f"Found {len(sup_df)} SKUs in Reference Store Data")
        print("="*80)
        
        # 2. Portfolio Overview
        total_revenue = sup_df['Total_Revenue'].sum()
        avg_margin = sup_df['Margin_Pct'].mean()
        
        # Weighted Margin by Revenue
        weighted_margin = (sup_df['Margin_Pct'] * sup_df['Total_Revenue']).sum() / total_revenue if total_revenue > 0 else 0
        
        print(f"Total Reference Revenue: KES {total_revenue:,.0f}")
        print(f"Avg Margin (Simple):     {avg_margin:.1f}%")
        print(f"Avg Margin (Weighted):   {weighted_margin:.1f}%")
        print("-" * 80)
        
        # 3. Product Breakdown (Ranked by Revenue)
        sup_df = sup_df.sort_values("Total_Revenue", ascending=False)
        
        print(f"{'Product Name':<45} {'ADS':<6} {'Price':<8} {'Margin':<6} {'Revenue':<12}")
        print("-" * 80)
        
        for _, row in sup_df.iterrows():
            prod = row['Product'][:43]
            ads = row['Avg_Daily_Sales']
            price = row['Unit_Price']
            margin = row['Margin_Pct']
            rev = row['Total_Revenue']
            
            print(f"{prod:<45} {ads:<6.1f} {price:<8.0f} {margin:<6.1f} {rev:,.0f}")
            
        print("="*80)
        
        # 4. Department Mix
        print("\nDEPARTMENT MIX:")
        dept_mix = sup_df.groupby('Department')['Total_Revenue'].sum().sort_values(ascending=False)
        for dept, rev in dept_mix.items():
            share = (rev / total_revenue) * 100
            print(f"- {dept:<20}: KES {rev:,.0f} ({share:.1f}%)")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    analyze_supplier_source_data("KINANGOP")
