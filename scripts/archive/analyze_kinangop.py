import pandas as pd
import glob
import os

def analyze_kinangop_performance():
    # Find latest simulation results
    files = glob.glob("simulation_results_*.xlsx")
    if not files:
        print("No simulation results found.")
        return

    latest_file = max(files, key=os.path.getmtime)
    print(f"Analyzing File: {latest_file}")
    
    try:
        # Read SKU Performance Sheet
        df = pd.read_excel(latest_file, sheet_name="SKU Performance")
        
        # Filter for Kinangop
        # Normalize supplier names for matching
        kinangop_df = df[df['Supplier'].astype(str).str.contains("KINANGOP", case=False, na=False)]
        
        if kinangop_df.empty:
            print("No products found for 'Kinangop Dairy'. Checking similar names...")
            # Print unique suppliers to help debug
            suppliers = sorted(df['Supplier'].dropna().unique().astype(str))
            for s in suppliers:
                if "KIN" in s.upper():
                    print(f"Match candidate: {s}")
            return

        print(f"\nFound {len(kinangop_df)} products for KINANGOP DAIRY")
        print("="*60)
        
        # 1. Overall Metrics
        total_revenue = (kinangop_df['Total Sales'] * kinangop_df['Unit Price']).sum()
        total_lost_value = (kinangop_df['Lost Sales'] * kinangop_df['Unit Price']).sum()
        total_demand = kinangop_df['Total Demand'].sum()
        total_sales = kinangop_df['Total Sales'].sum()
        
        fill_rate = (total_sales / total_demand * 100) if total_demand > 0 else 0
        
        # Substituted Sales (if column exists)
        if 'Substituted Sales' in kinangop_df.columns:
            sub_sales_units = kinangop_df['Substituted Sales'].sum()
            sub_sales_value = (kinangop_df['Substituted Sales'] * kinangop_df['Unit Price']).sum()
        else:
            sub_sales_units = 0
            sub_sales_value = 0

        print(f"Total Revenue:       KES {total_revenue:,.0f}")
        print(f"Lost Revenue:        KES {total_lost_value:,.0f}")
        print(f"Substituted Revenue: KES {sub_sales_value:,.0f} (Recovered)")
        print(f"Overall Fill Rate:   {fill_rate:.1f}%")
        print("-" * 60)

        # 2. Product Breakdown
        print(f"{'Product Name':<40} {'ADS':<5} {'Fill%':<6} {'Stockout Days':<14} {'Lost KES':<10}")
        print("-" * 80)
        
        for _, row in kinangop_df.iterrows():
            prod_name = row['Product'][:38]
            ads = row['ADS']
            # Calculate SKU Fill Rate
            sku_demand = row['Total Demand']
            sku_sales = row['Total Sales']
            sku_fill = (sku_sales / sku_demand * 100) if sku_demand > 0 else 100.0
            
            stockout_days = row['Stockout Days']
            lost_kes = row['Lost Sales'] * row['Unit Price']
            
            print(f"{prod_name:<40} {ads:<5.1f} {sku_fill:<6.1f} {stockout_days:<14} {lost_kes:,.0f}")

        print("="*60)
        
        # 3. Quick Insight
        avg_turnover = (kinangop_df['Total Sales'] / (kinangop_df['ADS'] * 60)).mean() # Rough turns calc
        print(f"Insight: Portfolio is performing at {fill_rate:.1f}% Fill Rate.")
        if fill_rate < 80:
             print("Analysis: High stockouts. Likely due to strict daily turnover targets vs volatility.")
        else:
             print("Analysis: Healthy performance.")
             
    except Exception as e:
        print(f"Error analyzing file: {e}")

if __name__ == "__main__":
    analyze_kinangop_performance()
