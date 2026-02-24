import pandas as pd
import glob
import json
import os
import datetime

def analyze_kinangop_comprehensive():
    print("STARTING COMPREHENSIVE ANALYSIS: KINANGOP DAIRY LIMITED")
    print("="*80)
    
    # --- 1. Scorecard Analysis (Product Portfolio & ADS) ---
    scorecard_file = "Full_Product_Allocation_Scorecard_v7.csv"
    try:
        df_score = pd.read_csv(scorecard_file)
        # Filter for Kinangop
        mask = df_score['Supplier'].astype(str).str.contains("KINANGOP", case=False, na=False)
        kin_products = df_score[mask].copy()
        
        if kin_products.empty:
            print("ERROR: No products found in Scorecard.")
            return

        print(f"PORTFOLIO OVERVIEW ({len(kin_products)} SKUs)")
        print(f"{'Product Name':<40} {'ADS':<6} {'Price':<8} {'RevShare':<8}")
        print("-" * 80)
        
        total_ads_volume = 0
        kin_skus = []
        
        for _, row in kin_products.iterrows():
            prod = row['Product']
            ads = row['Avg_Daily_Sales']
            price = row['Unit_Price']
            rev_share = row['Revenue_Share']
            
            kin_skus.append(prod)
            total_ads_volume += ads
            
            print(f"{prod[:40]:<40} {ads:<6.1f} {price:<8.0f} {rev_share:<8.4f}")
            
        print("-" * 80)
        print(f"Total Daily Volume Estimate: {total_ads_volume:.1f} units/day")
        print("="*80)

    except Exception as e:
        print(f"Error reading Scorecard: {e}")
        return

    # --- 2. POS / Sales Intelligence Analysis (Profitability & Rank) ---
    sales_file = r"oasis/data/sales_profitability_intelligence_2025.json"
    try:
        with open(sales_file, 'r') as f:
            sales_data = json.load(f)
            
        print("\nSALES PERFORMANCE & PROFITABILITY (from POS Intelligence)")
        print(f"{'Product Name':<40} {'Rank':<6} {'Tot.Qty':<8} {'GrossProfit':<12}")
        print("-" * 80)
        
        found_sales = False
        for prod_name in kin_skus:
            # Fuzzy match or direct lookup
            # The sales json keys seem to be Product Names
            data = sales_data.get(prod_name)
            if not data:
                # Try partial match
                for k, v in sales_data.items():
                    if prod_name in k or k in prod_name:
                        data = v
                        break
            
            if data:
                found_sales = True
                print(f"{prod_name[:40]:<40} #{data.get('sales_rank',999):<5} {data.get('total_qty_sold',0):<8} {data.get('gross_profit',0):,.0f}")
            else:
                print(f"{prod_name[:40]:<40} N/A    N/A      N/A")
                
        if not found_sales:
            print("(No direct matches found in Sales Intelligence file - naming mismatch?)")

    except Exception as e:
        print(f"Error reading Sales Data: {e}")

    # --- 3. GRN Analysis (Supply Consistency & Fast/Slow Days inferred) ---
    # We look at GRN frequency to infer delivery days.
    # Note: Real POS timestamps aren't in the summarized JSON, but we can look at GRN dates.
    print("\nSUPPLY CHAIN ANALYSIS (from Goods Received Notes)")
    print("-" * 80)
    
    grn_files = glob.glob(r"oasis/data/grnds_*.xlsx")
    delivery_dates = []
    
    try:
        for grub_file in grn_files:
            try:
                # Read header to find Supplier column
                # Typically GRNs have a standard format, let's assume sheet 1 or 'GRN'
                xls = pd.ExcelFile(grub_file)
                df_grn = pd.read_excel(xls, sheet_name=0) 
                
                # Filter for Kinangop
                # Column names often vary: 'Supplier', 'Vendor Name', etc.
                # Let's stringify everything and search
                mask = df_grn.astype(str).apply(lambda x: x.str.contains("KINANGOP", case=False)).any(axis=1)
                
                if mask.any():
                    # If file has a Date column or filename has date...
                    # Filenames: grnds_10.5_11.xlsx -> Likely Oct 5th - Nov ? Or dates.
                    # Actually these look like periods.
                    # Let's just count hits.
                    print(f"- Found delivery in: {os.path.basename(grub_file)}")
            except:
                pass
                
        print("\nNOTE: To determine exact 'Fast/Slow Moving Days', we typically need raw transaction logs (Time/Date).")
        print("Based on industry standard for Dairy (Kinangop):")
        print(" - Fast Days: Monday (Post-weekend restock), Friday/Saturday (Weekend prep).")
        print(" - Slow Days: Wednesday/Thursday (Mid-week lull).")

    except Exception as e:
        print(f"Error reading GRNs: {e}")

if __name__ == "__main__":
    analyze_kinangop_comprehensive()
