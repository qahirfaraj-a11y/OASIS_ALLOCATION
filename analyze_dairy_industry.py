import pandas as pd
import glob
import os
import datetime
import numpy as np

def analyze_dairy_industry():
    print("STARTING DAIRY INDUSTRY ANALYSIS (Multi-Source)")
    print("="*80)
    
    # Configuration
    COMPETITORS = ["KINANGOP", "BROOKSIDE", "TUZO", "FRESHA", "KCC", "DAIRY"]
    
    # Data Containers
    sales_stats = {} # {Supplier: {Qty: 0, Rev: 0, Days: set()}}
    supply_stats = {} # {Supplier: {Deliveries: 0, Volume: 0}}
    
    # --- 1. CASH ANALYSIS (Sales Patterns) ---
    print("\n[1/3] Processing Cash Files (Sales)...")
    cash_files = glob.glob(r"oasis/data/*_cash.xlsx")
    
    for cf in cash_files:
        month_name = os.path.basename(cf).replace("_cash.xlsx", "").upper()
        print(f"  - Reading {month_name}...", end="\r")
        try:
            # Heuristic: Header often on row 1 (index 1) based on previous inspection
            # 'Item Name' was found at row 1.
            df = pd.read_excel(cf, header=1)
            
            # Filter for Dairy Candidates
            # Since Cash files don't have Supplier, we must infer from Item Name
            # We look for keywords like "MILK", "YOGHURT", "BUTTER", "GHEE"
            # AND then try to attribute to supplier based on Brand Name
            
            mask = df['Item Name'].astype(str).str.contains("MILK|YOGHURT|BUTTER|GHEE|CREAM", case=False, na=False)
            dairy_df = df[mask].copy()
            
            for _, row in dairy_df.iterrows():
                item = str(row['Item Name']).upper()
                qty = float(row.get('Qty', 0))
                
                # Attribute to Supplier
                supplier_match = "OTHER"
                for comp in COMPETITORS:
                    if comp in item:
                        supplier_match = comp
                        break
                
                if supplier_match not in sales_stats:
                    sales_stats[supplier_match] = {'Qty': 0, 'Count': 0}
                
                sales_stats[supplier_match]['Qty'] += qty
                sales_stats[supplier_match]['Count'] += 1
                
        except Exception as e:
            print(f"    Error reading {cf}: {e}")
            
    print("\n   Done processing Cash Files.")

    # --- 2. PRTS ANALYSIS (Supply Chain) ---
    print("\n[2/3] Processing PRTS Files (Purchases/Returns)...")
    prts_files = glob.glob(r"oasis/data/prts_*.xlsx")
    
    for pf in prts_files:
        print(f"  - Reading {os.path.basename(pf)}...", end="\r")
        try:
            df = pd.read_excel(pf)
            # Columns: 'Ven Code / Name', 'Item Name', 'Net Amt', 'Doc Date', 'Qty' (maybe 'Rejc Qty'?)
            # Previous inspection showed 'Rejc Qty'. Let's check for 'Qty' or assume 'Net Amt' is key.
            # Wait, PRTS usually means PURCHASES. 
            # Let's use 'Net Amt' and count frequency.
            
            # Filter for Competitors in Vendor Name
            for comp in COMPETITORS:
                mask = df['Ven Code / Name'].astype(str).str.contains(comp, case=False, na=False)
                comp_df = df[mask]
                
                if not comp_df.empty:
                    if comp not in supply_stats:
                        supply_stats[comp] = {'Deliveries': 0, 'Value': 0}
                    
                    supply_stats[comp]['Deliveries'] += len(comp_df['Doc No'].unique()) # Unique Deliveries
                    supply_stats[comp]['Value'] += comp_df['Net Amt'].sum()

        except Exception as e:
            print(f"    Error: {e}")

    print("\n   Done processing PRTS Files.")

    # --- 3. CONSOLIDATED REPORT ---
    print("\n" + "="*80)
    print("DAIRY INDUSTRY BENCHMARKING REPORT")
    print("="*80)
    print(f"{'SUPPLIER':<15} | {'SALES (Est Qty)':<15} | {'PURCHASES (KES)':<15} | {'DELIVERIES':<10}")
    print("-" * 80)
    
    all_suppliers = set(sales_stats.keys()) | set(supply_stats.keys())
    
    for sup in sorted(all_suppliers):
        if sup == "OTHER": continue
        
        s_data = sales_stats.get(sup, {'Qty': 0})
        p_data = supply_stats.get(sup, {'Value': 0, 'Deliveries': 0})
        
        sales_qty = s_data['Qty']
        purch_val = p_data['Value']
        deliv_cnt = p_data['Deliveries']
        
        print(f"{sup:<15} | {sales_qty:<15,.0f} | {purch_val:<15,.0f} | {deliv_cnt:<10}")

    print("-" * 80)
    
    # Kinangop Deep Dive
    if "KINANGOP" in sales_stats:
        k_share = sales_stats['KINANGOP']['Qty']
        total_dairy = sum(d['Qty'] for k,d in sales_stats.items())
        share_pct = (k_share / total_dairy * 100) if total_dairy > 0 else 0
        print(f"\nKINANGOP MARKET SHARE (Vol): {share_pct:.1f}% of identified Dairy items.")

if __name__ == "__main__":
    analyze_dairy_industry()
