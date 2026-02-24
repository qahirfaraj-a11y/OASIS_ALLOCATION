import pandas as pd
import glob
import os
import datetime

def analyze_kinangop_sku_details():
    print("STARTING SKU-LEVEL FLOW ANALYSIS (KINANGOP DAIRY)")
    print("="*80)
    
    # Prices for Lost Revenue Calc
    # From Scorecard V7:
    PRICES = {
        'KINANGOP FRESH 500ML MILK POUCH': 50,
        'KINANGOP 500ML GOLD FINO ASEPTIC': 55,
        'KINANGOP 500ML LONGLIFE MILK': 53
    }
    DEFAULT_PRICE = 50
    
    # 1. RETURNS BREAKDOWN (by SKU)
    print("Step 1: Categorizing Returns by Product...")
    prts_files = glob.glob(r"oasis/data/prts_*.xlsx")
    returns_breakdown = {} # {(Month, Product): Qty}
    
    for pf in prts_files:
        try:
            df = pd.read_excel(pf)
            mask = df['Ven Code / Name'].astype(str).str.contains("KINANGOP", case=False, na=False)
            k_df = df[mask].copy()
            
            if not k_df.empty:
                for _, row in k_df.iterrows():
                    date = pd.to_datetime(row['Doc Date'])
                    month = date.strftime('%b').upper()
                    # Clean Item Name
                    item = str(row['Item Name']).upper().strip()
                    qty = row['Rejc Qty']
                    
                    if qty > 0:
                        key = (month, item)
                        returns_breakdown[key] = returns_breakdown.get(key, 0) + qty
        except: pass

    # 2. SALES BREAKDOWN (by SKU)
    print("Step 2: breaking down Sales by Product...")
    cash_files = glob.glob(r"oasis/data/*_cash.xlsx")
    sales_breakdown = {} # {(Month, Product): Qty}
    
    for cf in cash_files:
        try:
            month_name = os.path.basename(cf).replace("_cash.xlsx", "").upper()
            df = pd.read_excel(cf, header=1)
            mask = df['Item Name'].astype(str).str.contains("KINANGOP", case=False, na=False)
            k_df = df[mask]
            
            for _, row in k_df.iterrows():
                item = str(row['Item Name']).upper().strip()
                qty = row['Qty']
                
                key = (month_name, item)
                sales_breakdown[key] = sales_breakdown.get(key, 0) + qty
        except: pass

    # 3. GENERATE SKU REPORT
    print("\n" + "="*80)
    print("SKU PERFORMANCE & FINANCIAL IMPACT")
    print("="*80)
    
    # Identify all Products
    products = set([k[1] for k in sales_breakdown.keys()] + [k[1] for k in returns_breakdown.keys()])
    months = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']
    
    for prod in sorted(products):
        print(f"\nPRODUCT: {prod}")
        print(f"{'MONTH':<10} | {'SOLD':<8} | {'RET.':<8} | {'RET %':<8} | {'LOST REV (KES)':<15}")
        print("-" * 60)
        
        total_lost_rev = 0
        total_sales = 0
        
        price = PRICES.get(prod, DEFAULT_PRICE)
        
        for m in months:
            s = sales_breakdown.get((m, prod), 0)
            r = returns_breakdown.get((m, prod), 0)
            
            if s == 0 and r == 0: continue
            
            ret_pct = (r/s*100) if s > 0 else 0
            lost_rev = r * price
            
            total_sales += s
            total_lost_rev += lost_rev
            
            # Highlight bad months
            flag = ""
            if ret_pct > 10: flag = " << ALERT"
            
            print(f"{m:<10} | {s:<8,.0f} | {r:<8.0f} | {ret_pct:<8.1f}% | {lost_rev:<15,.0f}{flag}")
            
        print("-" * 60)
        print(f"TOTAL LOST REVENUE: KES {total_lost_rev:,.0f} (on {total_sales:,.0f} sales)")

if __name__ == "__main__":
    analyze_kinangop_sku_details()
