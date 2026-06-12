import pandas as pd
import glob
import os
import datetime

def analyze_kinangop_flow():
    print("STARTING KINANGOP DETAILED FLOW ANALYSIS")
    print("="*80)
    
    # 1. RETURNS DATA (Daily from PRTS)
    print("Step 1: extracting Daily Returns from PRTS logs...")
    prts_files = glob.glob(r"oasis/data/prts_*.xlsx")
    returns_data = []
    
    for pf in prts_files:
        try:
            df = pd.read_excel(pf)
            # Filter for Kinangop
            mask = df['Ven Code / Name'].astype(str).str.contains("KINANGOP", case=False, na=False)
            k_df = df[mask].copy()
            
            if not k_df.empty:
                # Group by Date
                daily_grp = k_df.groupby('Doc Date')['Rejc Qty'].sum()
                for date, qty in daily_grp.items():
                    if qty > 0:
                        returns_data.append({'Date': date, 'Returns': qty})
                        
        except Exception as e:
            pass

    returns_df = pd.DataFrame(returns_data)
    if not returns_df.empty:
        returns_df['Date'] = pd.to_datetime(returns_df['Date'])
        returns_df = returns_df.sort_values('Date')
        print(f"Found {len(returns_df)} days with returns (Total: {returns_df['Returns'].sum():.0f} units)")
    else:
        print("No returns found for Kinangop.")

    # 2. SALES DATA (Monthly from Cash Files)
    print("\nStep 2: Extracting Monthly Sales from Cash Files...")
    cash_files = glob.glob(r"oasis/data/*_cash.xlsx")
    sales_data = []
    
    for cf in cash_files:
        try:
            month_name = os.path.basename(cf).replace("_cash.xlsx", "").upper()
            df = pd.read_excel(cf, header=1)
            
            # Filter Kinangop Products (approx by name)
            mask = df['Item Name'].astype(str).str.contains("KINANGOP", case=False, na=False)
            k_df = df[mask]
            
            total_qty = k_df['Qty'].sum()
            
            # Days in month (heuristic)
            days = 30
            if month_name in ['JAN', 'MAR', 'MAY', 'JUL', 'OCT', 'DEC']: days = 31
            elif month_name == 'FEB': days = 28
            
            ads = total_qty / days
            
            sales_data.append({
                'Month': month_name, 
                'Total_Sales': total_qty,
                'ADS': ads
            })
            
        except Exception as e:
            pass
            
    sales_df = pd.DataFrame(sales_data)
    
    # 3. CONSOLIDATED REPORT
    print("\n" + "="*80)
    print("KINANGOP: SALES vs RETURNS PERFORMANCE")
    print("="*80)
    print(f"{'MONTH':<10} | {'SALES (Qty)':<12} | {'ADS (Est)':<10} | {'RETURNS (Qty)':<12} | {'RETURN %':<10}")
    print("-" * 80)
    
    # Map months to their returns
    # We need to extract Month from returns_df dates
    if not returns_df.empty:
        returns_df['MonthStr'] = returns_df['Date'].dt.strftime('%b').str.upper()
        monthly_returns = returns_df.groupby('MonthStr')['Returns'].sum()
    else:
        monthly_returns = {}
        
    # Sort sales by calendar order (helper)
    month_order = {'JAN':1, 'FEB':2, 'MAR':3, 'APR':4, 'MAY':5, 'JUN':6, 
                   'JUL':7, 'AUG':8, 'SEP':9, 'OCT':10, 'NOV':11, 'DEC':12}
    
    sales_df['Order'] = sales_df['Month'].map(month_order)
    sales_df = sales_df.sort_values('Order')
    
    for _, row in sales_df.iterrows():
        m = row['Month']
        s = row['Total_Sales']
        ads = row['ADS']
        r = monthly_returns.get(m, 0) # Assumes dates match year (2025 in recent files)
        
        # Careful: Cash files might be prior year? 
        # PRTS showed 2025 dates. Assuming alignment.
        
        ret_pct = (r / s * 100) if s > 0 else 0
        
        print(f"{m:<10} | {s:<12,.0f} | {ads:<10.1f} | {r:<12,.0f} | {ret_pct:<9.1f}%")
        
    print("-" * 80)

if __name__ == "__main__":
    analyze_kinangop_flow()
