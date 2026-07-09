import os
import glob
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

import logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

BASE_DATE_STR = "2026-01-20"
BASE_DATE = datetime.strptime(BASE_DATE_STR, "%Y-%m-%d")

def get_days_in_month(year, month):
    if month in [1, 3, 5, 7, 8, 10, 12]: return 31
    if month in [4, 6, 9, 11]: return 30
    if year % 4 == 0 and (year % 100 != 0 or year % 400 == 0): return 29
    return 28

def load_baseline(data_dir):
    """Loads dept_*.xlsx to get starting SOH"""
    logging.info("Loading baseline snapshots...")
    dfs = []
    pattern = os.path.join(data_dir, "dept_*.xlsx")
    for file in glob.glob(pattern):
        try:
            df = pd.read_excel(file)
            if 'ITM_NAME' in df.columns and 'STOCK' in df.columns:
                dfs.append(df[['ITM_NAME', 'STOCK', 'VENDOR_NAME', 'DEPARTMENT', 'SellPrice', 'BARCODE']])
        except Exception as e:
            logging.error(f"Error reading {file}: {e}")
            
    if not dfs:
        raise ValueError("No baseline data found.")
        
    base_df = pd.concat(dfs, ignore_index=True)
    # Deduplicate keeping the max stock or last seen
    base_df = base_df.groupby('ITM_NAME').first().reset_index()
    base_df['STOCK'] = pd.to_numeric(base_df['STOCK'], errors='coerce').fillna(0)
    logging.info(f"Loaded {len(base_df)} items in baseline.")
    return base_df

def load_grns(data_dir):
    """Loads GRNs to build daily addition ledger."""
    logging.info("Loading GRN history...")
    dfs = []
    pattern = os.path.join(data_dir, "*grnd*.xlsx")
    files = glob.glob(pattern)
    if not files:
        # Fallback to checking any file with grnd in name
        files = [os.path.join(data_dir, f) for f in os.listdir(data_dir) if 'grnd' in f.lower() and f.endswith('.xlsx')]
        
    for file in files:
        try:
            # We skip 'Report' sheet checking and just read the first sheet
            df = pd.read_excel(file)
            if 'Item Name' in df.columns and 'GRN Date' in df.columns:
                # The user requested to use Ordered Qty (PO Qty) if available
                if 'PO Qty' in df.columns:
                    df['Add_Qty'] = pd.to_numeric(df['PO Qty'], errors='coerce').fillna(0)
                    # If PO Qty is 0, fallback to GRN Qty
                    if 'GRN Qty' in df.columns:
                        grn_qty = pd.to_numeric(df['GRN Qty'], errors='coerce').fillna(0)
                        df['Add_Qty'] = np.where(df['Add_Qty'] > 0, df['Add_Qty'], grn_qty)
                elif 'GRN Qty' in df.columns:
                    df['Add_Qty'] = pd.to_numeric(df['GRN Qty'], errors='coerce').fillna(0)
                else:
                    df['Add_Qty'] = 0
                    
                df['GRN Date'] = pd.to_datetime(df['GRN Date'], errors='coerce')
                df = df.dropna(subset=['GRN Date', 'Item Name'])
                
                dfs.append(df[['Item Name', 'GRN Date', 'Add_Qty']])
        except Exception as e:
            logging.error(f"Error reading {file}: {e}")
            
    if not dfs:
        return pd.DataFrame(columns=['Item Name', 'GRN Date', 'Add_Qty'])
        
    grn_df = pd.concat(dfs, ignore_index=True)
    # Aggregate by date and item
    grn_df = grn_df.groupby(['Item Name', 'GRN Date'])['Add_Qty'].sum().reset_index()
    logging.info(f"Loaded {len(grn_df)} daily GRN addition records.")
    return grn_df

def load_sales(data_dir, year=2026):
    """Loads *_cash.xlsx monthly summaries and converts to Daily Burn Rate"""
    logging.info("Loading Sales histories...")
    month_map = {
        'jan': 1, 'feb': 2, 'mar': 3, 'apr': 4, 'may': 5, 'jun': 6,
        'jul': 7, 'aug': 8, 'sep': 9, 'oct': 10, 'nov': 11, 'dec': 12
    }
    
    daily_burns = []
    
    for m_str, m_num in month_map.items():
        file_path = os.path.join(data_dir, f"{m_str}_cash.xlsx")
        if os.path.exists(file_path):
            try:
                # Based on peek, headers are on row 1
                df = pd.read_excel(file_path, header=1)
                if 'Item Name' in df.columns and 'Qty' in df.columns:
                    df['Qty'] = pd.to_numeric(df['Qty'], errors='coerce').fillna(0)
                    days_in_m = get_days_in_month(year, m_num)
                    df['Daily_Burn'] = df['Qty'] / days_in_m
                    df['Month'] = m_num
                    daily_burns.append(df[['Item Name', 'Month', 'Daily_Burn']])
            except Exception as e:
                logging.error(f"Error reading {file_path}: {e}")
                
    if not daily_burns:
        return pd.DataFrame(columns=['Item Name', 'Month', 'Daily_Burn'])
        
    burn_df = pd.concat(daily_burns, ignore_index=True)
    burn_df = burn_df.groupby(['Item Name', 'Month'])['Daily_Burn'].sum().reset_index()
    logging.info(f"Loaded daily burn rates for {len(burn_df)} item/month combinations.")
    return burn_df

def get_daily_burn_for_date(burn_df, target_date):
    """Extracts a mapping of Item -> Daily Burn for a specific month."""
    m_num = target_date.month
    sub = burn_df[burn_df['Month'] == m_num]
    return dict(zip(sub['Item Name'], sub['Daily_Burn']))

def run_extrapolation(target_date_str, data_dir=None):
    if data_dir is None:
        # data_dir is at oasis/data. This file is at oasis/logic/extrapolate_stock.py
        data_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
        
    target_date = datetime.strptime(target_date_str, "%Y-%m-%d")
    
    base_df = load_baseline(data_dir)
    grn_df = load_grns(data_dir)
    burn_df = load_sales(data_dir)
    
    # Initialize Current SOH from Base
    soh_map = dict(zip(base_df['ITM_NAME'], base_df['STOCK']))
    
    grn_df['Upper_Name'] = grn_df['Item Name'].astype(str).str.upper().str.strip()
    direction = 1 if target_date > BASE_DATE else -1
    current_date = BASE_DATE
    
    logging.info(f"Extrapolating from {BASE_DATE_STR} to {target_date_str}...")
    
    step_count = 0
    while current_date.date() != target_date.date():
        process_date = current_date
        daily_burns = get_daily_burn_for_date(burn_df, process_date)
        day_grns = grn_df[grn_df['GRN Date'].dt.date == process_date.date()]
        grn_map = dict(zip(day_grns['Upper_Name'], day_grns['Add_Qty']))
        
        all_items = set(soh_map.keys()).union(set(daily_burns.keys()))
        for item in all_items:
            item_upper = str(item).upper().strip()
            burn = daily_burns.get(item, 0.0)
            grn_add = grn_map.get(item_upper, 0.0)
            curr_stock = soh_map.get(item, 0.0)
            
            if direction == 1:
                new_stock = curr_stock + grn_add - burn
            else:
                new_stock = curr_stock - grn_add + burn
            soh_map[item] = new_stock
            
        current_date += timedelta(days=direction)
        step_count += 1
        
    logging.info(f"Extrapolation complete in {step_count} day steps.")
    base_df['Current_Stock'] = base_df['ITM_NAME'].map(soh_map).fillna(0).round(2)
    
    output_filename = f"Extrapolated_Scorecard_{target_date.strftime('%Y%m%d')}.csv"
    output_path = os.path.join(data_dir, output_filename)
    
    out_df = base_df.rename(columns={
        'ITM_NAME': 'Product',
        'VENDOR_NAME': 'Supplier',
        'DEPARTMENT': 'Department',
        'SellPrice': 'Unit_Price'
    })
    
    overall_burns = burn_df.groupby('Item Name')['Daily_Burn'].mean()
    out_df['Avg_Daily_Sales'] = out_df['Product'].map(overall_burns).fillna(0.0).round(4)
    
    out_df.to_csv(output_path, index=False)
    logging.info(f"Saved Extrapolated Scorecard to: {output_path}")
    return output_path

if __name__ == "__main__":
    import sys
    if len(sys.argv) < 2:
        print("Usage: python extrapolate_stock.py YYYY-MM-DD")
        sys.exit(1)
        
    run_extrapolation(sys.argv[1])
