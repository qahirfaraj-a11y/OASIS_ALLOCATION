import pandas as pd
import numpy as np
import glob
import os

print("Starting Human vs Engine Simulation Builder...")

# 1. Combine Stock Files
base_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
dept_files = glob.glob(os.path.join(base_dir, 'oasis', 'data', 'dept_*.xlsx'))
print(f"Loading {len(dept_files)} department stock files...")
dfs_stock = []
for f in dept_files:
    df = pd.read_excel(f)
    dfs_stock.append(df)
df_stock = pd.concat(dfs_stock, ignore_index=True) if dfs_stock else pd.DataFrame()

if not df_stock.empty:
    # Standardize
    df_stock = df_stock.rename(columns={
        'ITM_NAME': 'Item_Name',
        'STOCK': 'SOH',
        'SellPrice': 'Selling_Price',
        'VENDOR_NAME': 'Supplier',
        'DEPARTMENT': 'Department',
        'BARCODE': 'Barcode'
    })
    # Approximate Cost Price (75% of Sell) if missing
    df_stock['Unit_Cost'] = df_stock['Selling_Price'] * 0.75

    # Load ADS (Try to find a cash file or just use random for simulation)
    df_stock['ADS'] = np.random.uniform(0.1, 5.0, len(df_stock)).round(2)
    # Or if corrected_ads_from_pos.json exists:
    ads_path = os.path.join(base_dir, 'oasis', 'data', 'corrected_ads_from_pos.json')
    if os.path.exists(ads_path):
        import json
        with open(ads_path) as f:
            ads_map = json.load(f)
        df_stock['Fallback_ADS'] = np.random.uniform(0.1, 5.0, len(df_stock)).round(2)
        df_stock['ADS'] = df_stock.apply(lambda row: ads_map.get(str(row['Item_Name']), row['Fallback_ADS']), axis=1)
        df_stock.drop(columns=['Fallback_ADS'], inplace=True)

    print(f"Master stock built: {len(df_stock)} items.")

    # Save Scorecard
    scorecard_path = os.path.join(base_dir, 'oasis', 'data', 'Full_Product_Allocation_Scorecard_vSim.csv')
    df_stock.to_csv(scorecard_path, index=False)
    print(f"Saved scorecard to {scorecard_path}")

# 2. Combine human POs
po_files = glob.glob(os.path.join(base_dir, 'oasis', 'data', 'po_*.xlsx'))
print(f"Loading {len(po_files)} human PO files...")
dfs_po = []
for f in po_files:
    df = pd.read_excel(f)
    dfs_po.append(df)
if dfs_po and not df_stock.empty:
    df_po = pd.concat(dfs_po, ignore_index=True)
    # Aggregate PO amounts by vendor
    df_po['Clean_Vendor'] = df_po['Vendor Code / Name'].astype(str).str.split('-').str[-1].str.strip().str.upper()
    vendor_budgets = df_po.groupby('Clean_Vendor')['Net Amt'].sum().to_dict()
    print(f"Aggregated human budgets for {len(vendor_budgets)} vendors.")
    
    # Clean stock vendors for matching
    df_stock['Clean_Vendor'] = df_stock['Supplier'].astype(str).str.strip().str.upper()
    
    # 3. Regress budget to line items
    mock_po_lines = []
    
    for vendor, budget in vendor_budgets.items():
        v_items = df_stock[df_stock['Clean_Vendor'].str.contains(vendor, na=False, regex=False)]
        if v_items.empty:
            continue
            
        remaining_budget = budget
        # Sort items by urgency (Stock / ADS ratio)
        v_items = v_items.copy()
        v_items['SOH'] = pd.to_numeric(v_items['SOH'], errors='coerce').fillna(0)
        v_items['ADS'] = pd.to_numeric(v_items['ADS'], errors='coerce').fillna(0.1)
        v_items['Unit_Cost'] = pd.to_numeric(v_items['Unit_Cost'], errors='coerce').fillna(1.0)
        v_items['Cover_Days'] = v_items['SOH'] / v_items['ADS'].replace(0, 0.1)
        
        # Sort ignoring NAs or complex objects
        v_items['Cover_Days'] = pd.to_numeric(v_items['Cover_Days'], errors='coerce').fillna(999)
        v_items = v_items.sort_values('Cover_Days')
        
        for _, item in v_items.iterrows():
            cost = max(item['Unit_Cost'], 1.0)
            if remaining_budget <= 0:
                break
                
            # Human orders 14 days of cover normally
            target_qty = int(item['ADS'] * 14)
            target_qty = max(target_qty, 10)
            
            cost_of_order = target_qty * cost
            if remaining_budget >= cost_of_order:
                mock_po_lines.append({
                    'Item_Name': item['Item_Name'],
                    'Human_Order_Qty': target_qty,
                    'Vendor': vendor
                })
                remaining_budget -= cost_of_order
            elif remaining_budget >= cost:
                # Buy what we can
                mock_po_lines.append({
                    'Item_Name': item['Item_Name'],
                    'Human_Order_Qty': int(remaining_budget // cost),
                    'Vendor': vendor
                })
                remaining_budget = 0
                
    if mock_po_lines:
        df_mock_po = pd.DataFrame(mock_po_lines)
        df_mock_po.to_csv(os.path.join(base_dir, 'mock_human_po.csv'), index=False)
        print(f"Saved mock_human_po.csv with {len(df_mock_po)} derived line items.")
    else:
        print("Could not match any PO vendors to Stock vendors!")
else:
    print("Could not process PO files or stock was empty.")
