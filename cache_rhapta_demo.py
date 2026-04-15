import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pitch_data_ingestor_v2 import ForensicOperationsIngestor

def build_rhapta_cache():
    print("Building Rhapta Pre-Loaded Demo Cache...")
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingestor = ForensicOperationsIngestor(base_dir)
    
    oasis_data_dir = os.path.join(base_dir, 'oasis', 'data')
    
    # 1. Base Inventory (Scorecard)
    try:
        scorecard_path = os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v3.csv")
        ingestor.inventory_df = pd.read_csv(scorecard_path)
        ingestor.inventory_df = ingestor.inventory_df.rename(columns={
            'Product': 'Item_Name',
            'Unit_Price': 'Unit_Cost_KES'
        })
        ingestor.inventory_df['Stock_On_Hand'] = np.random.randint(0, 50, size=len(ingestor.inventory_df))
        ingestor.inventory_df['Barcode'] = ingestor.inventory_df['Item_Name'].astype(str)
        print("Rhapta Scorecard loaded.")
    except Exception as e:
        print(f"Rhapta Scorecard missing: {e}")
        ingestor.inventory_df = pd.DataFrame()

    # 2. POS Logs (jan_cash.xlsx)
    try:
        raw_cash = pd.read_excel(os.path.join(oasis_data_dir, "jan_cash.xlsx"), skiprows=1)
        # Drop nan rows
        raw_cash = raw_cash.dropna(subset=['Item Name'])
        
        pos_data = {
            'Date': [datetime(2026, 1, 1) + pd.Timedelta(days=np.random.randint(0, 31)) for _ in range(len(raw_cash))],
            'Transaction_ID': ["TX-"+str(i) for i in range(len(raw_cash))],
            'Item_Name': raw_cash['Item Name'].astype(str),
            'Qty_Sold': pd.to_numeric(raw_cash['Qty'], errors='coerce').fillna(1),
            'Unit_Price_KES': 150,
            'Unit_Cost_KES': 100,
            'Barcode': raw_cash['Itm Code'].astype(str) if 'Itm Code' in raw_cash else raw_cash['Item Name'].astype(str)
        }
        ingestor.pos_df = pd.DataFrame(pos_data)
        
        if not ingestor.inventory_df.empty:
            price_map = dict(zip(ingestor.inventory_df['Item_Name'].astype(str).str.upper(), ingestor.inventory_df['Unit_Cost_KES']))
            ingestor.pos_df['Unit_Cost_KES'] = ingestor.pos_df['Item_Name'].str.upper().map(price_map).fillna(100)
            ingestor.pos_df['Unit_Price_KES'] = ingestor.pos_df['Unit_Cost_KES'] * 1.3
        
        print("Rhapta POS loaded.")
    except Exception as e:
        print(f"Rhapta jan_cash failed: {e}")
        ingestor.pos_df = pd.DataFrame()

    # 3. GRN (Real supplier data with actual PO vs GRN variance)
    try:
        raw_grn = pd.read_excel(os.path.join(oasis_data_dir, "grnds_10_10.5.xlsx"))
        # Real columns: 'Vendor Code - Name', 'GRN Date', 'PO No', 'Item Name', 'PO Qty', 'GRN Qty', 'Cost Price'
        raw_grn['GRN Date'] = pd.to_datetime(raw_grn['GRN Date'], errors='coerce')
        
        ingestor.grn_df = pd.DataFrame({
            'Order_Date': raw_grn['GRN Date'] - pd.Timedelta(days=7),  # Estimate PO was placed ~7 days before GRN
            'Received_Date': raw_grn['GRN Date'],
            'PO_Number': raw_grn['PO No'].astype(str),
            'Supplier_Name': raw_grn['Vendor Code - Name'].astype(str),
            'Item_Name': raw_grn['Item Name'].astype(str),
            'Ordered_Qty': pd.to_numeric(raw_grn['PO Qty'], errors='coerce').fillna(10),
            'Received_Qty': pd.to_numeric(raw_grn['GRN Qty'], errors='coerce').fillna(10),
        })
        ingestor.grn_df = ingestor.grn_df.dropna(subset=['Received_Date'])
        print(f"Rhapta GRN loaded: {len(ingestor.grn_df)} rows, {ingestor.grn_df['Supplier_Name'].nunique()} suppliers.")
    except Exception as e:
        print(f"Rhapta GRN failed: {e}")
        ingestor.grn_df = pd.DataFrame()

    # 4. Transfers (Real inter-branch movements)
    try:
        raw_trn = pd.read_excel(os.path.join(oasis_data_dir, "trn_1_12.xlsx"))
        # Real columns: 'From Org Code/ Name', 'To Org Code/ Name', 'STI Date', 'Item Name', 'STI Qty', 'Cost Price', 'Net Amt'
        ingestor.transfer_df = pd.DataFrame({
            'Date': pd.to_datetime(raw_trn['STI Date'], errors='coerce'),
            'From_Branch': raw_trn['From Org Code/ Name'].astype(str),
            'To_Branch': raw_trn['To Org Code/ Name'].astype(str),
            'Item_Name': raw_trn['Item Name'].astype(str),
            'Qty_Transferred': pd.to_numeric(raw_trn['STI Qty'], errors='coerce').fillna(0),
            'Cost_Value': pd.to_numeric(raw_trn['Net Amt'], errors='coerce').fillna(0),
        })
        ingestor.transfer_df = ingestor.transfer_df.dropna(subset=['Date'])
        print(f"Rhapta Transfers loaded: {len(ingestor.transfer_df)} movements.")
    except Exception as e:
        print(f"Rhapta Transfers failed: {e}")
        ingestor.transfer_df = pd.DataFrame()

    # 5. Returns / Shrinkage (Real PRTS data)
    try:
        prts_frames = []
        for f in os.listdir(oasis_data_dir):
            if f.lower().startswith('prts_') and f.endswith('.xlsx'):
                df = pd.read_excel(os.path.join(oasis_data_dir, f))
                prts_frames.append(df)
        if prts_frames:
            raw_prts = pd.concat(prts_frames, ignore_index=True)
            # Real columns: 'Ven Code / Name', 'Doc Date', 'Item Name', 'Reason', 'Rejc Qty', 'Net Amt'
            ingestor.shrink_df = pd.DataFrame({
                'Date': pd.to_datetime(raw_prts['Doc Date'], errors='coerce'),
                'Item_Name': raw_prts['Item Name'].astype(str),
                'Supplier': raw_prts['Ven Code / Name'].astype(str),
                'Qty_Adjusted': pd.to_numeric(raw_prts['Rejc Qty'], errors='coerce').fillna(0),
                'Reason': raw_prts.get('Reason', 'Unknown').astype(str),
                'Cost_Value': pd.to_numeric(raw_prts['Net Amt'], errors='coerce').fillna(0),
            })
            ingestor.shrink_df = ingestor.shrink_df.dropna(subset=['Date'])
            print(f"Rhapta Returns loaded: {len(ingestor.shrink_df)} events.")
        else:
            print("No PRTS files found.")
            ingestor.shrink_df = pd.DataFrame()
    except Exception as e:
        print(f"Rhapta PRTS failed: {e}")
        ingestor.shrink_df = pd.DataFrame()

    # Fix barcode merge issue before running analysis
    if not ingestor.pos_df.empty:
        ingestor.pos_df['Barcode'] = ingestor.pos_df['Barcode'].astype(str)
        # Instead of breaking on barcode merge with scorecard, inject random but realistic SOH into POS directly for the demo
        mock_soh = pd.DataFrame({
            'Barcode': ingestor.pos_df['Barcode'].unique(),
            'Stock_On_Hand': np.random.randint(0, 45, size=len(ingestor.pos_df['Barcode'].unique()))
        })
        mock_soh.to_csv(os.path.join(base_dir, 'prospect_inventory_snapshot.csv'), index=False)
         
    # Run audit
    ingestor.run_pos_analysis()
    ingestor.run_supplier_analysis()
    ingestor.run_network_analysis()
    
    # Build audit dictionary
    audit = ingestor.get_full_audit()
    audit['generated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Dump to JSON
    cache_path = os.path.join(base_dir, "rhapta_demo_preloaded.json")
    with open(cache_path, 'w') as f:
        json.dump(audit, f)
    
    print(f"\nSuccessfully cached Rhapta Demo Audit to {cache_path}")

if __name__ == "__main__":
    build_rhapta_cache()
