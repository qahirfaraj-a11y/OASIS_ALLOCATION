import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from pitch_data_ingestor_v2 import ForensicOperationsIngestor

def _aggregate_all_grns(oasis_data_dir):
    """Aggregate ALL GRN files from oasis/data for statistically meaningful STI calculations.
    
    Returns a DataFrame with columns:
        Supplier_Name, GRN_Date, PO_No, Item_Name, PO_Qty, GRN_Qty, Cost_Price
    """
    grn_frames = []
    for f in sorted(os.listdir(oasis_data_dir)):
        if f.lower().startswith('grn') and f.endswith('.xlsx'):
            path = os.path.join(oasis_data_dir, f)
            try:
                df = pd.read_excel(path)
                # Standardize columns from raw iRetail format
                frame = pd.DataFrame({
                    'Supplier_Name': df['Vendor Code - Name'].astype(str),
                    'GRN_Date': pd.to_datetime(df['GRN Date'], errors='coerce'),
                    'PO_No': df['PO No'].astype(str),
                    'Item_Name': df['Item Name'].astype(str),
                    'PO_Qty': pd.to_numeric(df['PO Qty'], errors='coerce').fillna(0),
                    'GRN_Qty': pd.to_numeric(df['GRN Qty'], errors='coerce').fillna(0),
                    'Cost_Price': pd.to_numeric(df.get('Cost Price', 0), errors='coerce').fillna(0),
                })
                frame = frame.dropna(subset=['GRN_Date', 'Supplier_Name'])
                grn_frames.append(frame)
                print(f"  [GRN] Loaded {len(frame)} rows from {f}")
            except Exception as e:
                print(f"  [GRN] SKIP {f}: {e}")
    
    if not grn_frames:
        print("  [WARNING] No GRN files found!")
        return pd.DataFrame()
    
    all_grns = pd.concat(grn_frames, ignore_index=True)
    print(f"  [GRN] TOTAL: {len(all_grns)} rows, {all_grns['Supplier_Name'].nunique()} suppliers")
    return all_grns


def _aggregate_all_pos(oasis_data_dir):
    """Aggregate ALL PO files from oasis/data to get actual PO dates for lead time calculation.
    
    Returns a DataFrame with columns:
        PO_No, PO_Date, Supplier_Name
    """
    po_frames = []
    for f in sorted(os.listdir(oasis_data_dir)):
        if f.lower().startswith('po_') and f.endswith('.xlsx'):
            path = os.path.join(oasis_data_dir, f)
            try:
                df = pd.read_excel(path)
                frame = pd.DataFrame({
                    'PO_No': df['PO No'].astype(str),
                    'PO_Date': pd.to_datetime(df['PO Date'], errors='coerce'),
                    'Supplier_Name': df['Vendor Code / Name'].astype(str),
                })
                frame = frame.dropna(subset=['PO_Date', 'PO_No'])
                # Deduplicate PO headers (each PO has one date)
                frame = frame.drop_duplicates(subset=['PO_No'])
                po_frames.append(frame)
                print(f"  [PO]  Loaded {len(frame)} POs from {f}")
            except Exception as e:
                print(f"  [PO]  SKIP {f}: {e}")
    
    if not po_frames:
        return pd.DataFrame()
    
    all_pos = pd.concat(po_frames, ignore_index=True).drop_duplicates(subset=['PO_No'])
    print(f"  [PO]  TOTAL: {len(all_pos)} unique POs")
    return all_pos


def _build_lead_time_grn(all_grns, all_pos, delivery_gaps_path):
    """Cross-reference GRNs with POs to calculate real lead times.
    
    Strategy (Option C):
      1. Match by PO_No to get actual PO_Date → Lead Time = GRN_Date - PO_Date
      2. Where no PO match exists, use supplier_delivery_gaps.json median as fallback
    
    Returns enriched GRN DataFrame with 'Order_Date' and 'Received_Date' columns.
    """
    if all_grns.empty:
        return pd.DataFrame()
    
    # Step 1: Merge GRN with PO on PO_No for actual PO dates
    if not all_pos.empty:
        merged = all_grns.merge(
            all_pos[['PO_No', 'PO_Date']], 
            on='PO_No', how='left'
        )
        matched = merged['PO_Date'].notna().sum()
        total = len(merged)
        print(f"  [LT]  PO Date matched: {matched}/{total} ({matched/max(total,1)*100:.1f}%)")
    else:
        merged = all_grns.copy()
        merged['PO_Date'] = pd.NaT
    
    # Step 2: For unmatched rows, use supplier_delivery_gaps.json median as fallback
    fallback_gaps = {}
    if os.path.exists(delivery_gaps_path):
        try:
            with open(delivery_gaps_path, 'r') as f:
                raw_gaps = json.load(f)
            # Compute median gap per supplier (in days)
            for supplier, gaps in raw_gaps.items():
                if gaps:
                    fallback_gaps[supplier.upper()] = int(np.median(gaps))
            print(f"  [LT]  Loaded fallback delivery gaps for {len(fallback_gaps)} suppliers")
        except Exception as e:
            print(f"  [LT]  Fallback gaps load failed: {e}")
    
    # Apply fallback: PO_Date = GRN_Date - median_gap_days
    def _estimate_po_date(row):
        if pd.notna(row['PO_Date']):
            return row['PO_Date']
        # Try to find supplier in fallback gaps
        supplier_key = str(row['Supplier_Name']).split(' - ')[-1].strip().upper() if ' - ' in str(row['Supplier_Name']) else str(row['Supplier_Name']).upper()
        median_gap = fallback_gaps.get(supplier_key, 7)  # Ultimate fallback: 7 days
        return row['GRN_Date'] - pd.Timedelta(days=median_gap)
    
    merged['Order_Date'] = merged.apply(_estimate_po_date, axis=1)
    merged['Received_Date'] = merged['GRN_Date']
    
    # Final stats
    actual_lt = merged['PO_Date'].notna().sum()
    estimated_lt = merged['PO_Date'].isna().sum()
    print(f"  [LT]  Final: {actual_lt} actual PO dates, {estimated_lt} estimated from delivery gaps")
    
    return merged


def build_rhapta_cache():
    print("=" * 60)
    print("  Building Rhapta Pre-Loaded Demo Cache (Production Grade)")
    print("=" * 60)
    base_dir = os.path.dirname(os.path.abspath(__file__))
    ingestor = ForensicOperationsIngestor(base_dir)
    
    oasis_data_dir = os.path.join(base_dir, 'oasis', 'data')
    
    # 1. Base Inventory (Scorecard)
    print("\n[Phase 1] Loading Base Inventory...")
    try:
        scorecard_path = os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v3.csv")
        ingestor.inventory_df = pd.read_csv(scorecard_path)
        ingestor.inventory_df = ingestor.inventory_df.rename(columns={
            'Product': 'Item_Name',
            'Unit_Price': 'Unit_Cost_KES'
        })
        ingestor.inventory_df['Stock_On_Hand'] = np.random.randint(0, 50, size=len(ingestor.inventory_df))
        ingestor.inventory_df['Barcode'] = ingestor.inventory_df['Item_Name'].astype(str)
        print(f"  Rhapta Scorecard loaded: {len(ingestor.inventory_df)} items.")
    except Exception as e:
        print(f"  Rhapta Scorecard missing: {e}")
        ingestor.inventory_df = pd.DataFrame()

    # 2. POS Logs (jan_cash.xlsx)
    print("\n[Phase 2] Loading POS Logs...")
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
        
        print(f"  Rhapta POS loaded: {len(ingestor.pos_df)} transactions.")
    except Exception as e:
        print(f"  Rhapta jan_cash failed: {e}")
        ingestor.pos_df = pd.DataFrame()

    # 3. GRN — Aggregate ALL files with real PO cross-reference
    print("\n[Phase 3] Aggregating ALL GRN + PO data for STI integrity...")
    all_grns = _aggregate_all_grns(oasis_data_dir)
    all_pos = _aggregate_all_pos(oasis_data_dir)
    delivery_gaps_path = os.path.join(oasis_data_dir, 'supplier_delivery_gaps.json')
    
    enriched_grn = _build_lead_time_grn(all_grns, all_pos, delivery_gaps_path)
    
    if not enriched_grn.empty:
        ingestor.grn_df = pd.DataFrame({
            'Order_Date': enriched_grn['Order_Date'],
            'Received_Date': enriched_grn['Received_Date'],
            'PO_Number': enriched_grn['PO_No'],
            'Supplier_Name': enriched_grn['Supplier_Name'],
            'Item_Name': enriched_grn['Item_Name'],
            'Ordered_Qty': enriched_grn['PO_Qty'],
            'Received_Qty': enriched_grn['GRN_Qty'],
        })
        ingestor.grn_df = ingestor.grn_df.dropna(subset=['Received_Date', 'Order_Date'])
        print(f"  Final GRN DataFrame: {len(ingestor.grn_df)} rows, {ingestor.grn_df['Supplier_Name'].nunique()} suppliers.")
    else:
        print("  [WARNING] No GRN data available!")
        ingestor.grn_df = pd.DataFrame()

    # 4. Transfers (Real inter-branch movements)
    print("\n[Phase 4] Loading Transfer Logs...")
    try:
        raw_trn = pd.read_excel(os.path.join(oasis_data_dir, "trn_1_12.xlsx"))
        ingestor.transfer_df = pd.DataFrame({
            'Date': pd.to_datetime(raw_trn['STI Date'], errors='coerce'),
            'From_Branch': raw_trn['From Org Code/ Name'].astype(str),
            'To_Branch': raw_trn['To Org Code/ Name'].astype(str),
            'Item_Name': raw_trn['Item Name'].astype(str),
            'Qty_Transferred': pd.to_numeric(raw_trn['STI Qty'], errors='coerce').fillna(0),
            'Cost_Value': pd.to_numeric(raw_trn['Net Amt'], errors='coerce').fillna(0),
        })
        ingestor.transfer_df = ingestor.transfer_df.dropna(subset=['Date'])
        print(f"  Rhapta Transfers loaded: {len(ingestor.transfer_df)} movements.")
    except Exception as e:
        print(f"  Rhapta Transfers failed: {e}")
        ingestor.transfer_df = pd.DataFrame()

    # 5. Returns / Shrinkage (Real PRTS data)
    print("\n[Phase 5] Loading Returns/Shrinkage...")
    try:
        prts_frames = []
        for f in os.listdir(oasis_data_dir):
            if f.lower().startswith('prts_') and f.endswith('.xlsx'):
                df = pd.read_excel(os.path.join(oasis_data_dir, f))
                prts_frames.append(df)
        if prts_frames:
            raw_prts = pd.concat(prts_frames, ignore_index=True)
            ingestor.shrink_df = pd.DataFrame({
                'Date': pd.to_datetime(raw_prts['Doc Date'], errors='coerce'),
                'Item_Name': raw_prts['Item Name'].astype(str),
                'Supplier': raw_prts['Ven Code / Name'].astype(str),
                'Qty_Adjusted': pd.to_numeric(raw_prts['Rejc Qty'], errors='coerce').fillna(0),
                'Reason': raw_prts.get('Reason', 'Unknown').astype(str),
                'Cost_Value': pd.to_numeric(raw_prts['Net Amt'], errors='coerce').fillna(0),
            })
            ingestor.shrink_df = ingestor.shrink_df.dropna(subset=['Date'])
            print(f"  Rhapta Returns loaded: {len(ingestor.shrink_df)} events.")
        else:
            print("  No PRTS files found.")
            ingestor.shrink_df = pd.DataFrame()
    except Exception as e:
        print(f"  Rhapta PRTS failed: {e}")
        ingestor.shrink_df = pd.DataFrame()

    # Fix barcode merge issue before running analysis
    if not ingestor.pos_df.empty:
        ingestor.pos_df['Barcode'] = ingestor.pos_df['Barcode'].astype(str)
        mock_soh = pd.DataFrame({
            'Barcode': ingestor.pos_df['Barcode'].unique(),
            'Stock_On_Hand': np.random.randint(0, 45, size=len(ingestor.pos_df['Barcode'].unique()))
        })
        mock_soh.to_csv(os.path.join(base_dir, 'prospect_inventory_snapshot.csv'), index=False)
         
    # Run audit — FIXED EXECUTION ORDER: Supplier FIRST, then POS (so DHARAM gets real lead times)
    print("\n[Phase 6] Running Forensic Analysis...")
    print("  [1/4] Supplier Toxicity (LATA)...")
    ingestor.run_supplier_analysis()
    print("  [2/4] POS Velocity (AMIT/DHARAM)...")
    ingestor.run_pos_analysis()
    print("  [3/4] Network Entropy (MANDE)...")
    ingestor.run_network_analysis()
    print("  [4/4] Cycle Intelligence...")
    ingestor.run_cycle_analysis()
    
    # Build audit dictionary
    audit = ingestor.get_full_audit()
    audit['generated_at'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # --- JSON Serialization Safety ---
    # DataFrames are not JSON serializable; convert to records for evidence preservation
    for key in ['full_catalog_df', 'shrink_df', 'transfer_df', 'pos_raw_df', 'grn_raw_df']:
        val = audit.get(key)
        if val is not None and hasattr(val, 'to_dict'):
            try:
                # Convert datetime columns to strings for JSON
                df_copy = val.copy()
                for col in df_copy.select_dtypes(include=['datetime64[ns]', 'datetime64']).columns:
                    df_copy[col] = df_copy[col].dt.strftime('%Y-%m-%d').fillna('')
                audit[key] = df_copy.to_dict(orient='records')
            except Exception as e:
                print(f"  [WARNING] Could not serialize {key}: {e}")
                audit[key] = None
    
    # Dump to JSON
    cache_path = os.path.join(base_dir, "rhapta_demo_preloaded.json")
    with open(cache_path, 'w', encoding='utf-8') as f:
        json.dump(audit, f, default=str)
    
    # --- Post-Build Verification ---
    print("\n" + "=" * 60)
    print("  POST-BUILD VERIFICATION")
    print("=" * 60)
    
    sup = audit.get('suppliers', {})
    supplier_list = sup.get('supplier_list', [])
    if supplier_list:
        sti_scores = [s['sti_score'] for s in supplier_list]
        lead_vars = [s['lead_variance'] for s in supplier_list]
        print(f"  Suppliers scanned:     {len(supplier_list)}")
        print(f"  STI range:             {min(sti_scores):.3f} - {max(sti_scores):.3f}")
        print(f"  STI mean:              {np.mean(sti_scores):.3f}")
        print(f"  Lead Variance range:   {min(lead_vars):.1f} - {max(lead_vars):.1f} days")
        print(f"  Toxic/At-Risk:         {sup.get('criminal_count', 0)}")
    
    cat = audit.get('catalog', {})
    print(f"  SKUs scanned:          {cat.get('total_skus_scanned', 0)}")
    print(f"  Ghost Demand Threshold: {cat.get('ghost_demand_threshold', 'N/A')} ADS")
    print(f"  Ghost Demand Items:    {cat.get('ghost_demand_count', 0)}")
    print(f"  Dead Stock Items:      {cat.get('dead_stock_count', 0)}")
    
    cyc = audit.get('cycle', {})
    print(f"  Cycle Data:            {'Present' if cyc and cyc.get('demand_wave') else 'MISSING'}")
    
    print(f"\n  Cache saved to: {cache_path}")
    print(f"  File size: {os.path.getsize(cache_path) / 1024 / 1024:.1f} MB")

if __name__ == "__main__":
    build_rhapta_cache()
