import os
import pandas as pd
import numpy as np

def dry_run():
    data_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
    
    grn_files = [
        "grnds_2_2.5.xlsx",
        "grnds_2_3.0.xlsx",
        "grnds_3.5_4.xlsx",
        "grnds_3_3.5.xlsx",
        "grnds_7.5_8.xlsx",
        "grnds_8.5_9.xlsx",
        "grnds_8_8.5.xlsx",
        "grnds_9.5_10.xlsx",
        "grnds_9_9.5.xlsx",
        "grnds_10.5_11.xlsx",
        "grnds_10_10.5.xlsx",
        "grnds_11.5_12.xlsx",
        "grnds_11_11.5.xlsx",
        "grnds_12.xlsx",
        "grnd_1_1.5.xlsx",
        "grnds_1_1.5.xlsx",
        "grnds_1_2.0.xlsx"
    ]
    
    dept_files = [
        "dept_101_150.xlsx",
        "dept_151_200.xlsx",
        "dept_201_250.xlsx",
        "dept_301_350.xlsx",
        "dept_1_50.xlsx",
        "dept_51_100.xlsx"
    ]
    
    # 1. Load GRN cost data
    grn_rows = []
    print("Loading GRN files...")
    for f in grn_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            df = pd.read_excel(fp)
            kapa = df[df['Vendor Code - Name'].astype(str).str.contains('KAPA', case=False, na=False)].copy()
            if not kapa.empty:
                grn_rows.append(kapa)
                
    if grn_rows:
        df_grn_kapa = pd.concat(grn_rows, ignore_index=True)
        # Clean barcode and calculate CP
        df_grn_kapa['barcode_clean'] = df_grn_kapa['Bar Code'].astype(str).str.split('.').str[0].str.strip()
        df_grn_kapa['item_upper'] = df_grn_kapa['Item Name'].astype(str).str.strip().str.upper()
        df_grn_kapa['unit_cp'] = df_grn_kapa['Net Amt'] / df_grn_kapa['GRN Qty']
        
        # Filter out invalid unit_cp
        df_grn_kapa = df_grn_kapa[df_grn_kapa['unit_cp'] > 0]
        
        # Build maps
        barcode_cp_map = df_grn_kapa.groupby('barcode_clean')['unit_cp'].mean().to_dict()
        name_cp_map = df_grn_kapa.groupby('item_upper')['unit_cp'].mean().to_dict()
        
        print(f"Total Kapa GRN rows: {len(df_grn_kapa)}")
        print(f"Unique barcodes in GRN: {len(barcode_cp_map)}")
        print(f"Unique names in GRN: {len(name_cp_map)}")
    else:
        barcode_cp_map = {}
        name_cp_map = {}
        print("No Kapa GRN rows found")
        
    # 2. Load Department sell price & stock data
    dept_rows = []
    print("Loading Department files...")
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            df = pd.read_excel(fp)
            kapa = df[df['VENDOR_NAME'].astype(str).str.contains('KAPA', case=False, na=False)].copy()
            if not kapa.empty:
                dept_rows.append(kapa)
                
    if dept_rows:
        df_dept_kapa = pd.concat(dept_rows, ignore_index=True)
        df_dept_kapa['barcode_clean'] = df_dept_kapa['BARCODE'].astype(str).str.split('.').str[0].str.strip()
        df_dept_kapa['item_upper'] = df_dept_kapa['ITM_NAME'].astype(str).str.strip().str.upper()
        
        # Build maps
        barcode_sp_map = df_dept_kapa.set_index('barcode_clean')['SellPrice'].to_dict()
        barcode_stock_map = df_dept_kapa.set_index('barcode_clean')['STOCK'].to_dict()
        name_sp_map = df_dept_kapa.set_index('item_upper')['SellPrice'].to_dict()
        name_stock_map = df_dept_kapa.set_index('item_upper')['STOCK'].to_dict()
        
        print(f"Total Kapa Dept rows: {len(df_dept_kapa)}")
        print(f"Unique barcodes in Dept: {len(barcode_sp_map)}")
        print(f"Unique names in Dept: {len(name_sp_map)}")
    else:
        barcode_sp_map = {}
        barcode_stock_map = {}
        name_sp_map = {}
        name_stock_map = {}
        print("No Kapa Dept rows found")
        
    # 3. Match catalog items
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa_data = pd.read_excel(kapa_excel_path, header=2)
    
    matches = []
    for idx, row in df_kapa_data.iterrows():
        desc = row['DESCRIPTION']
        if pd.isna(desc):
            continue
        desc_upper = str(desc).strip().upper()
        barcode = str(row['BARCODE']).split('.')[0].strip()
        
        # Get SP
        sp = barcode_sp_map.get(barcode, name_sp_map.get(desc_upper, None))
        if sp is None:
            sp = row['SP']
            if isinstance(sp, str):
                sp = float(sp.replace(',', ''))
            else:
                sp = float(sp)
            is_new_sp = False
        else:
            is_new_sp = True
            
        # Get CP
        cp = barcode_cp_map.get(barcode, name_cp_map.get(desc_upper, None))
        if cp is None:
            is_new_cp = False
        else:
            is_new_cp = True
            
        matches.append({
            'name': desc_upper,
            'barcode': barcode,
            'sp': sp,
            'cp': cp,
            'is_new_sp': is_new_sp,
            'is_new_cp': is_new_cp
        })
        
    df_matches = pd.DataFrame(matches)
    print("\n=== Matching Summary for 124 Catalog SKUs ===")
    print(f"Total mapped: {len(df_matches)}")
    print(f"SKUs with updated SP: {len(df_matches[df_matches['is_new_sp'] == True])}")
    print(f"SKUs with updated CP: {len(df_matches[df_matches['is_new_cp'] == True])}")
    print("\nSample matches:")
    print(df_matches.head(10).to_string())

dry_run()
