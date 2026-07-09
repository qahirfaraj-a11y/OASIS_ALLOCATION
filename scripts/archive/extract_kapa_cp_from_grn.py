import pandas as pd
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    master_skus = df_kapa['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist()
    
    detail_path = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    df_detail = pd.read_excel(detail_path)
    kapa_detail = df_detail[df_detail['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)].copy()
    kapa_detail['Item_Name_upper'] = kapa_detail['Item Name'].astype(str).str.strip().str.upper()
    
    # Calculate CP for each row
    kapa_detail['calculated_cp'] = kapa_detail['Net Amt'] / kapa_detail['GRN Qty']
    
    # Group by Item Name to get mean CP
    cp_map = kapa_detail.groupby('Item_Name_upper')['calculated_cp'].mean().to_dict()
    
    print("=== Unique Cost Prices extracted from GRN ===")
    found = 0
    for s in master_skus:
        if s in cp_map:
            print(f"  {s} -> CP: {cp_map[s]:.2f}")
            found += 1
    print(f"Found cost price for {found} out of {len(master_skus)} in GRN detail!")

check()
