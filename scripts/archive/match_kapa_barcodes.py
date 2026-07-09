import pandas as pd
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    # clean barcodes
    df_kapa = df_kapa.dropna(subset=['BARCODE'])
    kapa_barcodes = set(df_kapa['BARCODE'].astype(str).str.split('.').str[0].str.strip())
    print(f"Total unique barcodes in kapa.xlsx: {len(kapa_barcodes)}")
    
    pos_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_pos_sales.csv"
    if os.path.exists(pos_path):
        df_pos = pd.read_csv(pos_path)
        # convert POS barcodes to string
        df_pos['Barcode_str'] = df_pos['Barcode'].astype(str).str.split('.').str[0].str.strip()
        matches = df_pos[df_pos['Barcode_str'].isin(kapa_barcodes)]
        print(f"POS Matches by Barcode: {len(matches)}")
        print("Unique matched items in POS:")
        print(matches[['Barcode', 'Item_Name']].drop_duplicates().head(20))
        
    grn_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_inbound_grn.csv"
    if os.path.exists(grn_path):
        # wait, GRN has no barcode column, it has Item_Name
        df_grn = pd.read_csv(grn_path)
        print("GRN has no barcode. Matches by Item Name in GRN:")
        matched_names = set(matches['Item_Name'].unique()) if os.path.exists(pos_path) else set()
        grn_matches = df_grn[df_grn['Item_Name'].isin(matched_names)]
        print(f"GRN Matches by Item Name: {len(grn_matches)}")
        print("Unique matched items in GRN:")
        print(grn_matches['Item_Name'].unique()[:20])

check()
