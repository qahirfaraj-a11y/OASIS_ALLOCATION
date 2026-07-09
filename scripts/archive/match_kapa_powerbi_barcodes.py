import pandas as pd
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    # clean barcodes
    df_kapa = df_kapa.dropna(subset=['BARCODE'])
    df_kapa['barcode_clean'] = df_kapa['BARCODE'].astype(str).str.split('.').str[0].str.strip()
    kapa_barcodes = set(df_kapa['barcode_clean'])
    print(f"Total unique barcodes in kapa.xlsx: {len(kapa_barcodes)}")
    
    pbi_path = r"C:\Users\iLink\.gemini\antigravity\scratch\powerbi_data_inspection.xlsx"
    if os.path.exists(pbi_path):
        df_pbi = pd.read_excel(pbi_path, sheet_name='Item Analysis')
        df_pbi['barcode_clean'] = df_pbi['Barcode'].astype(str).str.split('.').str[0].str.strip()
        matches = df_pbi[df_pbi['barcode_clean'].isin(kapa_barcodes)]
        print(f"PowerBI Matches by Barcode: {len(matches)}")
        print("Unique matched items in PowerBI:")
        print(matches[['Barcode', 'Item_Name', 'Sell_Price', 'Daily_Sales_Velocity', 'Actual_Margin_Pct']].head(20).to_string())
        
        # Let's save a summary of matches to see how many we can get
        df_merged = pd.merge(df_kapa, df_pbi, on='barcode_clean', how='inner')
        print(f"Merged count: {len(df_merged)}")
        
check()
