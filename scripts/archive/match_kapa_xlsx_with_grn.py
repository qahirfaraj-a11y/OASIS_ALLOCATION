import pandas as pd
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    master_skus = df_kapa['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist()
    
    detail_path = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    if os.path.exists(detail_path):
        df_detail = pd.read_excel(detail_path)
        # Filter for KAPA
        kapa_detail = df_detail[df_detail['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)]
        print(f"Total Kapa GRN rows in All_Suppliers_Fulfillment_Detail: {len(kapa_detail)}")
        kapa_detail['Item_Name_upper'] = kapa_detail['Item Name'].astype(str).str.strip().str.upper()
        
        matches = []
        for s in master_skus:
            m = kapa_detail[kapa_detail['Item_Name_upper'] == s]
            if m.empty:
                m = kapa_detail[kapa_detail['Item_Name_upper'].str.contains(s, regex=False)]
            if not m.empty:
                matches.append((s, m.iloc[0]['Item Name'], len(m)))
                
        print(f"Matched {len(matches)} out of {len(master_skus)} in Fulfillment Detail!")
        print("Matches:")
        for cat_name, grn_name, count in matches[:20]:
            print(f"  {cat_name} -> {grn_name} ({count} GRN rows)")

check()
