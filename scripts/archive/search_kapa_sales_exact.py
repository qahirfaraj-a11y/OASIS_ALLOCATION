import pandas as pd
import os

def check():
    pos_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_pos_sales.csv"
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    master_skus = df_kapa['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist()
    
    if os.path.exists(pos_path):
        df_pos = pd.read_csv(pos_path)
        df_pos['Item_Name_upper'] = df_pos['Item_Name'].astype(str).str.strip().str.upper()
        
        matches = []
        for s in master_skus:
            # check exact or substring
            m = df_pos[df_pos['Item_Name_upper'] == s]
            if m.empty:
                m = df_pos[df_pos['Item_Name_upper'].str.contains(s, regex=False)]
            if not m.empty:
                matches.append((s, m.iloc[0]['Item_Name'], len(m)))
                
        print(f"Matched {len(matches)} out of {len(master_skus)} in POS sales!")
        print("Matches:")
        for cat_name, pos_name, count in matches:
            print(f"  {cat_name} -> {pos_name} ({count} sales)")

check()
