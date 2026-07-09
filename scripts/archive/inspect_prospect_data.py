import pandas as pd
import os

def check():
    grn_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_inbound_grn.csv"
    pos_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_pos_sales.csv"
    
    if os.path.exists(grn_path):
        df_grn = pd.read_csv(grn_path, nrows=5)
        print("prospect_inbound_grn.csv columns and head:")
        print(df_grn.to_string())
        
        # Check unique supplier names in GRN
        df_grn_full = pd.read_csv(grn_path)
        print("\nDistinct suppliers in prospect_inbound_grn.csv:")
        print(df_grn_full['supplier_name'].unique()[:10] if 'supplier_name' in df_grn_full.columns else df_grn_full.columns)
    else:
        print("prospect_inbound_grn.csv not found")
        
    if os.path.exists(pos_path):
        df_pos = pd.read_csv(pos_path, nrows=5)
        print("\nprospect_pos_sales.csv columns and head:")
        print(df_pos.to_string())
    else:
        print("prospect_pos_sales.csv not found")

check()
