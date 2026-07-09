import pandas as pd
import os

def check():
    grn_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_inbound_grn.csv"
    pos_path = r"C:\Users\iLink\.gemini\antigravity\scratch\prospect_pos_sales.csv"
    
    if os.path.exists(pos_path):
        print("=== Searching Kapa items in prospect_pos_sales.csv ===")
        # Read in chunks to avoid memory issues or just read all since it's 10MB
        df_pos = pd.read_csv(pos_path)
        kapa_pos = df_pos[df_pos['Item_Name'].astype(str).str.contains('ATILLA|CAPTAIN COOK|KASUKU|RINA|PRESTIGE|TOSS|LANZO|JAMAA|CHAPA MANDASHI|SOFTLEAF|NEPTUNE', case=False, na=False)]
        print(f"Total sales lines matched: {len(kapa_pos)}")
        print("Unique item names matched in POS:")
        print(kapa_pos['Item_Name'].unique()[:20])
        print("Sample sales rows:")
        print(kapa_pos.head(5).to_string())
    
    if os.path.exists(grn_path):
        print("\n=== Searching Kapa items in prospect_inbound_grn.csv ===")
        df_grn = pd.read_csv(grn_path)
        kapa_grn = df_grn[df_grn['Item_Name'].astype(str).str.contains('ATILLA|CAPTAIN COOK|KASUKU|RINA|PRESTIGE|TOSS|LANZO|JAMAA|CHAPA MANDASHI|SOFTLEAF|NEPTUNE', case=False, na=False)]
        print(f"Total GRN lines matched: {len(kapa_grn)}")
        print("Unique item names matched in GRN:")
        print(kapa_grn['Item_Name'].unique()[:20])
        print("Sample GRN rows:")
        print(kapa_grn.head(5).to_string())

check()
