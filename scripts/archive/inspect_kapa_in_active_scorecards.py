import pandas as pd
import os

def check():
    active_path = r"C:\Users\iLink\.gemini\antigravity\scratch\active_sku_scorecards.csv"
    if os.path.exists(active_path):
        df = pd.read_csv(active_path, nrows=5)
        print("active_sku_scorecards.csv columns:", list(df.columns))
        df_full = pd.read_csv(active_path)
        kapa_rows = df_full[df_full['supplier'].astype(str).str.contains('KAPA', case=False, na=False)]
        print(f"KAPA rows in active_sku_scorecards: {len(kapa_rows)}")
        if len(kapa_rows) > 0:
            print("Unique suppliers in active_sku_scorecards KAPA:")
            print(kapa_rows['supplier'].unique())
            print("Sample KAPA rows in active_sku_scorecards:")
            print(kapa_rows.head(5).to_string())
            
    # Check unique_moats_portfolio.xlsx
    moat_path = r"C:\Users\iLink\.gemini\antigravity\scratch\unique_moats_portfolio.xlsx"
    if os.path.exists(moat_path):
        df_moat = pd.read_excel(moat_path)
        kapa_moat = df_moat[df_moat['supplier'].astype(str).str.contains('KAPA', case=False, na=False)]
        print(f"\nKAPA rows in unique_moats_portfolio: {len(kapa_moat)}")
        if len(kapa_moat) > 0:
            print("Sample KAPA rows in unique_moats_portfolio:")
            print(kapa_moat.head(5).to_string())

check()
