import pandas as pd
import os

def check():
    moat_path = r"C:\Users\iLink\.gemini\antigravity\scratch\unique_moats_portfolio.xlsx"
    if os.path.exists(moat_path):
        df = pd.read_excel(moat_path)
        print("Total rows in unique_moats_portfolio:", len(df))
        print("Columns:", list(df.columns))
        
        # search for KAPA
        kapa_rows = df[df['supplier'].astype(str).str.contains('KAPA', case=False, na=False)]
        print(f"KAPA rows in unique_moats_portfolio: {len(kapa_rows)}")
        print("KAPA items in unique_moats_portfolio:")
        print(kapa_rows[['id', 'price', 'margin_pct', 'velocity_ads', 'revenue', 'gross_profit']].head(20).to_string())

check()
