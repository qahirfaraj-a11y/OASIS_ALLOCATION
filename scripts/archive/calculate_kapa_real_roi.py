import pandas as pd
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    master_skus = df_kapa['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist()
    
    scorecard_path = r"C:\Users\iLink\.gemini\antigravity\scratch\active_sku_scorecards.csv"
    df_scorecard = pd.read_csv(scorecard_path)
    df_scorecard['id_upper'] = df_scorecard['id'].astype(str).str.strip().str.upper()
    
    print("=== Matching catalog items in active_sku_scorecards.csv ===")
    matches = []
    for s in master_skus:
        m = df_scorecard[df_scorecard['id_upper'] == s]
        if m.empty:
            m = df_scorecard[df_scorecard['id_upper'].str.contains(s, regex=False)]
        if not m.empty:
            row = m.iloc[0]
            matches.append({
                'catalog_name': s,
                'scorecard_name': row['id'],
                'price': row['price'],
                'margin_pct': row['margin_pct'],
                'revenue': row['revenue'],
                'gross_profit': row['gross_profit'],
                'velocity_ads': row['velocity_ads'],
                'total_quantity': row['total_quantity']
            })
            
    print(f"Total matched: {len(matches)} / {len(master_skus)}")
    df_matches = pd.DataFrame(matches)
    print(df_matches.head(10).to_string())

check()
