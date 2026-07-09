import pandas as pd

df = pd.read_csv(r"C:\Users\iLink\.gemini\antigravity\scratch\active_sku_scorecards.csv")
print("Total rows in active_sku_scorecards:", len(df))

# search for keywords
keywords = ['ATILLA', 'CAPTAIN COOK', 'KASUKU', 'PRESTIGE', 'RINSUN', 'RINA', 'TOSS', 'JAMAA', 'CHAPA MANDASHI']
for kw in keywords:
    matches = df[df['name'].astype(str).str.contains(kw, case=False, na=False)]
    print(f"Keyword '{kw}' has {len(matches)} matches in scorecard. Samples:")
    print(matches['name'].unique()[:5])
