import pandas as pd

nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
df = pd.read_csv(nodes_csv)
print("Total rows in nodes.csv:", len(df))
print("Distinct suppliers in nodes.csv:")
print(df['supplier'].unique()[:20] if 'supplier' in df.columns else df.columns)

kapa_nodes = df[df['supplier'].astype(str).str.contains('KAPA', case=False, na=False)]
print(f"\nKAPA nodes in nodes.csv: {len(kapa_nodes)}")
print("Sample KAPA nodes:")
print(kapa_nodes[['id', 'price', 'margin_pct', 'velocity_ads', 'revenue', 'gross_profit']].head(20).to_string())
