import pandas as pd
import os

nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"

df_nodes = pd.read_csv(nodes_csv)
df_kapa_data = pd.read_excel(kapa_excel_path, header=2)
master_skus = set(df_kapa_data['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist())

# Search by supplier
by_supplier = df_nodes[df_nodes['supplier'].astype(str).str.contains('KAPA', case=False, na=False)]
print(f"Total rows by supplier matches 'KAPA': {len(by_supplier)}")
if len(by_supplier) > 0:
    print("Unique suppliers found:")
    print(by_supplier['supplier'].unique())

# Search by id mapping
df_nodes['id_upper'] = df_nodes['id'].astype(str).str.strip().str.upper()
by_id = df_nodes[df_nodes['id_upper'].isin(master_skus)]
print(f"Total rows by exact ID match: {len(by_id)}")

# Search by substring ID match
by_substring = df_nodes[df_nodes['id_upper'].apply(lambda x: any(m in x for m in master_skus))]
print(f"Total rows by substring ID match: {len(by_substring)}")

# Let's combine the matches
combined = pd.concat([by_supplier, by_id, by_substring]).drop_duplicates(subset=['id'])
print(f"Total unique Kapa nodes in nodes.csv: {len(combined)}")
