import pandas as pd
import os

def check():
    nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
    df_nodes = pd.read_csv(nodes_csv)
    print("nodes.csv columns:", list(df_nodes.columns))
    print("nodes.csv shape:", df_nodes.shape)
    
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    master_skus = df_kapa['DESCRIPTION'].dropna().astype(str).str.strip().str.upper().tolist()
    
    matches = []
    for s in master_skus:
        m = df_nodes[df_nodes['id'].str.upper() == s]
        if m.empty:
            m = df_nodes[df_nodes['id'].str.upper().str.contains(s, regex=False)]
        if not m.empty:
            matches.append((s, m.iloc[0]['id'], m.iloc[0].to_dict()))
            
    print(f"Matched {len(matches)} out of {len(master_skus)} catalog items in nodes.csv")
    print("Sample matched node data:")
    for idx, (cat_name, net_name, node_data) in enumerate(matches[:5]):
        print(f"\nCatalog: {cat_name}")
        print(f"  Network: {net_name}")
        print(f"  Data: {node_data}")

check()
