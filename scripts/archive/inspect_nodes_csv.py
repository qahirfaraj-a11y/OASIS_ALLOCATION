import pandas as pd
import os

nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
if os.path.exists(nodes_csv):
    df = pd.read_csv(nodes_csv, nrows=5)
    print("Columns of nodes.csv:")
    print(list(df.columns))
    print("\nFirst 3 rows:")
    print(df.to_string())
else:
    print("nodes.csv not found")
