import pandas as pd
import os

path = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data\topselqty.xlsx'
if os.path.exists(path):
    print(f"--- Headers for topselqty.xlsx ---")
    df = pd.read_excel(path, nrows=5)
    print(df.columns.tolist())
    print("Sample data:")
    print(df.head(2))
else:
    print(f"File not found: {path}")
