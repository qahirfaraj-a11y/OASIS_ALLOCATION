import pandas as pd
import os

files = [
    r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trn_1_12.xlsx",
    r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trout_1_12.xlsx"
]

for f in files:
    print(f"\n--- Exploring {os.path.basename(f)} ---")
    try:
        df = pd.read_excel(f, nrows=5)
        print("Columns:", df.columns.tolist())
        print("Sample Data:")
        print(df.head(2))
    except Exception as e:
        print(f"Error reading {f}: {e}")
