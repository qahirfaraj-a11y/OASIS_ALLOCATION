import pandas as pd
import os

fp = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\grnd_1_1.5.xlsx"
if os.path.exists(fp):
    df = pd.read_excel(fp, header=None, nrows=10)
    print("First 10 rows of grnd_1_1.5.xlsx:")
    for idx, row in df.iterrows():
        print(f"Row {idx}: {list(row)}")
else:
    print("grnd_1_1.5.xlsx not found")
