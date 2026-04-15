
import pandas as pd
import os

fpath = "oasis/data/mar_cash.xlsx"
df = pd.read_excel(fpath)
print(f"Columns in MAR: {df.columns.tolist()}")

for col in df.columns:
    try:
        s = pd.to_numeric(df[col], errors='coerce').fillna(0)
        print(f"Col {col}: Sum = {s.sum()}")
    except:
        pass
