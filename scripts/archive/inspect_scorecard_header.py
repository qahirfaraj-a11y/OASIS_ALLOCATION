import pandas as pd

df = pd.read_csv(r"C:\Users\iLink\.gemini\antigravity\scratch\active_sku_scorecards.csv", nrows=3)
print("Columns parsed:", df.columns.tolist())
print("First row:", df.iloc[0].to_dict())
