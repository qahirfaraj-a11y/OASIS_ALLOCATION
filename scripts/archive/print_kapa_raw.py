import pandas as pd

kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
df = pd.read_excel(kapa_excel_path, header=None)
print("=== First 5 rows of kapa.xlsx (header=None) ===")
for idx, row in df.head(5).iterrows():
    print(f"Row {idx}: {list(row)}")
