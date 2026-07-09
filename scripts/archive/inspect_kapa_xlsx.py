import pandas as pd

kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
df_kapa = pd.read_excel(kapa_excel_path, header=2)
print("=== Columns of kapa.xlsx ===")
print(df_kapa.columns.tolist())
print("\n=== First 15 rows of kapa.xlsx ===")
print(df_kapa[['DESCRIPTION', 'BARCODE', 'ITEM CODE', 'SP', 'Rhapta']].head(15).to_string())
