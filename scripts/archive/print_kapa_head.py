import pandas as pd

kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
xls = pd.ExcelFile(kapa_excel_path)
print("Sheet names:", xls.sheet_names)
for sheet in xls.sheet_names:
    df = pd.read_excel(kapa_excel_path, sheet_name=sheet)
    print(f"\n--- Sheet '{sheet}' first 10 rows ---")
    print(df.head(10).to_string())
