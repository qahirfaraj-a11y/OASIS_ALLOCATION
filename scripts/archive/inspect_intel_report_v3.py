import pandas as pd
import os

fp = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx"
if os.path.exists(fp):
    xls = pd.ExcelFile(fp)
    print("Sheets in Supplier_Intelligence_Report_2025_v3.xlsx:", xls.sheet_names)
    for s in xls.sheet_names:
        df = pd.read_excel(fp, sheet_name=s)
        print(f"\n--- Sheet '{s}' columns and shape: {df.shape} ---")
        print(df.columns.tolist())
        # search for Kapa
        k = df[df.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)]
        print(f"KAPA rows in sheet '{s}': {len(k)}")
        if len(k) > 0:
            print(k.head(5).to_string())
else:
    print("Supplier_Intelligence_Report_2025_v3.xlsx not found")
