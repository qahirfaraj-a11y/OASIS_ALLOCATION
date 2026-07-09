import pandas as pd
import os

def check():
    f = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx"
    if not os.path.exists(f):
        print("Supplier_Intelligence_Report_2025_v3.xlsx not found")
        return
    xls = pd.ExcelFile(f)
    print("Sheets in Supplier_Intelligence_Report_2025_v3.xlsx:", xls.sheet_names)
    for sheet in xls.sheet_names:
        df = pd.read_excel(f, sheet_name=sheet)
        print(f"Sheet '{sheet}' columns:", df.columns.tolist()[:10])
        # Search for transport keywords
        for col in df.columns:
            if any(k in str(col).upper() for k in ["TRANSPORT", "LOGISTICS", "FREIGHT", "DELIVERY", "DISTRIBUTION", "FLEET"]):
                print(f"  Found logistics column in '{sheet}': {col}")
        # Search inside rows for KAPA
        kapa_rows = df[df.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)]
        if not kapa_rows.empty:
            print(f"  Found Kapa in '{sheet}': {len(kapa_rows)} rows")
            print(kapa_rows.head(2).to_string())

check()
