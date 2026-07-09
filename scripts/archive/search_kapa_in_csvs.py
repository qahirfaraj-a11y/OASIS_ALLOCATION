import os
import glob
import pandas as pd

def check_csvs():
    search_dir = r"C:\Users\iLink\.gemini\antigravity\scratch"
    files = glob.glob(os.path.join(search_dir, "*.csv")) + glob.glob(os.path.join(search_dir, "*.xlsx"))
    print(f"Checking {len(files)} files...")
    for f in files:
        if "oasis_logo" in f or "OASIS_Allocation_Engine" in f or "OASIS_Offline" in f:
            continue
        try:
            if f.endswith(".csv"):
                # Read first 10000 rows to see if KAPA is in it
                for chunk in pd.read_csv(f, chunksize=10000, encoding='utf-8', errors='ignore'):
                    mask = chunk.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)
                    if mask.any():
                        print(f"Match in CSV: {os.path.basename(f)} (columns: {list(chunk.columns)})")
                        break
            elif f.endswith(".xlsx"):
                xls = pd.ExcelFile(f)
                for sheet in xls.sheet_names:
                    df = pd.read_excel(f, sheet_name=sheet, nrows=1000)
                    mask = df.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)
                    if mask.any():
                        print(f"Match in XLSX: {os.path.basename(f)} (sheet: {sheet}, columns: {list(df.columns)})")
                        break
        except Exception as e:
            pass

check_csvs()
