import os
import glob
import pandas as pd

def inspect_processed():
    downloads_dir = r"C:\Users\iLink\Downloads"
    pattern = os.path.join(downloads_dir, "processed_kapa_*.xlsx")
    files = glob.glob(pattern)
    print(f"Found {len(files)} processed files:")
    for f in sorted(files):
        mtime = os.path.getmtime(f)
        import datetime
        dt = datetime.datetime.fromtimestamp(mtime)
        print(f"\nFile: {f} (mtime: {dt})")
        try:
            xls = pd.ExcelFile(f)
            print("  Sheets:", xls.sheet_names)
            for s in xls.sheet_names:
                df = pd.read_excel(f, sheet_name=s)
                print(f"    Sheet '{s}' shape: {df.shape}")
                print(f"    Columns: {list(df.columns)}")
                print(df.head(3).to_string())
        except Exception as e:
            print("  Error:", e)

inspect_processed()
