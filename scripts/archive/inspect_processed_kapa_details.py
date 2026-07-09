import os
import glob
import pandas as pd

def check_all():
    downloads_dir = r"C:\Users\iLink\Downloads"
    files = glob.glob(os.path.join(downloads_dir, "processed_kapa_*.xlsx"))
    files.append(r"C:\Users\iLink\Downloads\kapa.xlsx")
    for f in sorted(files):
        print(f"\n=== File: {f} ===")
        try:
            xls = pd.ExcelFile(f)
            for s in xls.sheet_names:
                df = pd.read_excel(f, sheet_name=s)
                # Print row 1 or column headers
                print(f"Sheet '{s}' shape: {df.shape}")
                for idx, row in df.head(3).iterrows():
                    print(f"  Row {idx}: {list(row)}")
        except Exception as e:
            print("  Error:", e)

check_all()
