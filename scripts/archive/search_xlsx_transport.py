import os
import glob
import pandas as pd

def search():
    for f in glob.glob(r"C:\Users\iLink\.gemini\antigravity\scratch\*.xlsx"):
        if "~$" in f:
            continue
        try:
            xls = pd.ExcelFile(f)
            for sheet in xls.sheet_names:
                df = pd.read_excel(f, sheet_name=sheet, nrows=5)
                cols_str = " ".join([str(c).upper() for c in df.columns])
                if any(x in cols_str for x in ["TRANSPORT", "LOGISTICS", "FREIGHT", "DELIVERY"]):
                    print(f"Found column in file: {os.path.basename(f)} sheet: {sheet}")
                    print(df.columns.tolist())
        except Exception as e:
            # print(f"Error {f}: {e}")
            pass

search()
