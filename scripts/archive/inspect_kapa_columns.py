import pandas as pd
import os

def check():
    f = r"C:\Users\iLink\Downloads\kapa.xlsx"
    if not os.path.exists(f):
        print("kapa.xlsx not found")
        return
    xls = pd.ExcelFile(f)
    print("Sheets in kapa.xlsx:", xls.sheet_names)
    for sheet in xls.sheet_names:
        df = pd.read_excel(f, sheet_name=sheet)
        print(f"Sheet '{sheet}' shape: {df.shape}")
        print("Columns:")
        for i, col in enumerate(df.columns):
            print(f"  Col {i}: {col}")
        print("First 5 rows:")
        print(df.iloc[:5].to_string())

check()
