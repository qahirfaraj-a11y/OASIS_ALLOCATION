import os
import pandas as pd

def check_barcode():
    data_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
    
    dept_files = [
        "dept_101_150.xlsx",
        "dept_151_200.xlsx",
        "dept_201_250.xlsx",
        "dept_301_350.xlsx",
        "dept_1_50.xlsx",
        "dept_51_100.xlsx"
    ]
    
    barcodes = ['6161101660891', '6161101660624', '6161101660648']
    
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            df = pd.read_excel(fp)
            df['BARCODE_str'] = df['BARCODE'].astype(str).str.split('.').str[0].str.strip()
            matches = df[df['BARCODE_str'].isin(barcodes)]
            if not matches.empty:
                print(f"\nMatches in {f}:")
                print(matches[['VENDOR_NAME', 'BARCODE', 'ITM_NAME', 'SellPrice', 'STOCK']].to_string())

check_barcode()
