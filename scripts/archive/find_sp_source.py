import os
import pandas as pd

def find_sp():
    data_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
    
    dept_files = [
        "dept_101_150.xlsx",
        "dept_151_200.xlsx",
        "dept_201_250.xlsx",
        "dept_301_350.xlsx",
        "dept_1_50.xlsx",
        "dept_51_100.xlsx"
    ]
    
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            df = pd.read_excel(fp)
            # Find any row where SellPrice is close to 1155.61
            matches = df[df['SellPrice'].astype(float).between(1155, 1156)]
            if not matches.empty:
                print(f"\nMatches in {f}:")
                print(matches[['VENDOR_NAME', 'BARCODE', 'ITM_NAME', 'SellPrice']].to_string())

find_sp()
