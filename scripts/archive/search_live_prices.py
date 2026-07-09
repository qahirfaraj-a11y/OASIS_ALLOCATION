import os
import pandas as pd

def search_live():
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
            matches = df[df['ITM_NAME'].astype(str).str.contains('SOLIO|CLEANROL', case=False, na=False)]
            if not matches.empty:
                print(f"\nMatches in {f}:")
                print(matches[['VENDOR_NAME', 'BARCODE', 'ITM_NAME', 'SellPrice', 'STOCK']].to_string())

search_live()
