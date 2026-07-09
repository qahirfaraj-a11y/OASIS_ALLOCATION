import pandas as pd
import os

def check():
    f = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    if not os.path.exists(f):
        print("All_Suppliers_Fulfillment_Detail.xlsx not found")
        return
    df = pd.read_excel(f, nrows=5)
    print("Columns:", list(df.columns))
    print("Head:")
    print(df.to_string())
    
    # Check vendor names
    df_full = pd.read_excel(f)
    print("Vendor names containing KAPA:")
    print(df_full[df_full['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)]['Vendor Name'].unique())

check()
