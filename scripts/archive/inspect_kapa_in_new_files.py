import os
import pandas as pd

def check_kapa():
    data_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
    
    grn_files = [
        "grnds_2_2.5.xlsx",
        "grnds_2_3.0.xlsx",
        "grnds_3.5_4.xlsx",
        "grnds_3_3.5.xlsx",
        "grnds_7.5_8.xlsx",
        "grnds_8.5_9.xlsx",
        "grnds_8_8.5.xlsx",
        "grnds_9.5_10.xlsx",
        "grnds_9_9.5.xlsx",
        "grnds_10.5_11.xlsx",
        "grnds_10_10.5.xlsx",
        "grnds_11.5_12.xlsx",
        "grnds_11_11.5.xlsx",
        "grnds_12.xlsx",
        "grnd_1_1.5.xlsx",
        "grnds_1_1.5.xlsx",
        "grnds_1_2.0.xlsx"
    ]
    
    dept_files = [
        "dept_101_150.xlsx",
        "dept_151_200.xlsx",
        "dept_201_250.xlsx",
        "dept_301_350.xlsx",
        "dept_1_50.xlsx",
        "dept_51_100.xlsx"
    ]
    
    total_grn_kapa = 0
    total_dept_kapa = 0
    
    print("=== Searching for KAPA in GRN Files ===")
    for f in grn_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            try:
                df = pd.read_excel(fp)
                # Check for Kapa
                kapa = df[df['Vendor Code - Name'].astype(str).str.contains('KAPA', case=False, na=False)]
                if len(kapa) > 0:
                    print(f"  {f}: found {len(kapa)} Kapa rows")
                    total_grn_kapa += len(kapa)
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                
    print(f"Total Kapa GRN rows found: {total_grn_kapa}")
    
    print("\n=== Searching for KAPA in Department Files ===")
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            try:
                df = pd.read_excel(fp)
                kapa = df[df['VENDOR_NAME'].astype(str).str.contains('KAPA', case=False, na=False)]
                if len(kapa) > 0:
                    print(f"  {f}: found {len(kapa)} Kapa rows")
                    total_dept_kapa += len(kapa)
                    # Print first 2 rows
                    print(kapa.head(2).to_string())
            except Exception as e:
                print(f"  Error reading {f}: {e}")
                
    print(f"Total Kapa Department rows found: {total_dept_kapa}")

check_kapa()
