import os
import pandas as pd

def check_schemas():
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
    
    print("=== GRN Files Check ===")
    for f in grn_files:
        fp = os.path.join(data_dir, f)
        print(f"  {f}: {'Exists' if os.path.exists(fp) else 'Missing'}")
        
    print("\n=== Department Files Check ===")
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        print(f"  {f}: {'Exists' if os.path.exists(fp) else 'Missing'}")
        
    # Read one GRN file
    for f in grn_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            print(f"\n--- Schema of {f} ---")
            df = pd.read_excel(fp, nrows=3)
            print("Columns:", list(df.columns))
            print("First row:\n", df.head(1).to_string())
            break
            
    # Read one Dept file
    for f in dept_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            print(f"\n--- Schema of {f} ---")
            df = pd.read_excel(fp, nrows=3)
            print("Columns:", list(df.columns))
            print("First row:\n", df.head(1).to_string())
            break

check_schemas()
