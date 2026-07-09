import os
import pandas as pd

def check_cols():
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
    
    for f in grn_files:
        fp = os.path.join(data_dir, f)
        if os.path.exists(fp):
            try:
                df = pd.read_excel(fp, nrows=2)
                print(f"File: {f} | Columns: {list(df.columns)}")
            except Exception as e:
                print(f"Error in {f}: {e}")

check_cols()
