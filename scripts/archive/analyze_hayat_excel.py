import pandas as pd
import os

files = [
    r"C:\Users\iLink\.gemini\antigravity\scratch\hayat_strategic_deep_dive.xlsx",
    r"C:\Users\iLink\.gemini\antigravity\scratch\Hayat_Competitive_Deep_Dive_2026.xlsx",
    r"C:\Users\iLink\.gemini\antigravity\scratch\market_competitiveness_master.xlsx"
]

for file_path in files:
    print(f"\n{'='*50}")
    print(f"Analyzing: {os.path.basename(file_path)}")
    print(f"Path: {file_path}")
    
    if not os.path.exists(file_path):
        print("ERROR: File does not exist.")
        continue
        
    try:
        xls = pd.ExcelFile(file_path)
        print(f"Sheet Names: {xls.sheet_names}")
        
        for sheet in xls.sheet_names:
            print(f"\n  --- Sheet: {sheet} ---")
            df = pd.read_excel(xls, sheet_name=sheet, nrows=5)
            print("  Columns:", list(df.columns))
            print("  Head:")
            print(df.head().to_string())
            
    except Exception as e:
        print(f"ERROR reading file: {e}")
