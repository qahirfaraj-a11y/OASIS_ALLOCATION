import pandas as pd
import json
import os
import glob

# Reuse logic from generate_final_analysis.py
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
CATEGORY_FILES = {
    'Diapers': 'diapers.xlsx',
    'Wipes': 'wipes.xlsx',
    'Fabric Conditioner': 'fabricconditioner.xlsx',
    'Sanitary Towels': 'sanitarytowels.xlsx'
}
SALES_JSON = 'sales_forecasting_2025 (1).json'

def normalize(text):
    if not text: return ""
    return str(text).upper().strip()

def check_data_contents():
    # 1. Load Sales
    json_path = os.path.join(DATA_DIR, SALES_JSON)
    with open(json_path, 'r') as f:
        sales_data = json.load(f)
    print(f"Total sales entries: {len(sales_data)}")

    # 2. Check Categories
    for cat, filename in CATEGORY_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            df = pd.read_excel(path)
            print(f"\n{cat} ({filename}): {len(df)} SKUs")
            # Sample vendor names
            vendors = df['VENDOR_NAME'].unique()
            print(f"Sample Vendors: {vendors[:5]}")
            
            # Count Hayat
            hayat_skus = df[df['VENDOR_NAME'].str.contains('HAYAT', na=False)]
            print(f"Hayat SKUs by Vendor Name: {len(hayat_skus)}")
            
            # Sample items
            print(f"Sample Items: {df['ITM_NAME'].head().tolist()}")

if __name__ == "__main__":
    check_data_contents()
