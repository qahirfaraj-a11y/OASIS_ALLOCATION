import json
import pandas as pd
import os
import glob

# Paths
DATA_DIR = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
BARCODE_DEPT_JSON = os.path.join(DATA_DIR, "barcode_department_map.json")
PRODUCT_BARCODE_JSON = os.path.join(DATA_DIR, "product_barcode_map.json")
MASTER_MAP_JSON = os.path.join(DATA_DIR, "master_product_dept_map.json")
EXCEL_FILES = glob.glob(os.path.join(DATA_DIR, "dept_*.xlsx"))

def consolidate():
    master_map = {} # Product Name -> Department
    barcode_to_dept = {} # Barcode -> Department
    product_to_barcode = {} # Product Name -> Barcode

    # 1. Load User's Primary Barcode-Dept Mapping
    print(f"Loading {BARCODE_DEPT_JSON}...")
    if os.path.exists(BARCODE_DEPT_JSON):
        with open(BARCODE_DEPT_JSON, 'r') as f:
            barcode_to_dept = json.load(f)
    
    # 2. Load DB-extracted Product-Barcode Mapping
    print(f"Loading {PRODUCT_BARCODE_JSON}...")
    if os.path.exists(PRODUCT_BARCODE_JSON):
        with open(PRODUCT_BARCODE_JSON, 'r') as f:
            product_to_barcode = json.load(f)

    # 3. Load Excel Snapshots for direct mappings
    print(f"Processing {len(EXCEL_FILES)} Excel files...")
    for f in EXCEL_FILES:
        try:
            df = pd.read_excel(f)
            # Normalize column names to uppercase for consistency
            df.columns = [c.upper() for c in df.columns]
            
            if 'BARCODE' in df.columns and 'DEPARTMENT' in df.columns:
                for _, row in df.iterrows():
                    barcode = str(row['BARCODE']).strip()
                    dept = str(row['DEPARTMENT']).strip()
                    if barcode and dept:
                        barcode_to_dept[barcode] = dept
            
            if 'ITM_NAME' in df.columns and 'DEPARTMENT' in df.columns:
                for _, row in df.iterrows():
                    product = str(row['ITM_NAME']).strip().upper()
                    dept = str(row['DEPARTMENT']).strip()
                    if product and dept:
                        master_map[product] = dept
        except Exception as e:
            print(f"Error processing {f}: {e}")

    # 4. Synthesize final Product -> Dept mapping
    print("Synthesizing final mapping...")
    # Link Product -> Barcode -> Dept
    for product, barcode in product_to_barcode.items():
        product_up = str(product).strip().upper()
        barcode_str = str(barcode).strip()
        if barcode_str in barcode_to_dept:
            master_map[product_up] = barcode_to_dept[barcode_str]

    # 5. Save Master Map
    with open(MASTER_MAP_JSON, 'w') as f:
        json.dump(master_map, f, indent=4)
    
    print(f"Master map generated with {len(master_map)} entries at {MASTER_MAP_JSON}")

if __name__ == "__main__":
    consolidate()
