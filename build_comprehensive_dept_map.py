import pandas as pd
import json
import os
import glob

# Data source configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
OUTPUT_JSON = os.path.join(DATA_DIR, "product_department_map.json")

# The specific files provided by the user
DEPT_FILES = [
    "dept_1_50.xlsx",
    "dept_51_100.xlsx",
    "dept_101_150.xlsx",
    "dept_151_200.xlsx",
    "dept_201_250.xlsx",
    "dept_301_350.xlsx"
]

def main():
    print("Building Comprehensive Product-Department Map from provided Excel files...")
    master_map = {}
    barcode_dept_map = {} # Secondary map for future robustness
    
    # Step 1: Process Bulk Department Files
    for filename in DEPT_FILES:
        fpath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(fpath):
            print(f"Warning: File {filename} not found in {DATA_DIR}")
            continue
            
        try:
            print(f"Processing Bulk File: {filename}")
            df = pd.read_excel(fpath)
            
            # Expected columns: BARCODE, ITM_NAME, DEPARTMENT
            if 'ITM_NAME' in df.columns and 'DEPARTMENT' in df.columns:
                print(f"  Mapping {len(df)} items via ITM_NAME")
                for _, row in df.iterrows():
                    name = str(row['ITM_NAME']).strip().upper()
                    dept = str(row['DEPARTMENT']).strip()
                    
                    # Update master map (Bulk files might overwrite legacy if newer/more comprehensive)
                    master_map[name] = dept
                    
                    # Map barcode as well
                    if 'BARCODE' in df.columns:
                        barcode = str(row['BARCODE']).strip()
                        if barcode and barcode != 'nan':
                            barcode_dept_map[barcode] = dept
            else:
                print(f"  Error: Required columns ITM_NAME or DEPARTMENT missing in {filename}")

        except Exception as e:
            print(f"  Error processing {filename}: {e}")

    # Step 3: Save results
    print(f"Mapped {len(master_map)} unique item names to departments.")
    print(f"Mapped {len(barcode_dept_map)} barcodes to departments.")
    
    # We save primarily the item name map as that's what generate_allocation_scorecard.py uses
    # But we can embed the barcode map or save it separately. 
    # For now, let's keep the format compatible but enriched.
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(master_map, f, indent=4)
    print(f"Saved to {OUTPUT_JSON}")

    # Also save a barcode map just in case
    BARCODE_MAP_PATH = os.path.join(DATA_DIR, "barcode_department_map.json")
    with open(BARCODE_MAP_PATH, 'w') as f:
        json.dump(barcode_dept_map, f, indent=4)
    print(f"Saved barcode map to {BARCODE_MAP_PATH}")

if __name__ == "__main__":
    main()
