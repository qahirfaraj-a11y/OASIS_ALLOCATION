import pandas as pd
import os
import json
import glob

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
OUTPUT_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data\product_supplier_map.json"

def get_grn_files():
    # Pattern to match the user's requested files (grnds_*.xlsx and grnd_*.xlsx)
    pattern1 = os.path.join(DATA_DIR, "grnds_*.xlsx")
    pattern2 = os.path.join(DATA_DIR, "grnd_*.xlsx")
    return glob.glob(pattern1) + glob.glob(pattern2)

def main():
    files = get_grn_files()
    print(f"Found {len(files)} GRN files to process.")
    
    master_map = {}
    
    for f in files:
        try:
            print(f"Processing {os.path.basename(f)}...")
            df = pd.read_excel(f, usecols=["Item Name", "Vendor Code - Name"])
            
            # Drop missing
            df = df.dropna()
            
            for index, row in df.iterrows():
                item = str(row["Item Name"]).strip().upper()
                vendor_raw = str(row["Vendor Code - Name"]).strip()
                
                # Cleanup Vendor "SA0015 - ALISON PRODUCTS LTD" -> "ALISON PRODUCTS LTD"
                parts = vendor_raw.split(" - ", 1)
                if len(parts) > 1:
                    vendor = parts[1].strip()
                else:
                    vendor = vendor_raw
                
                if item and vendor:
                    master_map[item] = vendor
                    
        except Exception as e:
            print(f"Error processing {f}: {e}")

    print(f"saving map with {len(master_map)} items to {OUTPUT_JSON}")
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(master_map, f, indent=4)

if __name__ == "__main__":
    main()
