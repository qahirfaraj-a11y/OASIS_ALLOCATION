import pandas as pd
import json
import os
import glob
import re

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
OUTPUT_JSON = os.path.join(DATA_DIR, "product_supplier_map_grn.json")

def clean_vendor_name(name):
    if not isinstance(name, str):
        return "Unknown"
    # Remove leading code like "SA0015 - "
    # Regex: Start with characters, then space, hyphen, space
    # Or just split by " - " and take the last part if multiple, or the second part.
    # Usually format is "CODE - NAME"
    parts = name.split(' - ', 1)
    if len(parts) > 1:
        return parts[1].strip()
    return name.strip()

def main():
    print("Extracting Supplier Data from GRN Files...")
    grn_files = glob.glob(os.path.join(DATA_DIR, "grnds_*.xlsx"))
    print(f"Found {len(grn_files)} GRN files.")

    grn_map = {}
    
    for fpath in grn_files:
        try:
            print(f"Processing {os.path.basename(fpath)}...")
            df = pd.read_excel(fpath)
            
            # Check required cols
            if 'Vendor Code - Name' in df.columns and 'Item Name' in df.columns:
                target_df = df[['Vendor Code - Name', 'Item Name']].dropna()
                
                for _, row in target_df.iterrows():
                    raw_vendor = row['Vendor Code - Name']
                    item_name = str(row['Item Name']).strip().upper()
                    
                    clean_vendor = clean_vendor_name(raw_vendor)
                    clean_vendor = clean_vendor.upper().strip()
                    
                    # Store in map
                    if item_name not in grn_map:
                        grn_map[item_name] = clean_vendor
            else:
                print(f"  Skipping {os.path.basename(fpath)}: Missing columns.")

        except Exception as e:
            print(f"  Error reading {os.path.basename(fpath)}: {e}")

    print(f"Mapped {len(grn_map)} items to suppliers from GRN data.")
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(grn_map, f, indent=4)
    print(f"Saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
