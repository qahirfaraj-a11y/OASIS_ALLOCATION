import pandas as pd
import json
import os
import glob

# Define the category files and their corresponding Department names
# User provided: 
# "fabricconditioner.xlsx"
# "Cooking.XLSX"
# "diapers.xlsx"
# "Honey.XLSX"
# "JAMS.XLSX"
# "Mayonnaise.XLSX"
# "Mustard.XLSX"
# "Tomato and Ketchuo.XLSX"

CATEGORY_FILES = {
    "fabricconditioner.xlsx": "Fabric Softener",
    "Cooking.XLSX": "Cooking Aids",
    "diapers.xlsx": "Diapers",
    "Honey.XLSX": "Honey",
    "JAMS.XLSX": "Jams & Spreads",
    "Mayonnaise.XLSX": "Sauces & Condiments",
    "Mustard.XLSX": "Sauces & Condiments",
    "Tomato and Ketchuo.XLSX": "Sauces & Condiments" # Fixing typo in name mapping
}

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
OUTPUT_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\product_department_map.json"

def get_file_path(filename):
    # Case insensitive search
    files = glob.glob(os.path.join(DATA_DIR, "*"))
    for f in files:
        if os.path.basename(f).lower() == filename.lower():
            return f
    return None

def main():
    print("Building Product-Department Map...")
    master_map = {}
    
    for filename, dept_name in CATEGORY_FILES.items():
        fpath = get_file_path(filename)
        if not fpath:
            print(f"Warning: File {filename} not found.")
            continue
            
        try:
            print(f"Processing {filename} -> {dept_name}")
            # Assuming 'Item Name' is the column, based on GRN structure (standard O.A.S.I.S format usually)
            # We'll check the output of inspect_category.py to be sure, but usually it's "Description" or "Item Name"
            # Let's try to read it.
            
            # Reads all cols to find the right one
            df = pd.read_excel(fpath)
            
            # Possible column names
            col_candidates = ["Item Name", "Description", "Product", "Item Description", "ITM_NAME", "Item Name"]
            item_col = None
            for col in df.columns:
                if col in col_candidates:
                    item_col = col
                    break
            
            if not item_col:
                # Fallback: check text columns
                for col in df.columns:
                    if df[col].dtype == 'object':
                         # simplistic check
                         item_col = col
                         break
            
            if item_col:
                print(f"  Using column: {item_col}")
                items = df[item_col].dropna().unique()
                for item in items:
                    item_clean = str(item).strip().upper()
                    master_map[item_clean] = dept_name
            else:
                print(f"  Error: Could not identify Item Name column in {filename}")

        except Exception as e:
            print(f"  Error processing {filename}: {e}")

    print(f"mapped {len(master_map)} items to {len(set(master_map.values()))} departments.")
    
    with open(OUTPUT_JSON, 'w') as f:
        json.dump(master_map, f, indent=4)
    print(f"Saved to {OUTPUT_JSON}")

if __name__ == "__main__":
    main()
