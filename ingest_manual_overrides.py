import pandas as pd
import json
import os

SCORECARD_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"
MAP_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data\product_department_map.json"

def main():
    if not os.path.exists(SCORECARD_PATH):
        print("Scorecard not found.")
        return

    print("Reading Manual Changes from Scorecard...")
    df = pd.read_csv(SCORECARD_PATH)
    
    # Extract items that have a non-'General Merchandise' department
    manual_map = {}
    for _, row in df.iterrows():
        name = str(row['Product']).strip().upper()
        dept = str(row['Department']).strip()
        if dept != 'General Merchandise' and dept != 'nan':
            manual_map[name] = dept

    print(f"Extracted {len(manual_map)} potential mappings from scorecard.")

    # Load existing map
    if os.path.exists(MAP_PATH):
        with open(MAP_PATH, 'r') as f:
            master_map = json.load(f)
    else:
        master_map = {}

    # Update master_map with manual changes
    # manual changes from the user's spreadsheet take priority
    update_count = 0
    new_count = 0
    for name, dept in manual_map.items():
        if name in master_map:
            if master_map[name] != dept:
                master_map[name] = dept
                update_count += 1
        else:
            master_map[name] = dept
            new_count += 1

    print(f"Updated {update_count} existing mappings.")
    print(f"Added {new_count} new mappings.")

    # Save master map
    with open(MAP_PATH, 'w') as f:
        json.dump(master_map, f, indent=4)
    print(f"Saved updated master map to {MAP_PATH}")

if __name__ == "__main__":
    main()
