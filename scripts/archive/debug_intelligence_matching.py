import pandas as pd
import json
import os

# Configuration
DATA_DIR = r"app/data"
JSON_FILES = {
    "forecast": r"app/data/sales_forecasting_2025 (1).json",
}
DEPT_FILE = "Honey.XLSX"

def debug_matching():
    # Load JSON Keys
    with open(JSON_FILES["forecast"], 'r', encoding='utf-8') as f:
        db = json.load(f)
        keys = list(db.keys())
        print("--- JSON Keys (First 10) ---")
        for k in keys[:10]:
            print(f"'{k}'")

    # Load Excel Names
    path = os.path.join(DATA_DIR, DEPT_FILE)
    df = pd.read_excel(path)
    print(f"\n--- Excel Names from {DEPT_FILE} (First 10) ---")
    for val in df['ITM_NAME'].head(10):
        print(f"'{val}'")
        
    # Check for simple containment
    print("\n--- Testing Containment ---")
    matched = 0
    for val in df['ITM_NAME'].head(20):
        val_str = str(val).strip()
        for k in keys:
            if val_str in k or k in val_str:
                print(f"MATCH FOUND: Excel '{val_str}' <---> JSON '{k}'")
                matched += 1
                break
    
    if matched == 0:
        print("No containment matches found in first 20 items.")

if __name__ == "__main__":
    debug_matching()
