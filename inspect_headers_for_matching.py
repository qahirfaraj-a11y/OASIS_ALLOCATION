import pandas as pd
import os
import json

DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
CATEGORIES = ['diapers.xlsx', 'fabricconditioner.xlsx', 'wipes.xlsx', 'sanitarytowels.xlsx']
DATABASES = [
    'sales_forecasting_2025 (1).json',
    'sales_profitability_intelligence_2025.json',
    'supplier_patterns_2025 (3).json',
    'supplier_quality_scores_2025 (1).json',
    'topselqty.xlsx'
]

def inspect_headers():
    print("--- CATEGORY HEADERS ---")
    for cat in CATEGORIES:
        path = os.path.join(DATA_DIR, cat)
        if os.path.exists(path):
            df = pd.read_excel(path, nrows=0)
            print(f"{cat}: {list(df.columns)}")
        else:
            print(f"{cat}: NOT FOUND")

    print("\n--- EXCEL DATABASE HEADERS ---")
    fin_path = os.path.join(DATA_DIR, 'topselqty.xlsx')
    if os.path.exists(fin_path):
        df = pd.read_excel(fin_path, nrows=0)
        print(f"topselqty.xlsx: {list(df.columns)}")

    print("\n--- JSON DATABASE SAMPLE KEYS ---")
    for db in DATABASES:
        if db.endswith('.json'):
            path = os.path.join(DATA_DIR, db)
            if os.path.exists(path):
                with open(path, 'r') as f:
                    data = json.load(f)
                    first_key = list(data.keys())[0] if data else "EMPTY"
                    print(f"{db}: First Key Sample -> {first_key}")
                    if isinstance(data[first_key], dict):
                        print(f"  Fields: {list(data[first_key].keys())}")

if __name__ == "__main__":
    inspect_headers()
