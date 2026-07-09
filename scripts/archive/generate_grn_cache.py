import os
import glob
import pandas as pd
import json
import logging

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

def generate_cache(data_dir):
    logging.info(f"Scanning GRN files in {data_dir}...")
    grn_files = glob.glob(os.path.join(data_dir, "*grnd*.xlsx"))
    
    cost_map = {} # Item -> [List of costs]
    
    for file in grn_files:
        try:
            df = pd.read_excel(file)
            # Standard GRN columns: 'Item Name', 'Cost Price'
            if 'Item Name' in df.columns and 'Cost Price' in df.columns:
                df['Cost Price'] = pd.to_numeric(df['Cost Price'], errors='coerce').fillna(0)
                for _, row in df.iterrows():
                    name = str(row['Item Name']).strip().upper()
                    cost = float(row['Cost Price'])
                    if cost > 0:
                        if name not in cost_map: cost_map[name] = []
                        cost_map[name].append(cost)
        except Exception as e:
            logging.error(f"Error reading {file}: {e}")

    # Calculate averages
    final_cache = {}
    for name, costs in cost_map.items():
        avg_cost = sum(costs) / len(costs)
        final_cache[name] = {
            'avg_cost': round(avg_cost, 2),
            'frequency': len(costs)
        }
    
    cache_path = os.path.join(data_dir, "grn_intelligence_cache.json")
    with open(cache_path, 'w') as f:
        json.dump(final_cache, f, indent=2)
        
    logging.info(f"SUCCESS: Generated cache for {len(final_cache)} items. Saved to {cache_path}")

if __name__ == "__main__":
    data_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
    generate_cache(data_dir)
