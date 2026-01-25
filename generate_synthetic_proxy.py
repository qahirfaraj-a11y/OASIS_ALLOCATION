import json
import os
import pandas as pd
import numpy as np

# --- Configuration ---
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\synthetic_core_profile.json"

FILES = {
    "forecast": "sales_forecasting_2025 (1).json",
    "suppliers": "supplier_patterns_2025 (3).json",
    "profitability": "sales_profitability_intelligence_2025_updated.json",
    "department_map": "product_department_map.json",
    "grn_freq": "sku_grn_frequency.json"
}

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if not os.path.exists(path):
        print(f"Warning: File not found: {path}")
        return {}
    with open(path, 'r') as f:
        return json.load(f)

def run_synthetic_proxy_generation():
    print("--- 1. Ingesting Mega Store Data (The 'Whale') ---")
    forecast_data = load_json(FILES["forecast"])
    supplier_patterns = load_json(FILES["suppliers"])
    profit_data = load_json(FILES["profitability"])
    dept_map = load_json(FILES["department_map"])
    grn_freq_map = load_json(FILES["grn_freq"])

    print(f"Loaded {len(forecast_data)} Forecast Items")
    
    # Convert to DataFrame for easier "Mining"
    data = []
    for p_name, fc in forecast_data.items():
        clean_name = p_name.strip().upper()
        
        # Get Dept
        dept = dept_map.get(clean_name, "GENERAL")
        
        # Get Velocity Metrics
        ads = fc.get('avg_daily_sales', 0)
        grn_freq = grn_freq_map.get(clean_name, 0)
        
        # Get Supplier Reliability (via patterns if possible, or skip for now)
        # simplistic check
        
        data.append({
            "Product": clean_name,
            "Department": dept,
            "Avg_Daily_Sales": ads,
            "GRN_Frequency": grn_freq,
            "Revenue": profit_data.get(clean_name, {}).get('revenue', 0)
        })
        
    df = pd.DataFrame(data)
    print(f"Total Universe: {len(df)} SKUs")

    print("\n--- 2. Applying Fractal Downscaling (The 'Filter') ---")
    
    # RULE 1: High Velocity Floor (Micro-Stores rely on turnover)
    # Must sell at least 0.3 units/day (approx 1 unit every 3 days)
    # OR have very high GRN frequency (Staple)
    velocity_mask = (df['Avg_Daily_Sales'] >= 0.3) | (df['GRN_Frequency'] > 0.6)
    core_df = df[velocity_mask].copy()
    print(f"Velocity Filter (ADS>0.3 or GRN>0.6): Reduced from {len(df)} to {len(core_df)} SKUs")

    # RULE 2: Choice Reduction (Category Compression)
    # A Mega Store has 50 types of juice. A Small Store needs the Top 5.
    # Logic: Group by Dept -> Keep Top 20% by Volume (Pareto Principle)
    
    print("Applying Pareto Compression per Department...")
    final_proxy_list = []
    
    for dept, group in core_df.groupby("Department"):
        # Sort by SALES VELOCITY (ADS) to find the "Winners"
        group = group.sort_values(by="Avg_Daily_Sales", ascending=False)
        
        # Determine "Small Store Slot Count" for this Dept
        # We assume a Small Store has ~10% of the variety of the Mega Store
        # But we ensure at least Top 3 items are kept if they exist
        total_items = len(group)
        slots_to_keep = max(3, int(total_items * 0.15)) 
        
        # Keep the top items
        winners = group.head(slots_to_keep)
        final_proxy_list.append(winners)
        
    proxy_df = pd.concat(final_proxy_list)
    print(f"Pareto Compression: Final 'Synthetic Proxy' contains {len(proxy_df)} SKUs")

    # --- 3. Normalization & Scoring ---
    # Assign a "Proxy Rank" based on their essential nature
    # 1.0 = Absolute Core (High GRN)
    # 0.5 = High Velocity (High ADS)
    
    proxy_result = {}
    for _, row in proxy_df.iterrows():
        is_staple = row['GRN_Frequency'] > 0.7
        rank_score = 1.0 if is_staple else 0.5
        
        proxy_result[row['Product']] = {
            "Proxy_Status": "Core_Staple" if is_staple else "Core_Velocity",
            "Proxy_Score_Boost": rank_score * 1000, # The "Weight" for the integration
            "Ideal_ADS": row['Avg_Daily_Sales']
        }
        
    # Stats
    staple_count = sum(1 for x in proxy_result.values() if x['Proxy_Status'] == 'Core_Staple')
    print(f"\n--- Proxy Profile Generated ---")
    print(f"Core Staples (Must Haves): {staple_count}")
    print(f"High Velocity (Traffic Drivers): {len(proxy_result) - staple_count}")
    
    # Save
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(proxy_result, f, indent=2)
    print(f"Saved Synthetic Proxy to: {OUTPUT_FILE}")

if __name__ == "__main__":
    run_synthetic_proxy_generation()
