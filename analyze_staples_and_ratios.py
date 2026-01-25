import pandas as pd
import json
import os
import glob

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
SCORECARD_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"
OUTPUT_STAPLES = r"c:\Users\iLink\.gemini\antigravity\scratch\staple_products.json"

def main():
    print("Identifying Staple Products from GRN Frequency...")
    
    grn_files = glob.glob(os.path.join(DATA_DIR, "grnds_*.xlsx"))
    total_grn_batches = len(grn_files)
    print(f"Analyzing {total_grn_batches} GRN batches...")

    product_occurrence = {}
    
    for fpath in grn_files:
        try:
            df = pd.read_excel(fpath)
            if 'Item Name' in df.columns:
                unique_items = df['Item Name'].dropna().unique()
                for item in unique_items:
                    name = str(item).strip().upper()
                    product_occurrence[name] = product_occurrence.get(name, 0) + 1
        except Exception:
            continue

    # A product is a "Staple" if it appears in more than 50% of GRN batches
    # (Tweak this threshold based on results)
    threshold = 0.5 * total_grn_batches
    staples = [name for name, count in product_occurrence.items() if count >= threshold]
    
    # Export full frequency map for Perfect Allocation Phase 2
    frequency_map = {name: count / total_grn_batches for name, count in product_occurrence.items()}
    
    print(f"Identified {len(staples)} Staple items (present in >= 50% of GRN intake).")
    
    with open(OUTPUT_STAPLES, 'w') as f:
        json.dump(staples, f, indent=4)
        
    with open(os.path.join(DATA_DIR, "sku_grn_frequency.json"), 'w') as f:
        json.dump(frequency_map, f, indent=4)
        
    print(f"Staples and Frequency map saved.")

    # --- Calculating Scaling Ratios ---
    print("\nCalculating Scaling Ratios per Department & Supplier...")
    score_df = pd.read_csv(SCORECARD_PATH)
    
    # 1. Dept Stats
    dept_stats = score_df.groupby('Department').agg(
        SKU_Count=('Product', 'count'),
        Total_Value=('Total_Revenue', 'sum')
    ).reset_index()
    
    total_portfolio_value = dept_stats['Total_Value'].sum()
    dept_stats['Capital_Weight'] = dept_stats['Total_Value'] / total_portfolio_value
    dept_stats['SKU_per_Million'] = (dept_stats['SKU_Count'] / 115_000_000) * 1_000_000 # Using 115M as reference portfolio
    
    dept_stats.to_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\department_scaling_ratios.csv", index=False)
    
    # 2. Supplier Share within Dept (Phase 1)
    sup_dept_stats = score_df.groupby(['Department', 'Supplier']).agg(
        Total_Value=('Total_Revenue', 'sum')
    ).reset_index()
    
    # Calculate share within each dept
    dept_totals = sup_dept_stats.groupby('Department')['Total_Value'].transform('sum')
    sup_dept_stats['Supplier_Share'] = sup_dept_stats['Total_Value'] / dept_totals
    
    # Export as nested dict for scorecard logic
    sup_share_map = {}
    for _, row in sup_dept_stats.iterrows():
        dept = row['Department']
        sup = row['Supplier']
        share = row['Supplier_Share']
        if dept not in sup_share_map: sup_share_map[dept] = {}
        sup_share_map[dept][sup] = share
        
    with open(os.path.join(DATA_DIR, "supplier_dept_ratios.json"), 'w') as f:
        json.dump(sup_share_map, f, indent=4)
        
    print("Department and Supplier ratios exported.")

if __name__ == "__main__":
    main()
