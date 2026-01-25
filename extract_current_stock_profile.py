import pandas as pd
import json
import os
import glob

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data"
DEPT_FILES = [
    "dept_1_50.xlsx",
    "dept_51_100.xlsx",
    "dept_101_150.xlsx",
    "dept_151_200.xlsx",
    "dept_201_250.xlsx",
    "dept_301_350.xlsx"
]

DEPT_MAP_PATH = os.path.join(DATA_DIR, "product_department_map.json")
SUP_MAP_PATH = os.path.join(DATA_DIR, "product_supplier_map.json")
GRN_MAP_PATH = os.path.join(DATA_DIR, "product_supplier_map_grn.json")
OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\capital_allocation_report.xlsx"

def load_json(path):
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def main():
    print("Loading Mapping Data...")
    dept_map = load_json(DEPT_MAP_PATH)
    sup_map = load_json(SUP_MAP_PATH)
    grn_map = load_json(GRN_MAP_PATH)
    
    print(f"Loaded {len(dept_map)} Dept Mappings, {len(sup_map)} Primary Supplier Mappings, {len(grn_map)} GRN Supplier Mappings.")

    all_items = []

    print("Processing Stock Snapshot Files...")
    for filename in DEPT_FILES:
        fpath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(fpath):
            print(f"Warning: {filename} not found.")
            continue
            
        try:
            print(f"  Reading {filename}...")
            df = pd.read_excel(fpath)
            
            # Normalizing columns
            df.columns = [c.strip().upper() for c in df.columns]
            
            req_cols = ['ITM_NAME', 'STOCK', 'SELLPRICE', 'VENDOR_NAME', 'DEPARTMENT']
            missing = [c for c in req_cols if c not in df.columns]
            if missing:
                print(f"    Missing columns {missing} in {filename}. Skipping.")
                continue

            for _, row in df.iterrows():
                name = str(row['ITM_NAME']).strip().upper()
                raw_dept = str(row['DEPARTMENT']).strip().upper()
                raw_sup = str(row['VENDOR_NAME']).strip().upper()
                
                # --- EXCLUSION LOGIC ---
                if "NO GRN" in raw_sup:
                    continue
                
                stock = float(row['STOCK']) if pd.notnull(row['STOCK']) else 0
                price = float(row['SELLPRICE']) if pd.notnull(row['SELLPRICE']) else 0
                
                # Apply Mapping overrides
                final_dept = dept_map.get(name, raw_dept)
                
                # Supplier lookup chain
                final_sup = sup_map.get(name)
                if not final_sup:
                    final_sup = grn_map.get(name)
                if not final_sup:
                    final_sup = raw_sup
                
                value = stock * price
                
                all_items.append({
                    'Product': name,
                    'Department': final_dept,
                    'Supplier': final_sup,
                    'Stock_Qty': stock,
                    'Unit_Price': price,
                    'Stock_Value': value
                })
        except Exception as e:
            print(f"    Error processing {filename}: {e}")

    if not all_items:
        print("No stock data collected after exclusions.")
        return

    master_df = pd.DataFrame(all_items)
    
    print("\nGenerating Aggregations...")
    
    # Global totals
    total_val = master_df['Stock_Value'].sum()
    total_qty = master_df['Stock_Qty'].sum()
    total_skus = len(master_df)

    # 1. Departmental Analysis
    dept_agg = master_df.groupby('Department').agg(
        SKU_Count=('Product', 'count'),
        Total_Qty=('Stock_Qty', 'sum'),
        Total_Value=('Stock_Value', 'sum')
    ).reset_index()
    
    dept_agg['Qty_Share_%'] = (dept_agg['Total_Qty'] / total_qty) * 100
    dept_agg['Value_Share_%'] = (dept_agg['Total_Value'] / total_val) * 100
    dept_agg = dept_agg.sort_values(by='Total_Value', ascending=False)

    # 2. Supplier Analysis (Concentration)
    sup_agg = master_df.groupby('Supplier').agg(
        SKU_Count=('Product', 'count'),
        Total_Qty=('Stock_Qty', 'sum'),
        Total_Value=('Stock_Value', 'sum')
    ).reset_index()
    
    sup_agg['Qty_Share_%'] = (sup_agg['Total_Qty'] / total_qty) * 100
    sup_agg['Value_Share_%'] = (sup_agg['Total_Value'] / total_val) * 100
    sup_agg['SKU_Share_%'] = (sup_agg['SKU_Count'] / total_skus) * 100
    sup_agg = sup_agg.sort_values(by='Total_Value', ascending=False)

    # 3. Supplier-Department Share Analysis (Dominance)
    print("Calculating Supplier Dominance per Department...")
    sup_dept_agg = master_df.groupby(['Department', 'Supplier']).agg(
        SKU_Count=('Product', 'count'),
        Total_Value=('Stock_Value', 'sum')
    ).reset_index()
    
    # Calculate share within department
    dept_totals = master_df.groupby('Department')['Stock_Value'].sum().reset_index()
    dept_totals.columns = ['Department', 'Dept_Total_Value']
    
    sup_dept_agg = sup_dept_agg.merge(dept_totals, on='Department')
    sup_dept_agg['Share_Within_Dept_%'] = (sup_dept_agg['Total_Value'] / sup_dept_agg['Dept_Total_Value']) * 100
    
    # Rank suppliers within each department
    sup_dept_agg['Rank'] = sup_dept_agg.groupby('Department')['Total_Value'].rank(ascending=False, method='first')
    
    # Create a simplified "Dominance" sheet (Rank 1 only)
    dominance_df = sup_dept_agg[sup_dept_agg['Rank'] == 1].copy()
    dominance_df = dominance_df.sort_values(by='Dept_Total_Value', ascending=False)
    dominance_df = dominance_df[['Department', 'Supplier', 'Share_Within_Dept_%', 'Dept_Total_Value', 'SKU_Count']]
    dominance_df.columns = ['Department', 'Dominant_Supplier', 'Dominance_Share_%', 'Department_Total_Value', 'Dominant_SKU_Count']

    print("\n--- NEW CAPITAL INSIGHTS ---")
    print(f"Total Portfolio Value (Excl. NO GRN): {total_val:,.2f}")
    print(f"Total Active SKUs: {total_skus}")
    
    print("\nTop 5 Departments and their Dominant Suppliers by Value:")
    print(dominance_df.head(10).to_string(index=False))

    # Save to Excel with triple sheets
    print(f"Saving expanded report to {OUTPUT_FILE}...")
    with pd.ExcelWriter(OUTPUT_FILE) as writer:
        dept_agg.to_excel(writer, sheet_name='Departmental_Allocation', index=False)
        sup_agg.to_excel(writer, sheet_name='Supplier_Concentration', index=False)
        sup_dept_agg.to_excel(writer, sheet_name='Supplier_Dept_Breakdown', index=False)
        dominance_df.to_excel(writer, sheet_name='Category_Dominance', index=False)

if __name__ == "__main__":
    main()
