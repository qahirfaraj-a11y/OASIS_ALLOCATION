import pandas as pd
import json
import os

def inspect_and_compare():
    json_path = r'C:\Users\iLink\.gemini\antigravity\scratch\supplier_rhythm_analysis.json'
    fulfillment_xlsx = r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Fulfillment_Summary.xlsx'
    intelligence_xlsx = r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx'

    print("--- Loading Data ---")
    
    # Load JSON
    if os.path.exists(json_path):
        with open(json_path, 'r') as f:
            json_data = json.load(f).get('po_rhythm', {})
        print(f"JSON: Loaded {len(json_data)} suppliers.")
    else:
        print("JSON file not found.")
        json_data = {}

    # Load Fulfillment XLSX
    if os.path.exists(fulfillment_xlsx):
        df_full = pd.read_excel(fulfillment_xlsx)
        print(f"Fulfillment XLSX: Loaded {len(df_full)} rows.")
        print("Columns:", df_full.columns.tolist())
        # print(df_full.head())
    else:
        print("Fulfillment XLSX not found.")
        df_full = pd.DataFrame()

    # Load Intelligence XLSX
    if os.path.exists(intelligence_xlsx):
        df_intel = pd.read_excel(intelligence_xlsx)
        print(f"Intelligence XLSX: Loaded {len(df_intel)} rows.")
        print("Columns:", df_intel.columns.tolist())
        # print(df_intel.head())
    else:
        print("Intelligence XLSX not found.")
        df_intel = pd.DataFrame()

    print("\n--- Comparing Samples ---")
    # Let's take 'BROOKSIDE DAIRY LIMITED' as a sample if it exists in all
    sample_supplier = 'BROOKSIDE DAIRY LIMITED'
    
    if sample_supplier in json_data:
        j = json_data[sample_supplier]
        print(f"\n[JSON] {sample_supplier}:")
        print(f"  Total Orders: {j.get('total_orders')}")
        print(f"  Median Gap: {j.get('median_gap')}")
        print(f"  Last Order: {j.get('last_order')}")

    if not df_full.empty:
        # Try to find the supplier. Might be slightly different name or column.
        # Common column names: 'Supplier', 'Supplier Name', 'Name'
        supp_col = [c for c in df_full.columns if 'Supplier' in c or 'Name' in c]
        if supp_col:
            match = df_full[df_full[supp_col[0]].str.contains(sample_supplier, na=False, case=False)]
            if not match.empty:
                print(f"\n[Fulfillment XLSX] {sample_supplier}:")
                print(match.iloc[0].to_dict())

    if not df_intel.empty:
        supp_col = [c for c in df_intel.columns if 'Supplier' in c or 'Name' in c]
        if supp_col:
            match = df_intel[df_intel[supp_col[0]].str.contains(sample_supplier, na=False, case=False)]
            if not match.empty:
                print(f"\n[Intelligence XLSX] {sample_supplier}:")
                print(match.iloc[0].to_dict())

if __name__ == "__main__":
    inspect_and_compare()
