import pandas as pd
import glob
import os

# Configuration
DATA_DIR = r"app/data"
OUTPUT_HAYAT = "Hayat_Kimya_Fulfillment_Detail.xlsx"
OUTPUT_ALL = "All_Suppliers_Fulfillment_Detail.xlsx"

def load_data():
    # Load POs
    # Pattern to catch all PO files
    po_files = glob.glob(os.path.join(DATA_DIR, "po_*.xlsx"))
    print(f"Loading {len(po_files)} PO files...")
    po_dfs = []
    for f in po_files:
        try:
            print(f"  Reading {os.path.basename(f)}...")
            df = pd.read_excel(f)
            po_dfs.append(df)
        except Exception as e:
            print(f"  Error loading {f}: {e}")
    
    if not po_dfs:
        raise ValueError("No PO files found or loaded.")
    
    po_df = pd.concat(po_dfs, ignore_index=True)
    
    # Load GRNs
    # Pattern to catch grnd_*, grnds_* etc.
    grn_files = glob.glob(os.path.join(DATA_DIR, "grnd*.xlsx"))
    print(f"Loading {len(grn_files)} GRN files...")
    grn_dfs = []
    for f in grn_files:
        try:
            print(f"  Reading {os.path.basename(f)}...")
            df = pd.read_excel(f)
            grn_dfs.append(df)
        except Exception as e:
            print(f"  Error loading {f}: {e}")

    if not grn_dfs:
        raise ValueError("No GRN files found or loaded.")
        
    grn_df = pd.concat(grn_dfs, ignore_index=True)
    
    return po_df, grn_df

def process_and_export():
    po_df, grn_df = load_data()
    
    # Normalize headers
    po_df.columns = po_df.columns.str.strip()
    grn_df.columns = grn_df.columns.str.strip()
    
    # Clean duplicates if any (entire rows)
    po_df.drop_duplicates(inplace=True)
    grn_df.drop_duplicates(inplace=True)

    print(f"Total PO lines loaded: {len(po_df)}")
    print(f"Total GRN lines loaded: {len(grn_df)}")

    # Prepare PO Data
    # We need PO Date for each PO.
    # Group by PO No to get the date.
    # Note: 'PO No' might be repeated for line items, but PO Date should be same.
    po_cols = ['PO No', 'PO Date', 'Vendor Code / Name']
    po_master = po_df[po_cols].drop_duplicates(subset=['PO No'])
    
    # Convert PO Date
    po_master['PO Date'] = pd.to_datetime(po_master['PO Date'], errors='coerce')
    po_master['PO No'] = po_master['PO No'].astype(str).str.strip()
    
    # Prepare GRN Data
    # GRN has 'PO No'. 
    # GRN Date is what we need.
    grn_df['GRN Date'] = pd.to_datetime(grn_df['GRN Date'], errors='coerce')
    grn_df['PO No'] = grn_df['PO No'].astype(str).str.strip()
    
    # Merge GRN with PO info
    # We merge on PO No. 
    # Left merge on GRN to keep all GRNs, identifying their PO dates.
    merged = pd.merge(grn_df, po_master, on='PO No', how='left')
    
    # Calculate Fulfillment Days
    merged['Fulfillment Days'] = (merged['GRN Date'] - merged['PO Date']).dt.days
    
    # Vendor Name cleanup
    # Use 'Vendor Code - Name' from GRN if available, else from PO
    merged['Vendor Name'] = merged['Vendor Code - Name'].fillna(merged['Vendor Code / Name'])
    
    # Select Columns for Report
    report_cols = [
        'Vendor Name',
        'PO No', 'PO Date', 
        'GRN No', 'GRN Date', 
        'Fulfillment Days', 
        'Item Name', 'GRN Qty', 'Net Amt'
    ]
    
    # Filter only valid rows (at least need Dates)
    # We keep rows even if fulfilment days is NaN (orphan GRNs? or Missing PO date?) 
    # but user wants calculated days.
    # Let's keep everything but sort by Vendor.
    
    final_df = merged[report_cols].copy()
    
    # Filter for Hayat
    hayat_mask = final_df['Vendor Name'].str.contains('HAYAT KIMYA', case=False, na=False)
    hayat_df = final_df[hayat_mask].copy()
    
    print(f"Generated {len(hayat_df)} rows for Hayat Kimya.")
    
    # Export Hayat File
    print(f"Exporting Hayat data to {OUTPUT_HAYAT}...")
    hayat_df.sort_values(by=['PO Date', 'GRN Date'], inplace=True)
    hayat_df.to_excel(OUTPUT_HAYAT, index=False)
    
    # Export All Suppliers File
    # To avoid a massive file if not needed, we will just dump it.
    print(f"Exporting All Suppliers data to {OUTPUT_ALL}...")
    final_df.sort_values(by=['Vendor Name', 'PO Date'], inplace=True)
    final_df.to_excel(OUTPUT_ALL, index=False)
    
    # Summary Stats for All Suppliers
    # Calculate average per vendor
    print("Calculating summary stats...")
    valid_days = final_df.dropna(subset=['Fulfillment Days'])
    summary = valid_days.groupby('Vendor Name')['Fulfillment Days'].agg(['count', 'mean', 'median', 'min', 'max'])
    summary['% Within 3 Days'] = valid_days[valid_days['Fulfillment Days'] <= 3].groupby('Vendor Name')['Fulfillment Days'].count() / summary['count'] * 100
    
    summary_file = "Supplier_Fulfillment_Summary.xlsx"
    summary.to_excel(summary_file)
    print(f"Summary stats exported to {summary_file}")

if __name__ == "__main__":
    process_and_export()
