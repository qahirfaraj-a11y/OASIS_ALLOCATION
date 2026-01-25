import pandas as pd
import glob
import os

# Configuration
DATA_DIR = r"app/data"
VENDOR_NAME = "SH0081 - HAYAT KIMYA  K  H PRODUCTS LTD"

def load_data():
    # Load POs
    po_files = glob.glob(os.path.join(DATA_DIR, "po_*.xlsx"))
    print(f"Loading {len(po_files)} PO files...")
    po_dfs = []
    for f in po_files:
        try:
            df = pd.read_excel(f)
            po_dfs.append(df)
        except Exception as e:
            print(f"Error loading {f}: {e}")
    
    if not po_dfs:
        raise ValueError("No PO files found or loaded.")
    
    po_df = pd.concat(po_dfs, ignore_index=True)
    
    # Load GRNs
    grn_files = glob.glob(os.path.join(DATA_DIR, "grnd*.xlsx"))
    print(f"Loading {len(grn_files)} GRN files...")
    grn_dfs = []
    for f in grn_files:
        try:
            df = pd.read_excel(f)
            grn_dfs.append(df)
        except Exception as e:
            print(f"Error loading {f}: {e}")

    if not grn_dfs:
        raise ValueError("No GRN files found or loaded.")
        
    grn_df = pd.concat(grn_dfs, ignore_index=True)
    
    return po_df, grn_df

def analyze_fulfillment():
    po_df, grn_df = load_data()
    
    # Normalize headers
    po_df.columns = po_df.columns.str.strip()
    grn_df.columns = grn_df.columns.str.strip()
    
    # Filter for Hayat
    hayat_pos = po_df[po_df['Vendor Code / Name'] == VENDOR_NAME].copy()
    hayat_grns = grn_df[grn_df['Vendor Code - Name'] == VENDOR_NAME].copy()
    
    print(f"Found {len(hayat_pos)} PO lines for Hayat.")
    print(f"Found {len(hayat_grns)} GRN lines for Hayat.")
    
    # Extract unique PO dates
    # Assuming 'PO No' is the key.
    # PO Date format might need parsing
    hayat_pos['PO Date'] = pd.to_datetime(hayat_pos['PO Date'], errors='coerce')
    
    # Create a mapping of PO No -> PO Date
    # We take the first date found for a PO No (should be unique)
    po_dates = hayat_pos.groupby('PO No')['PO Date'].first().reset_index()
    
    # Process GRNs
    hayat_grns['GRN Date'] = pd.to_datetime(hayat_grns['GRN Date'], errors='coerce')
    
    # Ensure PO No is string for merging
    hayat_grns['PO No'] = hayat_grns['PO No'].astype(str)
    po_dates['PO No'] = po_dates['PO No'].astype(str)
    
    # Merge
    merged = pd.merge(hayat_grns, po_dates, on='PO No', how='left')
    
    # Calculate difference
    merged['Fulfillment Days'] = (merged['GRN Date'] - merged['PO Date']).dt.days
    
    # Filter out missing dates
    valid_data = merged.dropna(subset=['Fulfillment Days']).copy()
    
    # Stats
    total_orders = valid_data['PO No'].nunique()
    total_grns = len(valid_data)
    
    if total_grns == 0:
        print("No matched PO-GRN pairs found.")
        return

    avg_days = valid_data['Fulfillment Days'].mean()
    median_days = valid_data['Fulfillment Days'].median()
    min_days = valid_data['Fulfillment Days'].min()
    max_days = valid_data['Fulfillment Days'].max()
    
    within_72h = valid_data[valid_data['Fulfillment Days'] <= 3]
    percent_within_72h = (len(within_72h) / total_grns) * 100
    
    print("\n" + "="*50)
    print("HAYAT KIMYA FULFILLMENT ANALYSIS")
    print("="*50)
    print(f"Vendor: {VENDOR_NAME}")
    print(f"Total POs analyzed: {total_orders}")
    print(f"Total GRN lines analyzed: {total_grns}")
    print("-" * 30)
    print(f"Average Fulfillment Time: {avg_days:.2f} days")
    print(f"Median Fulfillment Time: {median_days:.2f} days")
    print(f"Min Time: {min_days} days")
    print(f"Max Time: {max_days} days")
    print("-" * 30)
    print(f"GRNs within 72 hours (<= 3 days): {len(within_72h)} ({percent_within_72h:.1f}%)")
    print("="*50)
    
    print("\nDetailed Breakdown (first 20 rows):")
    cols = ['PO No', 'PO Date', 'GRN No', 'GRN Date', 'Fulfillment Days', 'Item Name']
    print(valid_data[cols].head(20).to_string())

    # Check for outliers
    long_delays = valid_data[valid_data['Fulfillment Days'] > 10].sort_values('Fulfillment Days', ascending=False)
    if not long_delays.empty:
        print("\nTop 10 Longest Delays:")
        print(long_delays[cols].head(10).to_string())

if __name__ == "__main__":
    analyze_fulfillment()
