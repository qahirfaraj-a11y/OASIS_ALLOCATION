import pandas as pd
import os

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
DEPT_FILES = [
    "dept_1_50.xlsx",
    "dept_51_100.xlsx",
    "dept_101_150.xlsx",
    "dept_151_200.xlsx",
    "dept_201_250.xlsx",
    "dept_301_350.xlsx"
]

# Excluded list (from previous scripts)
EXCLUDED_KEYWORDS = ["NO GRN"]

def main():
    print("Calculating Total Stock Asset Valuation...")
    all_data = []

    for filename in DEPT_FILES:
        fpath = os.path.join(DATA_DIR, filename)
        if not os.path.exists(fpath):
            print(f"Warning: File {filename} not found.")
            continue
            
        try:
            print(f"Processing {filename}...")
            df = pd.read_excel(fpath)
            
            # Ensure required columns exist
            if 'STOCK' in df.columns and 'SellPrice' in df.columns and 'VENDOR_NAME' in df.columns:
                # Filter out NO GRN suppliers
                df = df[~df['VENDOR_NAME'].str.contains("NO GRN", na=False, case=False)]
                
                # Calculate row-wise asset value
                # Using SellPrice for valuation (Standard Retail practice)
                df['Asset_Value'] = df['STOCK'] * df['SellPrice']
                
                # Keep relevant columns for aggregation
                cols_to_keep = ['DEPARTMENT', 'Asset_Value', 'STOCK']
                all_data.append(df[cols_to_keep])
            else:
                print(f"  Missing columns in {filename}. Columns found: {df.columns.tolist()}")
        except Exception as e:
            print(f"  Error processing {filename}: {e}")

    if not all_data:
        print("No valid data processed.")
        return

    master_df = pd.concat(all_data, ignore_index=True)
    
    # Departmental Summary
    dept_summary = master_df.groupby('DEPARTMENT')['Asset_Value'].sum().reset_index()
    dept_summary = dept_summary.sort_values(by='Asset_Value', ascending=False)
    
    total_assets = master_df['Asset_Value'].sum()
    total_items = master_df['STOCK'].sum()
    
    print("\n--- ASSET VALUATION SUMMARY ---")
    print(f"Total Unique Mapped Items: {len(master_df)}")
    print(f"Total Units in Stock: {total_items:,.0f}")
    print(f"Grand Total Asset Value: {total_assets:,.2f}")
    
    print("\nDepartmental Asset Breakdown:")
    print(dept_summary.to_markdown(index=False))
    
    # Store Categorization Suggester
    print("\n--- STORE CATEGORIZATION SUGGESTION ---")
    if total_assets < 1000000:
        print(f"Category: SMALL STORE (Current: {total_assets:,.2f} < 1M)")
    elif total_assets < 5000000:
        print(f"Category: MEDIUM STORE (Current: {total_assets:,.2f} < 5M)")
    else:
        print(f"Category: LARGE STORE (Current: {total_assets:,.2f} > 5M)")

    # Save summary to CSV for user
    dept_summary.to_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\asset_valuation_summary.csv", index=False)
    print(f"\nSummary saved to asset_valuation_summary.csv")

if __name__ == "__main__":
    main()
