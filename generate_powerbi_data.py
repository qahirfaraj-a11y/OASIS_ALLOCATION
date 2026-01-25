import pandas as pd
import numpy as np

# Configuration
INPUT_FILE = 'cross_category_matching_report.csv'
OUTPUT_ITEM_FILE = 'powerbi_item_analysis.csv'
OUTPUT_SUPPLIER_FILE = 'powerbi_supplier_scorecard.csv'

def generate_powerbi_data():
    print("Loading raw matching data...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Please run the matching script first.")
        return

    print(f"Processing {len(df)} global SKUs...")

    # ==========================================
    # 1. Item-Wise Analysis Table
    # ==========================================
    item_df = df.copy()

    # --- Feature Extraction ---
    # Extract Attributes from Name (e.g. Scent, Size, Type)
    def get_attributes(name):
        name = str(name).upper()
        attrs = []
        if 'SENSITIVE' in name: attrs.append('Sensitive')
        if 'ALOE' in name: attrs.append('Aloe')
        if 'LAVENDER' in name: attrs.append('Lavender')
        if 'WINGS' in name: attrs.append('Wings')
        if 'PANT' in name: attrs.append('Pants')
        if 'JUMBO' in name: attrs.append('Jumbo')
        if 'TWIN' in name: attrs.append('Twin Pack')
        return ', '.join(attrs) if attrs else 'Standard'

    item_df['Product_Attributes'] = item_df['Item Name'].apply(get_attributes)

    # --- Calculated Metrics ---
    # Global Category Averages for Benchmarking
    cat_avg_price = item_df.groupby('Category')['Sell Price'].transform('mean')
    item_df['Market_Avg_Price'] = cat_avg_price
    
    # Price Index (1.0 = Average, >1.0 = Expensive)
    item_df['Price_Index'] = item_df['Sell Price'] / cat_avg_price.replace(0, 1)

    # Financial Gaps
    item_df['Margin_Gap'] = item_df['Margin % (Intel)'] - item_df['Financial Margin %']
    item_df['Cost_Variance'] = item_df['WAC'] - item_df['LPP']

    # Supply Chain Risk Ratio
    # (Expiry Returns / Revenue)
    revenue_est = item_df['Total 10Mo Sales'] * item_df['Sell Price']
    item_df['Expiry_Risk_Ratio'] = 0.0
    mask_rev = revenue_est > 0
    item_df.loc[mask_rev, 'Expiry_Risk_Ratio'] = item_df.loc[mask_rev, 'Expiry Returns (Val)'] / revenue_est.loc[mask_rev]

    # Clean up columns for PowerBI (snake_case preferences usually, but keeping readable keys is fine if consistent)
    # Let's map to PowerBI friendly names
    pbi_item_cols = {
        'Category': 'Category',
        'Vendor': 'Vendor_Name',
        'Item Name': 'Item_Name',
        'Barcode': 'Barcode',
        'Sell Price': 'Sell_Price',
        'Current Stock': 'Current_Stock',
        'Total 10Mo Sales': 'Sales_Volume_10Mo',
        'Avg Daily Sales': 'Daily_Sales_Velocity',
        'Trend': 'Sales_Trend',
        'Margin % (Intel)': 'Target_Margin_Pct',
        'Financial Margin %': 'Actual_Margin_Pct',
        'Supplier Risk Level': 'Supplier_Risk_Label',
        'Quality Score': 'Quality_Score',
        'Product_Attributes': 'Product_Attributes',
        'Price_Index': 'Price_Index',
        'Market_Avg_Price': 'Market_Avg_Price',
        'Expiry_Risk_Ratio': 'Expiry_Risk_Ratio'
    }
    
    # Select and Rename
    final_item_df = item_df.rename(columns=pbi_item_cols)[list(pbi_item_cols.values())]
    
    # Handling missing values for visuals
    final_item_df.fillna(0, inplace=True) 
    # Revert string columns from 0 to "Unknown" if needed, but fillna(0) is mostly for metrics. 
    # Let's fix specific string cols
    str_cols = ['Vendor_Name', 'Item_Name', 'Category', 'Sales_Trend', 'Supplier_Risk_Label', 'Product_Attributes']
    for c in str_cols:
        final_item_df[c] = final_item_df[c].replace(0, 'Unknown')

    print(f"Exporting Item Analysis: {len(final_item_df)} rows.")
    final_item_df.to_csv(OUTPUT_ITEM_FILE, index=False)


    # ==========================================
    # 2. Supplier Scorecard Table
    # ==========================================
    print("Generating Supplier Scorecard...")
    
    # Aggregations
    supp_grp = item_df.groupby('Vendor').agg({
        'Category': lambda x: x.mode()[0] if not x.mode().empty else 'Mixed',
        'Total 10Mo Sales': 'sum',
        'Item Name': 'count',
        'Quality Score': 'mean',
        'Reliability Score': 'mean',
        'Expiry Returns (Val)': 'sum'
    }).reset_index()

    supp_grp.rename(columns={
        'Vendor': 'Vendor_Name',
        'Category': 'Primary_Category',
        'Total 10Mo Sales': 'Total_Volume',
        'Item Name': 'SKU_Count',
        'Quality Score': 'Avg_Quality_Score',
        'Reliability Score': 'Avg_Reliability_Score',
        'Expiry Returns (Val)': 'Total_Expiry_Returns'
    }, inplace=True)

    # Calculate Share of Wallet (Volume Share) per Supplier within their Primary Category
    # First get total vol per category
    cat_vols = item_df.groupby('Category')['Total 10Mo Sales'].sum().to_dict()
    
    def get_market_share(row):
        cat = row['Primary_Category']
        total = cat_vols.get(cat, 1) # avoid div/0
        return row['Total_Volume'] / total

    supp_grp['Category_Market_Share'] = supp_grp.apply(get_market_share, axis=1)

    print(f"Exporting Supplier Scorecard: {len(supp_grp)} suppliers.")
    supp_grp.to_csv(OUTPUT_SUPPLIER_FILE, index=False)

    print("\nPowerBI Generation Complete!")
    print(f"- Items: {OUTPUT_ITEM_FILE}")
    print(f"- Suppliers: {OUTPUT_SUPPLIER_FILE}")

if __name__ == "__main__":
    generate_powerbi_data()
