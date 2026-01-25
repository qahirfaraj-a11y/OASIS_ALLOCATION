import pandas as pd
import json
import os

# Configuration
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
CATEGORY_FILES = {
    'Diapers': 'diapers.xlsx',
    'Wipes': 'wipes.xlsx',
    'Fabric Conditioner': 'fabricconditioner.xlsx',
    'Sanitary Towels': 'sanitarytowels.xlsx'
}
SALES_JSON = 'sales_forecasting_2025 (1).json'
FINANCIAL_XLSX = 'topselqty.xlsx'
HAYAT_VENDOR = 'HAYAT KIMYA  K  H PRODUCTS LTD'

def load_data():
    # Load Category Data
    full_data_list = []
    for cat, filename in CATEGORY_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            df = pd.read_excel(path)
            df['Department'] = cat
            full_data_list.append(df)
    
    all_skus_df = pd.concat(full_data_list, ignore_index=True)

    # Load Granular Sales (JSON)
    json_path = os.path.join(DATA_DIR, SALES_JSON)
    with open(json_path, 'r') as f:
        sales_json_data = json.load(f)
    
    sales_list = []
    for item_name, metrics in sales_json_data.items():
        sales_list.append({
            'ITEM_NAME_CLEAN': item_name.strip().upper(),
            'DAILY_SALES': metrics.get('avg_daily_sales', 0),
            'TOTAL_10MO_SALES': metrics.get('total_10mo_sales', 0),
            'TREND': metrics.get('trend', 'stable')
        })
    sales_df = pd.DataFrame(sales_list)

    return all_skus_df, sales_df

def run_full_market_analysis():
    print("Initializing Full Market Competitor Analysis...")
    all_skus, sales_df = load_data()
    
    # 1. Clean and Merge
    all_skus['ITEM_NAME_CLEAN'] = all_skus['ITM_NAME'].str.strip().str.upper()
    all_skus = all_skus.merge(sales_df, on='ITEM_NAME_CLEAN', how='left')
    
    # Fill missing metrics
    all_skus['DAILY_SALES'] = all_skus['DAILY_SALES'].fillna(0)
    all_skus['TOTAL_10MO_SALES'] = all_skus['TOTAL_10MO_SALES'].fillna(0)
    all_skus['SellPrice'] = all_skus['SellPrice'].fillna(0)
    all_skus['STOCK'] = all_skus['STOCK'].fillna(0)
    
    # Calculate Revenue approximation
    all_skus['EST_REVENUE'] = all_skus['TOTAL_10MO_SALES'] * all_skus['SellPrice']
    
    # 2. Vendor Summary (High Level Decision Sheet)
    vendor_summary = all_skus.groupby('VENDOR_NAME').agg({
        'ITM_NAME': 'count',
        'TOTAL_10MO_SALES': 'sum',
        'EST_REVENUE': 'sum',
        'SellPrice': 'mean',
        'STOCK': 'sum'
    }).rename(columns={
        'ITM_NAME': 'SKU_Count',
        'TOTAL_10MO_SALES': 'Total_Volume',
        'EST_REVENUE': 'Total_Revenue',
        'SellPrice': 'Avg_Price',
        'STOCK': 'Total_Stock'
    })
    
    # Add Market Share
    total_market_vol = vendor_summary['Total_Volume'].sum()
    total_market_rev = vendor_summary['Total_Revenue'].sum()
    
    vendor_summary['Volume_Share_%'] = (vendor_summary['Total_Volume'] / total_market_vol * 100).round(2)
    vendor_summary['Revenue_Share_%'] = (vendor_summary['Total_Revenue'] / total_market_rev * 100).round(2)
    
    # Sort by Revenue Leader
    vendor_summary = vendor_summary.sort_values('Total_Revenue', ascending=False)
    
    # 3. Departmental Share
    dept_summary = all_skus.groupby(['Department', 'VENDOR_NAME']).agg({
        'TOTAL_10MO_SALES': 'sum',
        'EST_REVENUE': 'sum'
    })
    dept_total = all_skus.groupby('Department').agg({
        'TOTAL_10MO_SALES': 'sum',
        'EST_REVENUE': 'sum'
    }).rename(columns={'TOTAL_10MO_SALES': 'Dept_Total_Vol', 'EST_REVENUE': 'Dept_Total_Rev'})
    
    dept_summary = dept_summary.join(dept_total)
    dept_summary['Dept_Volume_Share_%'] = (dept_summary['TOTAL_10MO_SALES'] / dept_summary['Dept_Total_Vol'] * 100).round(2)
    
    # 4. Top SKU Matrix
    top_skus = all_skus.sort_values(['Department', 'TOTAL_10MO_SALES'], ascending=[True, False])
    sku_matrix = top_skus[['Department', 'VENDOR_NAME', 'ITM_NAME', 'TOTAL_10MO_SALES', 'SellPrice', 'STOCK', 'TREND', 'EST_REVENUE']]
    
    # 5. Export to Master Excel
    output_path = 'market_competitiveness_master.xlsx'
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        vendor_summary.to_excel(writer, sheet_name='Vendor Performance')
        dept_summary.to_excel(writer, sheet_name='Departmental Share')
        sku_matrix.to_excel(writer, sheet_name='SKU Deep Dive', index=False)
        
    print(f"Full Market Analysis Exported: {output_path}")

if __name__ == "__main__":
    run_full_market_analysis()
