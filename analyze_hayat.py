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
TARGET_SUPPLIER = 'HAYAT KIMYA  K  H PRODUCTS LTD'

def load_data():
    # Load Category Data
    category_dfs = {}
    for cat, filename in CATEGORY_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            category_dfs[cat] = pd.read_excel(path)
        else:
            print(f"Warning: {filename} not found.")

    # Load Granular Sales (JSON)
    json_path = os.path.join(DATA_DIR, SALES_JSON)
    with open(json_path, 'r') as f:
        sales_json_data = json.load(f)
    
    # Flatten JSON sales data for easier matching
    sales_list = []
    for item_name, metrics in sales_json_data.items():
        sales_list.append({
            'ITEM_NAME_CLEAN': item_name.strip().upper(),
            'DAILY_SALES': metrics.get('avg_daily_sales', 0),
            'TOTAL_SALES': metrics.get('total_10mo_sales', 0),
            'TREND': metrics.get('trend', 'stable')
        })
    sales_df = pd.DataFrame(sales_list)

    # Load Financial Data (Excel)
    fin_path = os.path.join(DATA_DIR, FINANCIAL_XLSX)
    fin_df = pd.read_excel(fin_path)
    fin_df['ITEM_NAME_CLEAN'] = fin_df['Item Name'].str.strip().str.upper()

    return category_dfs, sales_df, fin_df

def analyze():
    category_dfs, sales_df, fin_df = load_data()
    report_data = []

    for cat_name, df in category_dfs.items():
        # Clean names for matching
        df['ITEM_NAME_CLEAN'] = df['ITM_NAME'].str.strip().str.upper()
        
        # Merge with Sales JSON
        df = df.merge(sales_df, on='ITEM_NAME_CLEAN', how='left')
        
        # Merge with Financials (Margin etc)
        df = df.merge(fin_df[['ITEM_NAME_CLEAN', 'MARGIN %', 'QTY']], on='ITEM_NAME_CLEAN', how='left')
        
        # Fill missing values
        df['DAILY_SALES'] = df['DAILY_SALES'].fillna(0)
        df['TOTAL_SALES'] = df['TOTAL_SALES'].fillna(0)
        df['STOCK'] = df['STOCK'].fillna(0)
        df['SellPrice'] = df['SellPrice'].fillna(0)

        # Hayat vs Competitors
        hayat_mask = df['VENDOR_NAME'] == TARGET_SUPPLIER
        hayat_df = df[hayat_mask]
        comp_df = df[~hayat_mask]

        # Metrics
        cat_total_vol = df['TOTAL_SALES'].sum()
        hayat_vol = hayat_df['TOTAL_SALES'].sum()
        hayat_share = (hayat_vol / cat_total_vol * 100) if cat_total_vol > 0 else 0

        cat_total_stock = df['STOCK'].sum()
        hayat_stock = hayat_df['STOCK'].sum()
        
        avg_hayat_price = hayat_df['SellPrice'].mean()
        avg_comp_price = comp_df['SellPrice'].mean()

        report_data.append({
            'Category': cat_name,
            'Hayat SKUs': len(hayat_df),
            'Comp SKUs': len(comp_df),
            'Hayat Volume Share %': round(hayat_share, 2),
            'Hayat Total Stock': hayat_stock,
            'Comp Total Stock': cat_total_stock - hayat_stock,
            'Avg Hayat Price': round(avg_hayat_price, 2),
            'Avg Comp Price': round(avg_comp_price, 2),
            'Top Hayat SKU': hayat_df.sort_values('TOTAL_SALES', ascending=False).iloc[0]['ITM_NAME'] if not hayat_df.empty else 'N/A'
        })

    # Generate Report
    report_df = pd.DataFrame(report_data)
    print("\n" + "="*50)
    print("HAYAT KIMYA COMPETITIVENESS ANALYSIS")
    print("="*50)
    print(report_df.to_string(index=False))
    
    # Save to file
    report_df.to_csv('hayat_analysis_results.csv', index=False)
    
    with open('competitor_gap_analysis.md', 'w') as f:
        f.write("# Hayat Kimya Competitiveness Report 2025\n\n")
        f.write("## Executive Summary\n")
        f.write(f"Analyzed Hayat Kimya performance across {len(CATEGORY_FILES)} key departments using real-time SKU sales forecasting and stock data.\n\n")
        f.write("## Departmental Breakdown\n")
        f.write(report_df.to_markdown(index=False))
        f.write("\n\n## Key Insights\n")
        f.write("1. **Market Share**: Volume share reveals where Hayat is dominating or struggling.\n")
        f.write("2. **Inventory Positioning**: High stock with low volume share suggests potential overstocking or slow movers.\n")
        f.write("3. **Pricing Intelligence**: Relative pricing indicates if Hayat is positioned as a premium or economy brand in each category.\n")

if __name__ == "__main__":
    analyze()
