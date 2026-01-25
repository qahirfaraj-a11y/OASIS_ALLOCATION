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
            'TREND': metrics.get('trend', 'stable'),
            'MONTH_ACTIVE': metrics.get('months_active', 0)
        })
    sales_df = pd.DataFrame(sales_list)

    # Load Financial Data (Excel)
    fin_path = os.path.join(DATA_DIR, FINANCIAL_XLSX)
    fin_df = pd.read_excel(fin_path)
    fin_df['ITEM_NAME_CLEAN'] = fin_df['Item Name'].str.strip().str.upper()

    return category_dfs, sales_df, fin_df

def deep_dive_analysis():
    category_dfs, sales_df, fin_df = load_data()
    full_deep_dive = []

    for cat_name, df in category_dfs.items():
        # Clean names for matching
        df['ITEM_NAME_CLEAN'] = df['ITM_NAME'].str.strip().str.upper()
        
        # Merge with Sales JSON
        df = df.merge(sales_df, on='ITEM_NAME_CLEAN', how='left')
        
        # Merge with Financials
        df = df.merge(fin_df[['ITEM_NAME_CLEAN', 'MARGIN %']], on='ITEM_NAME_CLEAN', how='left')
        
        df['DAILY_SALES'] = df['DAILY_SALES'].fillna(0)
        df['TOTAL_SALES'] = df['TOTAL_SALES'].fillna(0)
        df['STOCK'] = df['STOCK'].fillna(0)
        df['SellPrice'] = df['SellPrice'].fillna(0)
        df['Category'] = cat_name

        full_deep_dive.append(df)

    # Combine all categories
    all_skus = pd.concat(full_deep_dive, ignore_index=True)
    
    # Identify Top Competitors by total category volume
    competitors = all_skus[all_skus['VENDOR_NAME'] != TARGET_SUPPLIER]
    top_competitors = competitors.groupby('VENDOR_NAME')['TOTAL_SALES'].sum().sort_values(ascending=False).head(10)
    
    # SKU Level Ranking
    all_skus['Rank_in_Category'] = all_skus.groupby('Category')['TOTAL_SALES'].rank(ascending=False, method='min')
    
    hayat_skus = all_skus[all_skus['VENDOR_NAME'] == TARGET_SUPPLIER].sort_values(['Category', 'TOTAL_SALES'], ascending=[True, False])
    
    # Comparative SKU Analysis
    report_lines = []
    report_lines.append("# Hayat Kimya SKU-Level Deep Dive Analysis\n")
    
    for cat in CATEGORY_FILES.keys():
        cat_df = all_skus[all_skus['Category'] == cat]
        hayat_cat = hayat_skus[hayat_skus['Category'] == cat]
        comp_cat = cat_df[cat_df['VENDOR_NAME'] != TARGET_SUPPLIER].sort_values('TOTAL_SALES', ascending=False)
        
        report_lines.append(f"## {cat} Department\n")
        
        # Top 5 Hayat vs Top 5 Competitors
        report_lines.append("### Top 5 Hayat SKUs vs Category Leaders\n")
        
        top_h = hayat_cat.head(5)[['ITM_NAME', 'TOTAL_SALES', 'SellPrice', 'STOCK', 'Rank_in_Category']]
        top_c = comp_cat.head(5)[['ITM_NAME', 'VENDOR_NAME', 'TOTAL_SALES', 'SellPrice', 'STOCK']]
        
        report_lines.append("#### Hayat Lead SKUs")
        report_lines.append(top_h.to_markdown(index=False))
        report_lines.append("\n#### Category Leaders (Competitors)")
        report_lines.append(top_c.to_markdown(index=False))
        
        # Price Sensitivity Analysis
        h_price = top_h['SellPrice'].mean()
        c_price = top_c['SellPrice'].mean()
        price_diff = ((h_price - c_price) / c_price * 100) if c_price > 0 else 0
        
        report_lines.append(f"\n**Pricing Positioning**: Hayat's lead SKUs are priced **{round(abs(price_diff), 1)}% {'higher' if price_diff > 0 else 'lower'}** than top competitor leads.\n")
        
        # Opportunity Gap
        # Find competitor SKUs with high volume where Hayat has low relative stock or no equivalent
        if not top_c.empty:
            leader_sku = top_c.iloc[0]['ITM_NAME']
            leader_vol = top_c.iloc[0]['TOTAL_SALES']
            report_lines.append(f"**Competitor Dominance**: The category leader is `{leader_sku}` with {leader_vol} units. Hayat's closest rival is `{top_h.iloc[0]['ITM_NAME'] if not top_h.empty else 'N/A'}`.\n")

    # Overall Top Competitors Table
    report_lines.append("## Market Landscape: Top Competitor Groups\n")
    report_lines.append(top_competitors.to_frame(name='Total 10Mo Volume').to_markdown())
    
    with open('hayat_sku_deep_dive.md', 'w') as f:
        f.write("\n".join(report_lines))
    
    print("SKU Deep Dive Report generated: hayat_sku_deep_dive.md")

if __name__ == "__main__":
    deep_dive_analysis()
