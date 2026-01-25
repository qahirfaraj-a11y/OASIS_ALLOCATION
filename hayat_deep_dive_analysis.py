import pandas as pd
import numpy as np

# Configuration
INPUT_FILE = 'cross_category_matching_report.csv'
OUTPUT_FILE = 'hayat_strategic_deep_dive.xlsx'
SUMMARY_FILE = 'hayat_strategic_summary.md'
TARGET_VENDOR = 'HAYAT KIMYA  K  H PRODUCTS LTD'

def analyze_hayat_deep_dive():
    print("Loading cross-category data...")
    try:
        df = pd.read_csv(INPUT_FILE)
    except FileNotFoundError:
        print(f"Error: {INPUT_FILE} not found. Please run the matching script first.")
        return

    # Filter Hayat vs Others
    # Note: Vendor names might need partial matching if inconsistent, but we cleaned them in the previous step
    hayat_mask = df['Vendor'] == TARGET_VENDOR
    hayat_df = df[hayat_mask].copy()
    competitors_df = df[~hayat_mask].copy()

    if hayat_df.empty:
        print(f"No data found for vendor: {TARGET_VENDOR}")
        return

    print(f"Found {len(hayat_df)} Hayat SKUs and {len(competitors_df)} Competitor SKUs.")

    writer = pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl')

    # ==========================================
    # View 1: Market Competitiveness
    # ==========================================
    print("Analyzing View 1: Market Competitiveness...")
    comp_view = []
    
    for cat in df['Category'].unique():
        cat_df = df[df['Category'] == cat]
        cat_hayat = cat_df[cat_df['Vendor'] == TARGET_VENDOR]
        cat_comp = cat_df[cat_df['Vendor'] != TARGET_VENDOR]

        total_vol = cat_df['Total 10Mo Sales'].sum()
        hayat_vol = cat_hayat['Total 10Mo Sales'].sum()
        
        avg_price_market = cat_df['Sell Price'].mean()
        avg_price_hayat = cat_hayat['Sell Price'].mean()
        
        # Price Index: Higher > 1.0 means more expensive than average
        price_index = (avg_price_hayat / avg_price_market) if avg_price_market > 0 else 0

        comp_view.append({
            'Category': cat,
            'Hayat Market Share (Vol)': hayat_vol / total_vol if total_vol > 0 else 0,
            'Hayat Avg Price': avg_price_hayat,
            'Market Avg Price': avg_price_market,
            'Price Index': price_index,
            'Competitiveness Status': 'Premium' if price_index > 1.1 else 'Economy' if price_index < 0.9 else 'Mid-Market'
        })
    
    pd.DataFrame(comp_view).to_excel(writer, sheet_name='1_Market_Competitiveness', index=False)

    # ==========================================
    # View 2: Profitability & Efficiency
    # ==========================================
    print("Analyzing View 2: Profitability & Efficiency...")
    # Focus on items where we have margin data
    prof_df = hayat_df.copy()
    prof_df['Margin Gap'] = prof_df['Margin % (Intel)'] - prof_df['Financial Margin %']
    prof_df['Cost Variance'] = prof_df['WAC'] - prof_df['LPP']
    
    # Flag potential issues
    prof_df['Efficiency Flag'] = 'OK'
    prof_df.loc[prof_df['Margin Gap'] > 0.05, 'Efficiency Flag'] = 'Underperforming vs Intel'
    prof_df.loc[prof_df['Financial Margin %'] < 0.15, 'Efficiency Flag'] = 'Low Margin Risk'
    
    cols_v2 = ['Category', 'Item Name', 'Sell Price', 'WAC', 'LPP', 'Financial Margin %', 'Margin % (Intel)', 'Margin Gap', 'Efficiency Flag']
    prof_df[cols_v2].sort_values('Financial Margin %').to_excel(writer, sheet_name='2_Profitability', index=False)

    # ==========================================
    # View 3: Supply Chain Reliability
    # ==========================================
    print("Analyzing View 3: Supply Chain Reliability...")
    # This is mostly at the vendor level, but since we are looking AT Hayat, we break it down by Category/SKU risk
    sc_df = hayat_df.copy()
    
    # Inferred Risk: High Expiry Returns vs Volume
    sc_df['Expiry Risk Ratio'] = 0.0
    # Avoid division by zero
    mask_sales = sc_df['Total 10Mo Sales'] > 0
    sc_df.loc[mask_sales, 'Expiry Risk Ratio'] = sc_df.loc[mask_sales, 'Expiry Returns (Val)'] / (sc_df.loc[mask_sales, 'Total 10Mo Sales'] * sc_df.loc[mask_sales, 'Sell Price'])

    cols_v3 = ['Category', 'Item Name', 'Supplier Risk Level', 'Quality Score', 'Order Frequency', 'Avg Gap Days', 'Expiry Returns (Val)', 'Expiry Risk Ratio']
    sc_df[cols_v3].sort_values('Expiry Risk Ratio', ascending=False).to_excel(writer, sheet_name='3_Supply_Chain', index=False)

    # ==========================================
    # View 4: Customer Preferences (Forecast)
    # ==========================================
    print("Analyzing View 4: Customer Preferences...")
    # Logic: Parse attributes from Item Name and aggregate sales
    
    def extract_attributes(row):
        name = row['Item Name'].upper()
        attrs = []
        if 'SENSITIVE' in name: attrs.append('Sensitive')
        if 'ALOE' in name: attrs.append('Aloe')
        if 'LAVENDER' in name: attrs.append('Lavender')
        if 'WINGS' in name: attrs.append('Wings')
        if 'PANT' in name: attrs.append('Pants')
        if 'JUMBO' in name: attrs.append('Jumbo Pack')
        return ', '.join(attrs) if attrs else 'Standard'

    pref_df = df.copy() # Look at WHOLE market for preferences, not just Hayat
    pref_df['Attributes'] = pref_df.apply(extract_attributes, axis=1)
    
    # Pivot to see sales by Attribute per Category
    pref_summary = pref_df.groupby(['Category', 'Attributes']).agg({
        'Total 10Mo Sales': 'sum',
        'Trend': lambda x: x.mode()[0] if not x.mode().empty else 'Unknown'
    }).reset_index()
    
    pref_summary = pref_summary.sort_values(['Category', 'Total 10Mo Sales'], ascending=False)
    pref_summary.to_excel(writer, sheet_name='4_Customer_Preferences', index=False)

    writer.close()
    
    # ==========================================
    # Generate Executive Summary Markdown
    # ==========================================
    with open(SUMMARY_FILE, 'w') as f:
        f.write("# Hayat Kimya Strategic Deep Dive\n\n")
        
        f.write("## 1. Market Competitiveness\n")
        f.write("How Hayat stacks up against the category average:\n\n")
        f.write(pd.DataFrame(comp_view).to_markdown(index=False))
        
        f.write("\n\n## 2. Profitability Gaps\n")
        low_margin = prof_df[prof_df['Financial Margin %'] < 0.15]
        f.write(f"- identified **{len(low_margin)} SKUs** with margins below 15%.\n")
        f.write("- **Recommendation**: Review procurement costs for these items immediately.\n\n")
        
        f.write("\n\n## 3. Supply Chain Risk\n")
        # Check overall risk level from the first row (since it's vendor level usually)
        risk_level = sc_df['Supplier Risk Level'].iloc[0] if not sc_df.empty else "Unknown"
        f.write(f"**Overall Supplier Risk**: {risk_level}\n")
        f.write("- **Expiry Analysis**: Items with high expiry returns relative to sales indicate over-ordering or poor shelf-life management.\n\n")
        
        f.write("\n\n## 4. Customer Preference Trends\n")
        f.write("Top attributes driving sales in each category:\n\n")
        # Get top attribute for each category
        top_attrs = pref_summary.loc[pref_summary.groupby('Category')['Total 10Mo Sales'].idxmax()]
        f.write(top_attrs.to_markdown(index=False))

    print(f"\nDeep dive complete!\n- Excel Analysis: {OUTPUT_FILE}\n- Strategic Summary: {SUMMARY_FILE}")

if __name__ == "__main__":
    analyze_hayat_deep_dive()
