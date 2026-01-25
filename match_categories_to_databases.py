import pandas as pd
import json
import os
import sys

# Configuration
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
CATEGORIES = {
    'Diapers': 'diapers.xlsx',
    'Fabric Conditioner': 'fabricconditioner.xlsx',
    'Wipes': 'wipes.xlsx',
    'Sanitary Towels': 'sanitarytowels.xlsx'
}
DB_SALES = 'sales_forecasting_2025 (1).json'
DB_PROFIT = 'sales_profitability_intelligence_2025.json'
DB_FINANCE = 'topselqty.xlsx'
DB_RISK = 'supplier_quality_scores_2025 (1).json'
DB_PATTERNS = 'supplier_patterns_2025 (3).json'

def load_json(filename):
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        with open(path, 'r') as f:
            return json.load(f)
    return {}

def clean_name(name):
    return str(name).strip().upper() if not pd.isna(name) else ""

def run_matching():
    print("--- Loading Intelligence Databases ---")
    sales_db = load_json(DB_SALES)
    profit_db = load_json(DB_PROFIT)
    risk_db = load_json(DB_RISK)
    patterns_db = load_json(DB_PATTERNS)
    
    fin_path = os.path.join(DATA_DIR, DB_FINANCE)
    fin_df = pd.read_excel(fin_path) if os.path.exists(fin_path) else pd.DataFrame()
    if not fin_df.empty:
        fin_df['MATCH_KEY'] = fin_df['Item Name'].apply(clean_name)
        # Drop duplicates to avoid M:M merge issues if any
        fin_df = fin_df.drop_duplicates(subset=['MATCH_KEY'])

    all_data = []

    print("\n--- Processing Categories ---")
    for cat_name, filename in CATEGORIES.items():
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            print(f"Warning: {filename} not found.")
            continue
            
        print(f"Baking {cat_name}...")
        df = pd.read_excel(path)
        
        for _, row in df.iterrows():
            itm_name = str(row.get('ITM_NAME', ""))
            match_key = clean_name(itm_name)
            vendor_name = str(row.get('VENDOR_NAME', ""))
            vendor_key = clean_name(vendor_name)
            
            # --- 1. Product Sales Intelligence ---
            sales_info = sales_db.get(match_key, {})
            # --- 2. Product Profitability Intelligence ---
            profit_info = profit_db.get(match_key, {})
            # --- 3. Financial Database (topselqty) ---
            fin_info = {}
            if not fin_df.empty:
                fin_row = fin_df[fin_df['MATCH_KEY'] == match_key]
                if not fin_row.empty:
                    fin_info = fin_row.iloc[0].to_dict()
            
            # --- 4. Supplier Intelligence (Risk & Patterns) ---
            risk_info = risk_db.get(vendor_name, {}) # Risk DB uses full name as key
            pattern_info = patterns_db.get(vendor_name, {}) # Pattern DB uses full name as key

            # Build Consolidated Record
            record = {
                'Category': cat_name,
                'Vendor': vendor_name,
                'Barcode': row.get('BARCODE', ''),
                'Item Name': itm_name,
                'Current Stock': row.get('STOCK', 0),
                'Sell Price': row.get('SellPrice', 0),
                
                # Sales Metrics
                'Avg Daily Sales': sales_info.get('avg_daily_sales', 0),
                'Total 10Mo Sales': sales_info.get('total_10mo_sales', 0),
                'Trend': sales_info.get('trend', 'stable'),
                
                # Profitability Metrics
                'Margin % (Intel)': profit_info.get('margin_pct', 0),
                'Revenue (Intel)': profit_info.get('revenue', 0),
                'Sales Rank': profit_info.get('sales_rank', 'N/A'),
                
                # Financials (Direct)
                'WAC': fin_info.get('WAC', 0),
                'LPP': fin_info.get('LPP', 0),
                'Financial Margin %': fin_info.get('MARGIN %', 0),
                
                # Supplier Risk
                'Supplier Risk Level': risk_info.get('risk_level', 'Unknown'),
                'Quality Score': risk_info.get('quality_score', 0),
                'Expiry Returns (Val)': risk_info.get('total_value_returned', 0),
                
                # Supplier Patterns
                'Order Frequency': pattern_info.get('order_frequency', 'Unknown'),
                'Avg Gap Days': pattern_info.get('avg_gap_days', 0),
                'Reliability Score': pattern_info.get('reliability_score', 0)
            }
            all_data.append(record)

    # Convert to DataFrame
    report_df = pd.DataFrame(all_data)
    
    # Save Results
    excel_out = 'cross_category_matching_report.xlsx'
    report_df.to_excel(excel_out, index=False)
    
    csv_out = 'cross_category_matching_report.csv'
    report_df.to_csv(csv_out, index=False)

    # Generate Markdown Summary
    summary_path = 'cross_category_matching_summary.md'
    with open(summary_path, 'w') as f:
        f.write("# Cross-Category Database Matching Summary\n\n")
        f.write(f"Analyzed {len(all_data)} SKUs across {len(CATEGORIES)} categories matched against 5 O.A.S.I.S. intelligence databases.\n\n")
        
        f.write("## Matching Statistics by Category\n")
        stats = report_df.groupby('Category').agg({
            'Item Name': 'count',
            'Avg Daily Sales': lambda x: (x > 0).sum(),
            'Margin % (Intel)': lambda x: (x > 0).sum(),
            'Supplier Risk Level': lambda x: (x != 'Unknown').sum()
        }).rename(columns={
            'Item Name': 'Total SKUs',
            'Avg Daily Sales': 'Sales Data Found',
            'Margin % (Intel)': 'Margin Data Found',
            'Supplier Risk Level': 'Supplier Data Found'
        })
        f.write(stats.to_markdown())
        f.write("\n\n## Top Performing SKUs (by Volumne)\n")
        top_skus = report_df.sort_values('Total 10Mo Sales', ascending=False).head(20)[['Item Name', 'Category', 'Total 10Mo Sales', 'Margin % (Intel)', 'Supplier Risk Level']]
        f.write(top_skus.to_markdown(index=False))
        
        f.write("\n\n## Data Gaps (Zero Sales or Unknown Supplier Risk)\n")
        f.write(f"- SKUs with No Sales Data: {len(report_df[report_df['Avg Daily Sales'] == 0])}\n")
        f.write(f"- SKUs with Unknown Supplier Risk: {len(report_df[report_df['Supplier Risk Level'] == 'Unknown'])}\n")

    print(f"\nMatching complete!\n- Excel: {excel_out}\n- Summary: {summary_path}")

if __name__ == "__main__":
    run_matching()
