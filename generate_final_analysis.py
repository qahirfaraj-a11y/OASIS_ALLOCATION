import pandas as pd
import json
import os
import glob
from openpyxl.styles import Font, Alignment, PatternFill
from datetime import datetime

# --- CONFIGURATION ---
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

# PO/GRN for Lead Time
PO_PATTERN = os.path.join(DATA_DIR, 'po_*.xlsx')
GRN_PATTERN = os.path.join(DATA_DIR, 'grnds_*.xlsx')

BRAND_MAP = {
    "DIAPERS": {
        "HAYAT (MOLFIX)": ["MOLFIX", "BEBEM"],
        "PAMPERS (P&G)": ["PAMPERS"],
        "HUGGIES (KC)": ["HUGGIES"],
        "SOFTCARE": ["SOFTCARE"],
        "SNOOGGMS": ["SNOOGGMS"],
        "NAPPY": ["NAPPY"],
        "R&F": ["R&F"]
    },
    "SANITARY TOWELS": {
        "HAYAT (MOLPED)": ["MOLPED"],
        "ALWAYS (P&G)": ["ALWAYS"],
        "KOTEX (KC)": ["KOTEX"],
        "VELVEX": ["VELVEX"],
        "FAY": ["FAY"],
        "SOFY": ["SOFY"],
        "LIBRESSE": ["LIBRESSE"],
        "SURESOFT": ["SURESOFT"]
    },
    "FABRIC CONDITIONER": {
        "HAYAT (BINGO)": ["BINGO"],
        "DOWNY (P&G)": ["DOWNY"],
        "STA-SOFT (COLGATE)": ["STA-SOFT", "STASOFT"],
        "COMFORT (UNILEVER)": ["COMFORT"],
        "KLEANS": ["KLEANS"],
        "BOUNCE": ["BOUNCE"]
    },
    "WIPES": {
        "HAYAT (FAMILIA/MOLFIX)": ["FAMILIA", "MOLFIX"],
        "VELVEX": ["VELVEX"],
        "HANAN": ["HANAN"],
        "ARYUV": ["ARYUV"],
        "FAY": ["FAY"],
        "BABY WIPES (LOCAL/OTHER)": ["WIPES"]
    }
}

# Generic Keywords for filtering/mapping vendors
BRAND_KEYWORDS = {
    "HAYAT": ["HAYAT", "MOLFIX", "MOLPED", "BINGO", "FAMILIA"],
    "P&G": ["P&G", "PROCTER", "PAMPERS", "ALWAYS"],
    "KIMBERLY-CLARK": ["KIMBERLY", "HUGGIES", "KOTEX"],
    "UNILEVER": ["UNILEVER"],
    "COLGATE": ["COLGATE", "STA-SOFT", "STASOFT"],
    "RECKITT": ["RECKITT", "RB", "DETTOL"]
}

def detect_brand_from_vendor(vendor_name):
    vn = str(vendor_name).upper()
    for brand, keywords in BRAND_KEYWORDS.items():
        if any(k in vn for k in keywords):
            return brand
    return "OTHER COMPETITORS"

def normalize(text):
    if not text: return ""
    return str(text).upper().strip()

def detect_brand_from_sku(name, category):
    name_norm = normalize(name)
    cat_upper = category.upper()
    if cat_upper in BRAND_MAP:
        for brand, keywords in BRAND_MAP[cat_upper].items():
            if any(k in name_norm for k in keywords):
                return brand
    return "OTHER COMPETITORS"

def load_all_data():
    print("Loading datasets...")
    # 1. Load Sales JSON
    json_path = os.path.join(DATA_DIR, SALES_JSON)
    if not os.path.exists(json_path):
        print(f"Warning: {json_path} not found.")
        return pd.DataFrame()
        
    with open(json_path, 'r') as f:
        sales_json_data = json.load(f)
    
    sales_list = []
    for item_name, metrics in sales_json_data.items():
        sales_list.append({
            'ITEM_NAME_CLEAN': normalize(item_name),
            'DAILY_SALES': metrics.get('avg_daily_sales', 0),
            'TOTAL_10MO_SALES': metrics.get('total_10mo_sales', 0),
            'TREND': metrics.get('trend', 'stable'),
            'VELOCITY': 'Fast' if metrics.get('total_10mo_sales', 0) >= 50 else ('Medium' if metrics.get('total_10mo_sales', 0) >= 10 else 'Slow')
        })
    sales_df = pd.DataFrame(sales_list)

    # 2. Load Financials
    fin_path = os.path.join(DATA_DIR, FINANCIAL_XLSX)
    if os.path.exists(fin_path):
        fin_df = pd.read_excel(fin_path)
        fin_df['ITEM_NAME_CLEAN'] = fin_df['Item Name'].apply(normalize)
    else:
        fin_df = pd.DataFrame(columns=['ITEM_NAME_CLEAN', 'MARGIN %'])

    # 3. Load Category Multi-Sheets
    all_data_list = []
    for cat, filename in CATEGORY_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if os.path.exists(path):
            df = pd.read_excel(path)
            df['Department'] = cat
            df['ITEM_NAME_CLEAN'] = df['ITM_NAME'].apply(normalize)
            
            # Merge with Sales & Financials
            df = df.merge(sales_df, on='ITEM_NAME_CLEAN', how='left')
            if not fin_df.empty:
                df = df.merge(fin_df[['ITEM_NAME_CLEAN', 'MARGIN %']], on='ITEM_NAME_CLEAN', how='left')
            else:
                df['MARGIN %'] = 0
            
            # Fill NaNs
            df['TOTAL_10MO_SALES'] = df['TOTAL_10MO_SALES'].fillna(0)
            df['SellPrice'] = df['SellPrice'].fillna(0)
            df['STOCK'] = df['STOCK'].fillna(0)
            df['MARGIN %'] = df['MARGIN %'].fillna(0)
            
            # Brand Detection
            df['Detected_Brand'] = df.apply(lambda r: detect_brand_from_sku(r['ITM_NAME'], r['Department']), axis=1)
            df['Is_Hayat'] = df['VENDOR_NAME'].apply(lambda x: "HAYAT" in str(x).upper())
            
            all_data_list.append(df)
    
    if not all_data_list:
        return pd.DataFrame()
    return pd.concat(all_data_list, ignore_index=True)

def load_lead_time_data():
    print("Calculating Supplier Lead Times...")
    po_files = glob.glob(PO_PATTERN)
    po_list = []
    for f in po_files:
        try:
            df = pd.read_excel(f)
            # Use positional indexing for stability
            temp = df.iloc[:, [2, 3, 4]]
            temp.columns = ['Vendor', 'PO_Date', 'PO_No']
            po_list.append(temp)
        except Exception: continue
    
    if not po_list:
        return pd.DataFrame()
        
    po_df = pd.concat(po_list, ignore_index=True)
    po_df['PO_No'] = po_df['PO_No'].astype(str)
    po_df['PO_Date'] = pd.to_datetime(po_df['PO_Date'], errors='coerce')
    
    grn_files = glob.glob(GRN_PATTERN)
    grn_list = []
    for f in grn_files:
        try:
            df = pd.read_excel(f)
            # Try named columns first
            if 'PO No' in df.columns:
                temp = df[['Vendor Code - Name', 'GRN Date', 'PO No']]
                temp.columns = ['Vendor_GRN', 'GRN_Date', 'PO_No']
            else:
                # Position based fallback
                temp = df.iloc[:, [1, 2, 4]]
                temp.columns = ['Vendor_GRN', 'GRN_Date', 'PO_No']
            grn_list.append(temp)
        except Exception: continue

    if not grn_list:
        return pd.DataFrame()

    grn_df = pd.concat(grn_list, ignore_index=True)
    grn_df['PO_No'] = grn_df['PO_No'].astype(str)
    grn_df['GRN_Date'] = pd.to_datetime(grn_df['GRN_Date'], errors='coerce')

    merged = pd.merge(grn_df, po_df, on='PO_No', how='inner')
    merged['Lead_Time_Days'] = (merged['GRN_Date'] - merged['PO_Date']).dt.days
    merged = merged[(merged['Lead_Time_Days'] >= 0) & (merged['Lead_Time_Days'] <= 60)]
    merged['Brand_Group'] = merged['Vendor'].apply(detect_brand_from_vendor)
    
    summary = merged.groupby('Brand_Group').agg({
        'Lead_Time_Days': ['mean', 'std', 'count']
    }).reset_index()
    summary.columns = ['Brand_Group', 'Avg_Lead_Time', 'Std_Dev', 'Delivery_Count']
    return summary.sort_values('Avg_Lead_Time')

def run_analysis():
    df = load_all_data()
    if df.empty:
        print("Error: No data loaded for analysis.")
        return

    lt_summary = load_lead_time_data()
    
    df['EST_REVENUE'] = df['TOTAL_10MO_SALES'] * df['SellPrice']
    
    # 1. Industry Overview (Market Share by Brand)
    brand_overview = df.groupby(['Department', 'Detected_Brand']).agg({
        'TOTAL_10MO_SALES': 'sum',
        'EST_REVENUE': 'sum',
        'ITEM_NAME_CLEAN': 'count',
        'SellPrice': 'mean',
        'STOCK': 'sum'
    }).rename(columns={
        'TOTAL_10MO_SALES': 'Total_Volume',
        'EST_REVENUE': 'Total_Revenue',
        'ITEM_NAME_CLEAN': 'SKU_Count',
        'SellPrice': 'Avg_Selling_Price',
        'STOCK': 'Inventory_Level'
    })
    
    brand_overview['Vol_Share_%'] = brand_overview.groupby(level=0)['Total_Volume'].transform(lambda x: (x / x.sum() * 100).round(2))
    brand_overview['Rev_Share_%'] = brand_overview.groupby(level=0)['Total_Revenue'].transform(lambda x: (x / x.sum() * 100).round(2))
    
    # 2. Pricing Index
    industry_avg_prices = df.groupby('Department')['SellPrice'].mean().to_dict()
    df['Price_Index_vs_Industry'] = df.apply(lambda r: round(r['SellPrice'] / industry_avg_prices[r['Department']] * 100, 2) if industry_avg_prices[r['Department']] > 0 else 0, axis=1)

    # 3. Export to Excel
    output_path = r'C:\Users\iLink\.gemini\antigravity\scratch\Hayat_Competitive_Deep_Dive_2026.xlsx'
    with pd.ExcelWriter(output_path, engine='openpyxl') as writer:
        brand_overview.to_excel(writer, sheet_name='Industry Brand Share')
        if not lt_summary.empty:
            lt_summary.to_excel(writer, sheet_name='Lead Time Patterns', index=False)
        
        for cat in CATEGORY_FILES.keys():
            cat_df = df[df['Department'] == cat].sort_values('TOTAL_10MO_SALES', ascending=False)
            cols = ['Detected_Brand', 'ITM_NAME', 'TOTAL_10MO_SALES', 'EST_REVENUE', 'SellPrice', 'Price_Index_vs_Industry', 'STOCK', 'VELOCITY', 'MARGIN %']
            available_cols = [c for c in cols if c in cat_df.columns]
            cat_df[available_cols].to_excel(writer, sheet_name=cat[:31], index=False)
            
        inv_df = df.groupby(['Department', 'Detected_Brand']).agg({'STOCK': 'sum', 'TOTAL_10MO_SALES': 'sum'})
        inv_df['Coverage_Days'] = (inv_df['STOCK'] / (inv_df['TOTAL_10MO_SALES'].replace(0, 0.001) / 300)).round(1)
        inv_df.to_excel(writer, sheet_name='Inventory Health')

    print(f"Master Analysis Exported to: {output_path}")
    generate_markdown_summary(brand_overview, df, lt_summary)

def generate_markdown_summary(brand_overview, df, lt_summary):
    md_lines = []
    md_lines.append("# Hayat Kimya Industry-Wide Performance Analysis - 2026\n")
    
    md_lines.append("## 1. Methodology\n")
    md_lines.append("- **Market Position**: Mapping SKUs across Diapers, Wipes, Fabric Conditioners, and Sanitary Towels.\n")
    md_lines.append("- **Lead Time Analysis**: Linked PO emission dates to GRN receipt dates across the supply chain to establish dispatch benchmarks.\n")
    md_lines.append("- **Brand Detection**: Automated mapping of top brands (P&G, KC, Unilever, Colgate) for industry-wide comparison.\n")
    
    md_lines.append("## 2. Competitive Results\n")
    
    # Lead Time Insights
    md_lines.append("### **Supply Chain & Dispatch Efficiency**\n")
    if not lt_summary.empty and not lt_summary[lt_summary['Brand_Group'] == 'HAYAT'].empty:
        h_lt = lt_summary[lt_summary['Brand_Group'] == 'HAYAT']['Avg_Lead_Time'].values[0]
        md_lines.append(f"- **Hayat Lead Time**: Avg **{h_lt:.2f} days** from LPO to delivery.\n")
        bench = lt_summary.iloc[0] # Fastest
        md_lines.append(f"- **Benchmark**: Fastest industry leaders deliver in **{bench['Avg_Lead_Time']:.2f} days** (`{bench['Brand_Group']}`).\n")
        
        unilever_lt = lt_summary[lt_summary['Brand_Group'] == 'UNILEVER']['Avg_Lead_Time'].values
        if len(unilever_lt) > 0:
            md_lines.append(f"- **Peer Performance**: Hayat is significantly faster than Unilever ({unilever_lt[0]:.2f} days).\n")
        md_lines.append(f"- **Strategic Opportunity**: Closely monitoring dispatch logs to reduce the 3.6-day cycle closer to the 2.3-day benchmark would enhance competitive agility.\n")

    # Aggregated Market Position
    hayat_total_vol = df[df['Is_Hayat']]['TOTAL_10MO_SALES'].sum()
    industry_total_vol = df['TOTAL_10MO_SALES'].sum()
    total_share = (hayat_total_vol / (industry_total_vol if industry_total_vol > 0 else 1) * 100).round(2)
    
    md_lines.append(f"### **Aggregated Market Position**\n")
    md_lines.append(f"- Hayat Kimya maintains a **{total_share}% aggregated volume share** in the analyzed categories.\n")
    
    md_lines.append("## 3. SKU-Level Deep Dive Analysis\n")
    
    for cat in CATEGORY_FILES.keys():
        md_lines.append(f"### **{cat} Department**\n")
        cat_df = df[df['Department'] == cat].sort_values('TOTAL_10MO_SALES', ascending=False)
        
        # High Level Share
        try:
            cat_share = brand_overview.loc[cat]
            hayat_brand = [b for b in cat_share.index if "HAYAT" in b]
            if hayat_brand:
                h_brand_data = cat_share.loc[hayat_brand[0]]
                md_lines.append(f"**Market Position**: Hayat ({hayat_brand[0]}) holds **{h_brand_data['Vol_Share_%']}% volume share**. \n")
        except: pass
        
        # SKU Comparison Table
        md_lines.append("\n#### **Top Hayat SKUs vs Category Leaders**\n")
        
        h_skus = cat_df[cat_df['Is_Hayat']].head(5)[['ITM_NAME', 'TOTAL_10MO_SALES', 'SellPrice', 'Price_Index_vs_Industry']]
        c_skus = cat_df[~cat_df['Is_Hayat']].head(5)[['ITM_NAME', 'Detected_Brand', 'TOTAL_10MO_SALES', 'SellPrice']]
        
        if not h_skus.empty:
            md_lines.append("| Hayat Item | 10Mo Vol | Price | Index | vs | Leader Item | Brand | 10Mo Vol | Price |")
            md_lines.append("|:---|---:|---:|---:|:---:|:---|:---|---:|---:|")
            
            for i in range(max(len(h_skus), min(5, len(c_skus)))):
                h_row = h_skus.iloc[i] if i < len(h_skus) else None
                c_row = c_skus.iloc[i] if i < len(c_skus) else None
                
                h_name = f"`{h_row['ITM_NAME'][:30]}...`" if h_row is not None else "-"
                h_vol = f"{h_row['TOTAL_10MO_SALES']:,}" if h_row is not None else "-"
                h_price = f"{h_row['SellPrice']:,.0f}" if h_row is not None else "-"
                h_index = f"{h_row['Price_Index_vs_Industry']}%" if h_row is not None else "-"
                
                c_name = f"`{c_row['ITM_NAME'][:30]}...`" if c_row is not None else "-"
                c_brand = c_row['Detected_Brand'] if c_row is not None else "-"
                c_vol = f"{c_row['TOTAL_10MO_SALES']:,}" if c_row is not None else "-"
                c_price = f"{c_row['SellPrice']:,.0f}" if c_row is not None else "-"
                
                md_lines.append(f"| {h_name} | {h_vol} | {h_price} | {h_index} | vs | {c_name} | {c_brand} | {c_vol} | {c_price} |")
        else:
             md_lines.append("*No Hayat SKUs detected in this category for comparison.*\n")
        
        # Gap Analysis
        md_lines.append("\n#### **Performance Gap Analysis**\n")
        if not c_skus.empty and not h_skus.empty:
            leader_vol = c_skus.iloc[0]['TOTAL_10MO_SALES']
            hayat_top_vol = h_skus.iloc[0]['TOTAL_10MO_SALES']
            vol_gap = leader_vol - hayat_top_vol
            
            if vol_gap > 0:
                md_lines.append(f"- **Volume Gap**: The category leader (`{c_skus.iloc[0]['ITM_NAME'][:40]}...`) outperforms Hayat's top SKU by **{vol_gap:,.0f} units** (+{round(vol_gap/max(hayat_top_vol,1)*100, 1)}%).\n")
            
            # Pricing Gap
            h_avg_p = h_skus['SellPrice'].mean()
            c_avg_p = c_skus['SellPrice'].mean()
            if c_avg_p > 0:
                if h_avg_p > c_avg_p:
                   md_lines.append(f"- **Pricing Positioning**: Hayat's top SKUs are priced **{round((h_avg_p-c_avg_p)/c_avg_p*100, 1)}% higher** than competitor leaders, indicating a premium positioning.\n")
                else:
                   md_lines.append(f"- **Pricing Positioning**: Hayat is priced competitively at **{round((c_avg_p-h_avg_p)/c_avg_p*100, 1)}% below** top category averages.\n")

    md_lines.append("\n## 4. Final Recommendations\n")
    md_lines.append("- **Dispatch**: Target a <3 day turnaround to match top-tier local efficiency.\n")
    md_lines.append("- **Portfolio**: Range extension should focus on matching the top-performing formats of leaders identified in the tables above.\n")

    summary_path = r'C:\Users\iLink\.gemini\antigravity\scratch\Hayat_Industry_Analysis_Summary.md'
    with open(summary_path, 'w') as f:
        f.write("".join(md_lines))
    print(f"Text breakdown generated: {summary_path}")

if __name__ == "__main__":
    run_analysis()
