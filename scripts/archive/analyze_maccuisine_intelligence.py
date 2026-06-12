import pandas as pd
import json
import os
import glob

# Configuration
DATA_DIR = r"app/data"
OUTPUT_REPORT = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/maccuisine_intelligence_report.md"

DEPT_FILES = [
    "Honey.XLSX",
    "JAMS.XLSX",
    "Mayonnaise.XLSX",
    "Mustard.XLSX",
    "Tomato and Ketchuo.XLSX",
    "Cooking.XLSX"
]

JSON_FILES = {
    "forecast": r"app/data/sales_forecasting_2025 (1).json",
    "profit": r"app/data/sales_profitability_intelligence_2025_updated.json",
    "supplier": r"app/data/supplier_patterns_2025 (3).json"
}

def load_json(path):
    try:
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        print(f"Error loading {path}: {e}")
        return {}

def load_dept_files():
    dfs = []
    for f in DEPT_FILES:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            try:
                df = pd.read_excel(path)
                df.columns = df.columns.str.strip()
                df['Department'] = f.replace('.XLSX', '').replace('.xlsx', '')
                dfs.append(df)
            except Exception as e:
                print(f"Error loading {f}: {e}")
    if dfs:
        return pd.concat(dfs, ignore_index=True)
    return pd.DataFrame()

def normalize(text):
    return str(text).upper().strip()

def analyze_intelligence():
    print("Loading databases...")
    # Load and Normalize DB Keys
    forecast_raw = load_json(JSON_FILES["forecast"])
    forecast_db = {normalize(k): v for k, v in forecast_raw.items()}
    
    profit_raw = load_json(JSON_FILES["profit"])
    profit_db = {normalize(k): v for k, v in profit_raw.items()}
    
    supplier_raw = load_json(JSON_FILES["supplier"])
    supplier_db = {normalize(k): v for k, v in supplier_raw.items()}
    
    print("Loading departments...")
    df = load_dept_files()
    if df.empty:
        print("No departmental data found.")
        return

    # Initialize columns
    df['Forecast_Avg_Sales'] = 0.0
    df['Forecast_Trend'] = 'Unknown'
    df['Margin_Pct'] = 0.0
    df['Supplier_Reliability'] = 0.0
    df['Supplier_Lead_Time'] = 0.0
    df['Matched_Forecast'] = False
    
    hits_forecast = 0
    hits_profit = 0
    hits_supplier = 0
    
    print(f"Processing {len(df)} SKUs...")
    
    for idx, row in df.iterrows():
        item_name = normalize(row.get('ITM_NAME', ''))
        vendor_name = normalize(row.get('VENDOR_NAME', ''))
        
        # 1. FORECAST LOOKUP
        f_data = forecast_db.get(item_name)
        if f_data:
            df.at[idx, 'Forecast_Avg_Sales'] = f_data.get('avg_monthly_sales', 0)
            df.at[idx, 'Forecast_Trend'] = f_data.get('trend', 'Unknown')
            df.at[idx, 'Matched_Forecast'] = True
            hits_forecast += 1
            
        # 2. PROFITABILITY LOOKUP
        p_data = profit_db.get(item_name)
        if p_data:
            df.at[idx, 'Margin_Pct'] = p_data.get('margin_pct', 0)
            hits_profit += 1
            
        # 3. SUPPLIER PATTERNS
        s_data = supplier_db.get(vendor_name)
        # Fallback: substring match for supplier
        if not s_data:
             for k, v in supplier_db.items():
                 if vendor_name in k or k in vendor_name:
                     s_data = v
                     break
        
        if s_data:
            df.at[idx, 'Supplier_Reliability'] = s_data.get('reliability_score', 0) * 100
            df.at[idx, 'Supplier_Lead_Time'] = s_data.get('estimated_delivery_days', 0)
            hits_supplier += 1

    print(f"Matched {hits_forecast} items to Forecast DB.")
    print(f"Matched {hits_profit} items to Profitability DB.")
    print(f"Matched {hits_supplier} suppliers.")

    # SEGMENTATION Analysis
    dept_analysis = df.groupby('Department').agg({
        'Forecast_Avg_Sales': 'sum',
        'Margin_Pct': 'mean',
        'Supplier_Reliability': 'mean',
        'Supplier_Lead_Time': 'mean',
        'ITM_NAME': 'count'
    }).rename(columns={'ITM_NAME': 'SKU_Count'})
    
    # Calculate Coverage
    coverage = df.groupby('Department')['Matched_Forecast'].mean() * 100

    # High Potential Items
    growth_stars = df[ (df['Forecast_Trend'] == 'growing') & (df['Margin_Pct'] > 15) ]
    
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("# Maccuisine Intelligence Analysis: Internal & External Factors\n\n")
        f.write("**Data Integration Success**:\n")
        f.write(f"- Forecast Match Rate: {hits_forecast}/{len(df)} SKUs\n")
        f.write(f"- Supplier Match Rate: {hits_supplier}/{len(df)} SKUs\n\n")
        
        # 1. INTERNAL: Profitability & Forecasting
        f.write("## 1. Internal Factors: Sales & Profitability Structure\n")
        f.write("Using `Sales Forecasting 2025` and `Profitability Intelligence`, we analyzed the departments.\n\n")
        
        f.write("### Departmental Intelligence Matrix\n")
        f.write("| Department | Data Coverage | Est. Monthly Vol | Avg Margin % | Strategy |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- |\n")
        
        for dept, row in dept_analysis.iterrows():
            cov = coverage.get(dept, 0)
            vol = row['Forecast_Avg_Sales']
            margin = row['Margin_Pct']
            
            if margin > 20: strategy = "Profit Driver"
            elif vol > 100: strategy = "Volume Builder"
            else: strategy = "Watch List"
            
            f.write(f"| **{dept}** | {cov:.0f}% | {vol:.1f} Units | {margin:.1f}% | {strategy} |\n")
            
        f.write("\n### 🌟 'Star' SKUs (Internal Growth Engines)\n")
        if not growth_stars.empty:
            f.write("These items are trending **UP** and have healthy margins (>15%). **Action: Ensure 100% In-Stock.**\n")
            # Dedupe by Item Name
            unique_stars = growth_stars.drop_duplicates(subset=['ITM_NAME']).head(10)
            for _, row in unique_stars.iterrows():
                f.write(f"- **{row['ITM_NAME']}** ({row['Department']}): +{row['Forecast_Trend']} Trend, {row['Margin_Pct']:.1f}% Margin, Vol: {row['Forecast_Avg_Sales']:.1f}/mo\n")
        else:
            f.write("No 'Star' items identified with current mapping.\n")

        f.write("\n---\n")

        # 2. EXTERNAL: Supplier Intelligence
        f.write("## 2. External Factors: Supplier Reliability & Risk\n")
        f.write("Using `Supplier Patterns 2025`, we assessed supply chain risk.\n\n")
        
        f.write("| Department | Avg Reliability | Avg Lead Time (Days) | Risk Assessment |\n")
        f.write("| :--- | :--- | :--- | :--- |\n")
        
        for dept, row in dept_analysis.iterrows():
            rel = row['Supplier_Reliability']
            lt = row['Supplier_Lead_Time']
            
            if rel > 90: risk = "Low (Stable)"
            elif rel > 80: risk = "Medium (Buffer)"
            elif rel > 1: risk = "High (Volatile)"
            else: risk = "Unknown (No Data)"
            
            f.write(f"| **{dept}** | {rel:.1f}% | {lt:.1f} | {risk} |\n")
            
        f.write("\n> **Strategic Recommendation**: \n")
        f.write("> **External Factor Mitigation**: Departments with <85% reliability require a **Safety Stock Multiplier of 1.5x**. Do not rely on Just-In-Time for these categories.\n")
        
if __name__ == "__main__":
    analyze_intelligence()
