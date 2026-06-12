import pandas as pd
import os
import glob

# Configuration
DATA_DIR = r"app/data"
OUTPUT_REPORT = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/maccuisine_strategic_analysis.md"

FILES = [
    "Honey.XLSX",
    "JAMS.XLSX",
    "Mayonnaise.XLSX",
    "Mustard.XLSX",
    "Tomato and Ketchuo.XLSX",
    "Cooking.XLSX"
]

def load_data():
    dfs = []
    for f in FILES:
        path = os.path.join(DATA_DIR, f)
        if os.path.exists(path):
            try:
                # Read header
                df = pd.read_excel(path)
                df.columns = df.columns.str.strip()
                # Add Dept Name from filename
                dept_name = f.replace('.XLSX', '').replace('.xlsx', '')
                df['Department'] = dept_name
                dfs.append(df)
            except Exception as e:
                print(f"Error loading {f}: {e}")
    
    if not dfs:
        return pd.DataFrame()
        
    return pd.concat(dfs, ignore_index=True)

def analyze_maccuisine():
    print("Loading Departmental Data...")
    df = load_data()
    
    if df.empty:
        print("No data loaded.")
        return

    # Cleaning
    # Ensure numeric
    df['STOCK'] = pd.to_numeric(df['STOCK'], errors='coerce').fillna(0)
    df['SellPrice'] = pd.to_numeric(df['SellPrice'], errors='coerce').fillna(0)
    
    # Calculate Estimated Stock Value (Revenue potential)
    df['Stock Value'] = df['STOCK'] * df['SellPrice']
    
    # 1. Internal Factor: Inventory & Capital
    total_stock_value = df['Stock Value'].sum()
    dept_value = df.groupby('Department')['Stock Value'].sum().sort_values(ascending=False)
    
    # 2. External Factor: Supplier Concentration (Supplier Intelligence)
    # Vendor Name might mean Brand or Distributor
    supplier_counts = df.groupby(['Department', 'VENDOR_NAME'])['BARCODE'].count().reset_index()
    supplier_counts.rename(columns={'BARCODE': 'SKU_Count'}, inplace=True)
    
    # Risk: Single Supplier Departments?
    dept_supplier_count = df.groupby('Department')['VENDOR_NAME'].nunique()
    single_source_risks = dept_supplier_count[dept_supplier_count == 1]
    
    # 3. Internal Factor: Pricing Structure
    price_stats = df.groupby('Department')['SellPrice'].agg(['mean', 'min', 'max']).reset_index()
    
    # Generate Report
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("# Maccuisine Strategic Department Analysis\n\n")
        
        f.write("> **Critical Observation**: The provided departmental files (Honey, Jams, etc.) contain *Current Stock* and *Price* data, but **no historical sales history** or **dates**. \n")
        f.write("> Therefore, \"Sales Forecasting\" in the traditional sense is impossible with this dataset. \n")
        f.write("> **Strategic Pivot**: We have analyzed **Stock Position**, **Supplier Dependency**, and **Pricing Architecture** instead.\n\n")
        
        # Section 1: Internal Factors
        f.write("## 1. Internal Factors: Inventory & Capital Exposure\n")
        f.write(f"**Total Capital Locked in Stock**: **KES {total_stock_value:,.2f}** (Estimated at Retail Value)\n\n")
        
        f.write("### Departmental Stock Value Split:\n")
        for dept, val in dept_value.items():
            perc = (val / total_stock_value) * 100
            f.write(f"- **{dept}**: KES {val:,.0f} ({perc:.1f}%)\n")
            
        f.write("\n**Strategic Insight**: \n")
        f.write(f"> \"{dept_value.index[0]} holds the highest inventory value. Efficient turnover here is critical for cash flow. Any 'Dead Stock' in this category is expensive real estate.\"\n\n")

        # Section 2: External Factors - Supplier Intelligence
        f.write("## 2. External Factors: Supplier Intelligence & Risk\n")
        
        if not single_source_risks.empty:
            f.write("### 🚨 Supply Chain Vulnerability (Single Sourcing)\n")
            f.write("The following departments appear to rely on a **single supplier**:\n")
            for dept in single_source_risks.index:
                vendor = df[df['Department'] == dept]['VENDOR_NAME'].iloc[0]
                f.write(f"- **{dept}**: Supplied solely by **{vendor}**.\n")
            f.write("\n**Strategic Recommendation**: \n")
            f.write("> \"We have a 'Single Point of Failure' risk in {', '.join(single_source_risks.index)}. If this supplier strikes or fails, we lose the entire category. We must immediately qualify a secondary backup supplier.\"\n\n")
        else:
             f.write("### Diverse Supply Base\n")
             f.write("Most departments have multiple suppliers, reducing risk.\n\n")
             
        # Section 3: Pricing & Profitability Proxies
        f.write("## 3. Pricing Strategy (Profitability Proxy)\n")
        f.write("Since we lack Cost Price (CP), we analyzed Sell Price bandwidth to understand our Market Positioning.\n\n")
        f.write("| Department | Avg Price | Min Price | Max Price | Positioning |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- |\n")
        
        for idx, row in price_stats.iterrows():
            spread = row['max'] - row['min']
            pos = "Wide Range (Mass + Premium)" if spread > 500 else "Niche/Focused"
            f.write(f"| {row['Department']} | {row['mean']:.0f} | {row['min']:.0f} | {row['max']:.0f} | {pos} |\n")
            
        f.write("\n**Strategic Insight**:\n")
        f.write("> \"Departments with wide price ranges (e.g. Cooking?) need clear merchandising segmentation so customers distinguish 'Budget' from 'Premium' options.\"\n")

if __name__ == "__main__":
    analyze_maccuisine()
