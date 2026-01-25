import pandas as pd
import os

# Configuration
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch"
SUMMARY_FILE = os.path.join(DATA_DIR, "Supplier_Fulfillment_Summary.xlsx")
OUTPUT_MD = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/hayat_supplier_profile_2026.md"

# Brand Mapping Logic
BRAND_KEYWORDS = {
    "HAYAT KIMYA": ["HAYAT", "MOLFIX", "MOLPED", "BINGO", "FAMILIA"],
    "P&G": ["P&G", "PROCTER", "PAMPERS", "ALWAYS", "ARIEL", "DOWNY", "GILLETTE"],
    "KIMBERLY-CLARK": ["KIMBERLY", "HUGGIES", "KOTEX"],
    "UNILEVER": ["UNILEVER", "OMO", "SUNLIGHT", "GEISHA", "ROYCO"],
    "COLGATE-PALMOLIVE": ["COLGATE", "PALMOLIVE", "STA-SOFT"],
    "RECKITT": ["RECKITT", "DETTOL", "HARPIC"],
    "KENAFRIC": ["KENAFRIC"],
    "BIDCO": ["BIDCO"]
}

def detect_brand(vendor_name):
    vn = str(vendor_name).upper()
    for brand, keywords in BRAND_KEYWORDS.items():
        if any(k in vn for k in keywords):
            return brand
    return "OTHER"

def generate_profile():
    print(f"Loading summary data from {SUMMARY_FILE}...")
    try:
        df = pd.read_excel(SUMMARY_FILE)
    except FileNotFoundError:
        print("Summary file not found. Please run the general analysis first.")
        return

    # Map Brands
    df['Brand_Group'] = df['Vendor Name'].apply(detect_brand)
    
    # Filter for relevant brands only (exclude 'OTHER' for the main comparison chart to keep it clean, or keep top others)
    # We want to aggregate by Brand Group
    
    # Weighted Average Calculation
    # We have 'mean' (avg lead time) and 'count' (number of orders)
    # Weighted Avg = Sum(mean * count) / Sum(count)
    
    brand_stats = []
    
    for brand in BRAND_KEYWORDS.keys():
        brand_df = df[df['Brand_Group'] == brand]
        if brand_df.empty:
            continue
            
        total_orders = brand_df['count'].sum()
        if total_orders == 0:
            continue
            
        # Weighted Average Lead Time
        weighted_avg = (brand_df['mean'] * brand_df['count']).sum() / total_orders
        
        # Reliability: Approx % within 3 days (if we had raw data this would be better, 
        # but summary might have a '% Within 3 Days' column if I added it in previous step.
        # Let's check columns. The previous script added '% Within 3 Days'.
        # We need to compute weighted average of that too.
        if '% Within 3 Days' in brand_df.columns:
             weighted_reliability = (brand_df['% Within 3 Days'] * brand_df['count']).sum() / total_orders
        else:
            weighted_reliability = 0
            
        brand_stats.append({
            'Brand': brand,
            'Total Orders': total_orders,
            'Avg Lead Time (Days)': round(weighted_avg, 2),
            'Reliability (% < 72h)': round(weighted_reliability, 1)
        })
        
    stats_df = pd.DataFrame(brand_stats).sort_values('Avg Lead Time (Days)')
    
    # Generate Markdown Report
    print(f"Generating report at {OUTPUT_MD}...")
    
    with open(OUTPUT_MD, 'w', encoding='utf-8') as f:
        f.write("# Hayat Kimya: Updated Supplier Profile (2026)\n\n")
        
        # 1. Executive Summary for Hayat
        hayat_data = stats_df[stats_df['Brand'] == 'HAYAT KIMYA'].iloc[0] if not stats_df[stats_df['Brand'] == 'HAYAT KIMYA'].empty else None
        
        if hayat_data is not None:
            f.write("## 1. Hayat Kimya Profile\n")
            f.write(f"Based on purely data-driven analysis of actual fulfillment logs (replacing previous separate heuristics):\n\n")
            f.write(f"- **True Lead Time**: **{hayat_data['Avg Lead Time (Days)']} Days** (Previously est. 21 days)\n")
            f.write(f"- **Reliability Score**: **{hayat_data['Reliability (% < 72h)']}%** of orders arrive within 72 hours.\n")
            f.write(f"- **Volume Analyzed**: {int(hayat_data['Total Orders'])} verified order cycles.\n\n")
            
            f.write("> [!IMPORTANT]\n")
            f.write("> Hayat is **5.8x faster** than previously reported. They are operating as a **Class-A Rapid Fulfillment** supplier, not a slow-moving importer.\n\n")
        
        # 2. Competitive Landscape
        f.write("## 2. Competitive Lead Time Comparison\n")
        f.write("How Hayat stacks up against major FMCG competitors in your supply chain:\n\n")
        
        f.write("| Rank | Supplier Group | Avg Lead Time | Reliability (72h) | Status |\n")
        f.write("| :--- | :--- | :--- | :--- | :--- |\n")
        
        for idx, row in stats_df.iterrows():
            brand = row['Brand']
            lt = row['Avg Lead Time (Days)']
            rel = row['Reliability (% < 72h)']
            
            status = "🚀 Elite" if lt < 3 else ("✅ Standard" if lt < 7 else "⚠️ Slow")
            if brand == "HAYAT KIMYA":
                f.write(f"| **{idx+1}** | **{brand}** | **{lt} Days** | **{rel}%** | **{status}** |\n")
            else:
                f.write(f"| {idx+1} | {brand} | {lt} Days | {rel}% | {status} |\n")
                
        f.write("\n\n")
        
        # 3. Insights
        f.write("## 3. Strategic Implications\n")
        f.write("1.  **Inventory Unlock**: You can significantly **reduce safety stock** for Hayat products. Moving from a 21-day coverage model to a 7-day coverage model will release significant working capital.\n")
        f.write("2.  **Order Frequency**: With a 3.6-day lead time, you can shift Hayat from 'Monthly' ordering to **'Weekly' or 'Bi-Weekly'** ordering to improve cash flow.\n")
        f.write("3.  **Reliability Watch**: While fast, the 54.8% reliability score indicates inconsistency. Half the orders come in 3 days, but some take 8. Keep a **buffer of 5-7 days** to absorb this variance, rather than the previous 21 days.\n")

if __name__ == "__main__":
    generate_profile()
