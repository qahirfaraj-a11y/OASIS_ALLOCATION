import pandas as pd
import os
import glob
from datetime import datetime

# --- CONFIGURATION ---
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
PO_PATTERN = os.path.join(DATA_DIR, 'po_*.xlsx')
GRN_PATTERN = os.path.join(DATA_DIR, 'grnds_*.xlsx')
HAYAT_VENDOR_SEARCH = 'HAYAT KIMYA'

# Reuse Brand Detection Logic (Simplified for Lead Time)
BRAND_KEYWORDS = {
    "HAYAT": ["HAYAT", "MOLFIX", "MOLPED", "BINGO", "FAMILIA"],
    "P&G (PAMPERS/ALWAYS)": ["P&G", "PROCTER", "PAMPERS", "ALWAYS"],
    "KIMBERLY-CLARK (HUGGIES/KOTEX)": ["KIMBERLY", "HUGGIES", "KOTEX"],
    "UNILEVER": ["UNILEVER"],
    "COLGATE": ["COLGATE", "STA-SOFT", "STASOFT"],
    "RECKITT": ["RECKITT", "RB", "DETTOL"],
    "LOCAL/OTHER": []
}

def detect_brand_from_vendor(vendor_name):
    vn = str(vendor_name).upper()
    for brand, keywords in BRAND_KEYWORDS.items():
        if any(k in vn for k in keywords):
            return brand
    return "LOCAL/OTHER COMPETITORS"

def analyze_lead_times():
    print("Loading PO Data...")
    po_files = glob.glob(PO_PATTERN)
    po_list = []
    for f in po_files:
        try:
            df = pd.read_excel(f)
            # Standardize columns based on inspection
            # Headers: {'raisedbyorgcode/name': 0, 'fororgcode/name': 1, 'vendorcode/name': 2, 'podate': 3, 'pono': 4 ...}
            temp = df.iloc[:, [2, 3, 4]]
            temp.columns = ['Vendor', 'PO_Date', 'PO_No']
            po_list.append(temp)
        except Exception as e:
            print(f"Error reading {f}: {e}")
    
    po_df = pd.concat(po_list, ignore_index=True)
    po_df['PO_No'] = po_df['PO_No'].astype(str)
    po_df['PO_Date'] = pd.to_datetime(po_df['PO_Date'], errors='coerce')
    
    print("Loading GRN Data...")
    grn_files = glob.glob(GRN_PATTERN)
    grn_list = []
    for f in grn_files:
        try:
            # grnds headers: ('Org Code - Name', 'Vendor Code - Name', 'GRN Date', 'GRN No', 'PO No', ...)
            df = pd.read_excel(f)
            temp = df[['Vendor Code - Name', 'GRN Date', 'PO No', 'GRN No']]
            temp.columns = ['Vendor_GRN', 'GRN_Date', 'PO_No', 'GRN_No']
            grn_list.append(temp)
        except Exception as e:
            # Fallback if headers differ
            print(f"Header warning in {f}, attempting positional load...")
            try:
                df = pd.read_excel(f)
                temp = df.iloc[:, [1, 2, 4, 3]]
                temp.columns = ['Vendor_GRN', 'GRN_Date', 'PO_No', 'GRN_No']
                grn_list.append(temp)
            except:
                print(f"Failed to load {f}")

    grn_df = pd.concat(grn_list, ignore_index=True)
    grn_df['PO_No'] = grn_df['PO_No'].astype(str)
    grn_df['GRN_Date'] = pd.to_datetime(grn_df['GRN_Date'], errors='coerce')

    print("Merging PO and GRN Data...")
    # Link by PO Number
    merged = pd.merge(grn_df, po_df, on='PO_No', how='inner')
    
    # Calculate Lead Time
    merged['Lead_Time_Days'] = (merged['GRN_Date'] - merged['PO_Date']).dt.days
    
    # Filter out invalid lead times (negative or extremely high outliers)
    merged = merged[(merged['Lead_Time_Days'] >= 0) & (merged['Lead_Time_Days'] <= 60)]
    
    # Detect Brands
    merged['Brand_Group'] = merged['Vendor'].apply(detect_brand_from_vendor)

    print("Aggregating Results...")
    lead_time_summary = merged.groupby('Brand_Group').agg({
        'Lead_Time_Days': ['mean', 'std', 'count'],
        'GRN_No': 'count'
    }).reset_index()
    
    lead_time_summary.columns = ['Brand_Group', 'Avg_Lead_Time', 'Std_Dev', 'Delivery_Count', 'Total_GRNs']
    lead_time_summary = lead_time_summary.sort_values('Avg_Lead_Time')

    # Output to File
    output_path = r'C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Lead_Time_Analysis.csv'
    lead_time_summary.to_csv(output_path, index=False)
    
    print(f"Lead time analysis complete. Results saved to {output_path}")
    
    # Generate Text Breakdown
    h_lt = lead_time_summary[lead_time_summary['Brand_Group'] == 'HAYAT']['Avg_Lead_Time'].values[0] if not lead_time_summary[lead_time_summary['Brand_Group'] == 'HAYAT'].empty else None
    
    with open(r'C:\Users\iLink\.gemini\antigravity\scratch\Lead_Time_Insights.md', 'w') as f:
        f.write("# Supplier Patterns: Lead Time Analysis (LPO to Delivery)\n\n")
        f.write(f"This analysis evaluates how fast companies respond to LPOs and deliver goods.\n\n")
        f.write(lead_time_summary.to_markdown(index=False))
        f.write("\n\n## Dispatch Insights for Hayat\n")
        if h_lt:
            f.write(f"- **Hayat Average Lead Time**: {h_lt:.2f} days.\n")
            competitors = lead_time_summary[lead_time_summary['Brand_Group'] != 'HAYAT']
            if not competitors.empty:
                best_comp = competitors.iloc[0]
                f.write(f"- **Industry Benchmark**: The fastest deliveries are from `{best_comp['Brand_Group']}` at `{best_comp['Avg_Lead_Time']:.2f}` days.\n")
                if h_lt > best_comp['Avg_Lead_Time']:
                    f.write(f"- **Gap Analysis**: Hayat is `{h_lt - best_comp['Avg_Lead_Time']:.2f}` days slower than industry leaders. Improving dispatch logistics could close this gap.\n")
                else:
                    f.write("- **Performance**: Hayat is leading or performing efficiently compared to industry peers.\n")
        f.write("- **Consistency**: Low Standard Deviation (Std_Dev) indicates reliable delivery schedules, while high values suggest unpredictable logistics.\n")

if __name__ == "__main__":
    analyze_lead_times()
