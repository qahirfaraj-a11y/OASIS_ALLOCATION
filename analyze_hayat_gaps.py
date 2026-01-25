import pandas as pd
import glob
import os
import numpy as np

# Configuration
DATA_DIR = r"app/data"
OUTPUT_REPORT = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/hayat_supply_chain_gaps.md"
HAYAT_VENDOR = "HAYAT KIMYA"

def load_hayat_grn_data():
    print("Loading GRN files...")
    grn_files = glob.glob(os.path.join(DATA_DIR, "grnd*.xlsx"))
    dfs = []
    
    for f in grn_files:
        try:
            df = pd.read_excel(f)
            # Normalize Columns
            df.columns = df.columns.str.strip()
            
            # Check for Vendor Column
            if 'Vendor Code - Name' in df.columns:
                hayat_df = df[df['Vendor Code - Name'].str.contains(HAYAT_VENDOR, case=False, na=False)].copy()
                if not hayat_df.empty:
                    dfs.append(hayat_df)
            elif 'Vendor Code / Name' in df.columns: # Sometimes headers vary
                 hayat_df = df[df['Vendor Code / Name'].str.contains(HAYAT_VENDOR, case=False, na=False)].copy()
                 if not hayat_df.empty:
                    dfs.append(hayat_df)
        except Exception as e:
            print(f"Skipping {f}: {e}")
            
    if not dfs:
        return pd.DataFrame()
        
    full_df = pd.concat(dfs, ignore_index=True)
    return full_df

def analyze_gaps():
    df = load_hayat_grn_data()
    if df.empty:
        print("No Hayat data found.")
        return

    print(f"analyzing {len(df)} GRN lines for Hayat...")

    # 1. Fill Rate Analysis
    # PO Qty vs GRN Qty
    # Handle zeros to avoid division error
    df['PO Qty'] = pd.to_numeric(df['PO Qty'], errors='coerce').fillna(0)
    df['GRN Qty'] = pd.to_numeric(df['GRN Qty'], errors='coerce').fillna(0)
    
    # Line Fill Rate
    df['Fill_Rate'] = np.where(df['PO Qty'] > 0, df['GRN Qty'] / df['PO Qty'], 1.0)
    
    # Aggregate Fill Rate (Total GRN / Total PO)
    total_po_qty = df['PO Qty'].sum()
    total_grn_qty = df['GRN Qty'].sum()
    overall_fill_rate = (total_grn_qty / total_po_qty * 100) if total_po_qty > 0 else 0
    
    # Under-delivery count
    under_delivered = df[df['Fill_Rate'] < 1.0]
    perfect_orders = df[df['Fill_Rate'] == 1.0]
    over_delivered = df[df['Fill_Rate'] > 1.0] # Rare but possible
    
    # 2. Split Delivery Analysis
    # How many GRNs per PO?
    if 'PO No' in df.columns:
        df['PO No'] = df['PO No'].astype(str)
        po_split_summary = df.groupby('PO No')['GRN No'].nunique()
        split_pos = po_split_summary[po_split_summary > 1]
        split_percent = (len(split_pos) / len(po_split_summary) * 100) if not po_split_summary.empty else 0
    else:
        split_percent = 0
        
    # 3. Generating Report
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("# Hayat Kimya: Supply Chain Gap Analysis\n\n")
        f.write("Since we established that **speed (3.6 days)** is not the problem, we analyzed the data for **reliability and accuracy gaps**.\n\n")
        
        # Section 1: Quantity Accuracy
        f.write("## 1. The \"Fill Rate\" Gap\n")
        f.write(f"- **Overall Fill Rate**: **{overall_fill_rate:.2f}%**\n")
        f.write(f"- **Perfect Orders**: {len(perfect_orders)} lines ({len(perfect_orders)/len(df)*100:.1f}%)\n")
        f.write(f"- **Shorted Orders**: {len(under_delivered)} lines ({len(under_delivered)/len(df)*100:.1f}%)\n")
        
        if overall_fill_rate < 95:
            f.write("> [!WARNING]\n")
            f.write(f"> A Fill Rate of {overall_fill_rate:.2f}% indicates that while Hayat delivers *fast*, they don't always deliver *full*. \n")
            f.write("> **Recommendation**: You need to order **5% more stock** than predicted to account for these shorts, or implement a \"Backorder Penalty\" clause.\n\n")
        else:
            f.write("> [!TIP]\n")
            f.write(f"> Fill Rate is excellent ({overall_fill_rate:.2f}%). You do not need to buffer for quantity loss.\n\n")

        # Section 2: Consolidation
        f.write("## 2. The \"Split Delivery\" Gap\n")
        f.write(f"- **Split POs**: {split_percent:.1f}% of Purchase Orders arrive in multiple chunks.\n")
        if split_percent > 10:
             f.write("- **Diagnosis**: **Fragmentation**. Hayat is struggling to consolidate orders. This increases receiving costs at your warehouse (multiple trucks for one order).\n")
             f.write("- **Recommendation**: Enforce a \"Single Ship\" policy or consolidate orders to reduce receiving overhead.\n\n")
        else:
             f.write("- **Diagnosis**: **Consolidated**. Orders generally arrive largely intact.\n\n")

        # Section 3: SKU Level Shorts
        f.write("## 3. SKU-Level Availability Risks\n")
        if not under_delivered.empty:
            f.write("The following items are most frequently shorted:\n")
            shorts = under_delivered.groupby('Item Name').size().sort_values(ascending=False).head(5)
            for item, count in shorts.items():
                f.write(f"- **{item}**: Shorted {count} times.\n")
            f.write("\n> **Recommendation**: Increase safety stock specifically for these \"Risk SKUs\".\n")
        else:
            f.write("No specific SKUs are consistently shorted.\n")

if __name__ == "__main__":
    analyze_gaps()
