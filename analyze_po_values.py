import pandas as pd
import os
import glob
import numpy as np

DATA_DIR = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

def analyze_po_values():
    print(" Analyzing PO Values from Excel Files...")
    print("="*60)
    
    # 1. Find Files
    po_files = glob.glob(os.path.join(DATA_DIR, "po_*.xlsx"))
    if not po_files:
        print("ERROR: No PO files found in", DATA_DIR)
        return

    all_orders = []
    
    for f in po_files:
        print(f"Reading: {os.path.basename(f)}...")
        try:
            # Assume standarized PO format where:
            # - Need to group by 'PO Number' or similar
            # - Need 'Total Amount' column? 
            # Often raw extracts have line items.
            
            # Let's inspect the first few rows to deduce structure if we fail
            df = pd.read_excel(f)
            
            # Normalize Headers
            df.columns = [str(c).upper().strip() for c in df.columns]
            
            # Identify columns
            po_col = next((c for c in df.columns if 'PO' in c and 'NUMBER' in c), None) # PO NUMBER, LPO NUMBER
            if not po_col: po_col = next((c for c in df.columns if 'PO' in c and 'NO' in c), None)
            
            # Look for NET AMT specifically based on logs
            amt_col = 'NET AMT'
            if amt_col not in df.columns:
                 amt_col = next((c for c in df.columns if 'AMOUNT' in c or 'NET' in c), None)
            
            if po_col and amt_col:
                # Group by PO
                # Clean amount (remove commas, currency)
                df[amt_col] = pd.to_numeric(df[amt_col], errors='coerce').fillna(0)
                
                po_totals = df.groupby(po_col)[amt_col].sum()
                all_orders.extend(po_totals.values.tolist())
                print(f"  -> Found {len(po_totals)} orders.")
            else:
                print(f"  -> SKIPPED: Could not identify PO/Amount columns. All Cols: {list(df.columns)}")

                
        except Exception as e:
            print(f"  -> ERROR: {e}")

    # 2. Analyze Stats
    if not all_orders:
        print("No valid orders extracted.")
        return

    values = np.array(all_orders)
    values = values[values > 0] # Filter zeroes
    
    # Outlier Removal (Upper 5%?)
    # Mega stores have huge POs, but let's filter purely anomalies
    cutoff = np.percentile(values, 95)
    filtered = values[values < cutoff]
    
    print("\nSTATISTICS (Mega Store Baseline):")
    print("-" * 30)
    print(f"Count:      {len(values)}")
    print(f"Mean:       KES {np.mean(values):,.0f}")
    print(f"Median:     KES {np.median(values):,.0f} (Robust Baseline)")
    print(f"90th Pctl:  KES {np.percentile(values, 90):,.0f}")
    print(f"Max (Raw):  KES {np.max(values):,.0f}")
    
    print("\nRECOMMENDED MOV (Median / 2?):")
    # MOV shouldn't be the Median (avg order), it should be the floor.
    # Often 20-30% of Average? OR somewhat lower.
    # Let's suggest Median * 0.5 as "Efficient Minimum"?
    rec_mov = np.median(values) * 0.5
    print(f"  -> KES {rec_mov:,.0f}")

if __name__ == "__main__":
    analyze_po_values()
