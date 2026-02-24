"""
Diagnostic: Check Recent Simulation Stockouts
"""

import pandas as pd
import sys

# Force UTF-8 output
if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

print("=" * 80)
print("ANALYZING MOST RECENT SIMULATION")
print("=" * 80)

# Load most recent simulation
file_path = "simulation_results_20260207_230013.xlsx"

print(f"\nFile: {file_path}\n")

xl = pd.ExcelFile(file_path)
print(f"Sheets available: {xl.sheet_names}\n")

# Check each sheet
for sheet in xl.sheet_names:
    print(f"\n--- Sheet: {sheet} ---")
    df = pd.read_excel(file_path, sheet_name=sheet, nrows=5)
    print(f"Columns: {list(df.columns)}")
    print(f"Rows: {len(pd.read_excel(file_path, sheet_name=sheet))}")

# Try to load SKU Performance sheet
if 'SKU Performance' in xl.sheet_names:
    print("\n" + "=" * 80)
    print("SKU PERFORMANCE ANALYSIS")
    print("=" * 80)
    
    df = pd.read_excel(file_path, sheet_name='SKU Performance')
    
    # Look for stockout columns
    print(f"\nAll columns: {list(df.columns)}")
    
    stockout_cols = [col for col in df.columns if 'stock' in col.lower()]
    print(f"\nStockout-related columns: {stockout_cols}")
    
    # Check if there's a first stockout day column
    if any('first' in str(col).lower() and 'stock' in str(col).lower() for col in df.columns):
        first_stockout_col = [col for col in df.columns if 'first' in str(col).lower() and 'stock' in str(col).lower()][0]
        
        stockouts = df[df[first_stockout_col].notna()]
        early = stockouts[stockouts[first_stockout_col] <= 10]
        
        print(f"\nTotal SKUs: {len(df)}")
        print(f"Stockouts: {len(stockouts)} ({len(stockouts)/len(df)*100:.1f}%)")
        print(f"Early (Day 1-10): {len(early)} ({len(early)/len(df)*100:.1f}%)")
        
        if len(early) > 0:
            print("\nTop 10 Early Stockouts:")
            display_cols = [col for col in df.columns if col in ['Product Name', 'product_name', 'SKU', 'Department', 'department', first_stockout_col]]
            if display_cols:
                print(early[display_cols].head(10).to_string(index=False))

print("\nDone!")
