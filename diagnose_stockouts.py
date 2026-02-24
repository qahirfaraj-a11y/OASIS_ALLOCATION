"""
Diagnostic Script: Analyze Recent Simulation Stockouts
=======================================================
Identifies which simulations are experiencing Day 1-10 stockouts.
"""

import pandas as pd
import os
from glob import glob
from datetime import datetime

print("=" * 80)
print("STOCKOUT DIAGNOSTIC ANALYSIS")
print("=" * 80)

# Find all recent simulation results
results_files = glob("simulation_results_*.xlsx")
results_files.sort(key=lambda x: os.path.getmtime(x), reverse=True)

print(f"\nFound {len(results_files)} simulation result files")
print("Analyzing most recent 5 simulations...\n")

for idx, file_path in enumerate(results_files[:5]):
    try:
        print(f"\n{'='*80}")
        print(f"FILE {idx+1}: {file_path}")
        
        # Get file timestamp
        mod_time = datetime.fromtimestamp(os.path.getmtime(file_path))
        print(f"Generated: {mod_time.strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Read the Excel file
        xl = pd.ExcelFile(file_path)
        
        # Look for SKU-level data (usually in a sheet called 'SKU Details' or 'Final State')
        sheet_names = xl.sheet_names
        print(f"Sheets: {', '.join(sheet_names)}")
        
        # Try to find stockout data
        stockout_data = None
        for sheet in ['Final State', 'SKU Details', 'Simulation Results', 'Summary']:
            if sheet in sheet_names:
                df = pd.read_excel(file_path, sheet_name=sheet)
                
                # Check if this sheet has stockout information
                stockout_cols = [col for col in df.columns if 'stockout' in col.lower() or 'first_stockout' in col.lower()]
                
                if stockout_cols:
                    print(f"\nFound stockout data in '{sheet}' sheet")
                    print(f"Columns: {stockout_cols}")
                    
                    # Analyze early stockouts (Day 1-10)
                    if 'first_stockout_day' in df.columns or 'First Stockout Day' in df.columns:
                        stockout_col = 'first_stockout_day' if 'first_stockout_day' in df.columns else 'First Stockout Day'
                        
                        # Filter for items with stockouts
                        df_stockouts = df[df[stockout_col].notna()]
                        total_skus = len(df)
                        stockout_skus = len(df_stockouts)
                        
                        # Early stockouts (Day 1-10)
                        early_stockouts = df_stockouts[df_stockouts[stockout_col] <= 10]
                        
                        print(f"\n📊 STOCKOUT SUMMARY:")
                        print(f"  Total SKUs: {total_skus}")
                        print(f"  SKUs with Stockouts: {stockout_skus} ({stockout_skus/total_skus*100:.1f}%)")
                        print(f"  Early Stockouts (Day 1-10): {len(early_stockouts)} ({len(early_stockouts)/total_skus*100:.1f}%)")
                        
                        if len(early_stockouts) > 0:
                            print(f"\n⚠️ TOP 10 EARLY STOCKOUT SKUs:")
                            
                            # Get product name and department if available
                            display_cols = [stockout_col]
                            for col in ['product_name', 'Product Name', 'SKU', 'department', 'Department']:
                                if col in early_stockouts.columns:
                                    display_cols.insert(0, col)
                            
                            display_cols = list(dict.fromkeys(display_cols))  # Remove duplicates
                            
                            print(early_stockouts[display_cols].head(10).to_string(index=False))
                            
                            # Category breakdown
                            if 'department' in early_stockouts.columns or 'Department' in early_stockouts.columns:
                                dept_col = 'department' if 'department' in early_stockouts.columns else 'Department'
                                dept_counts = early_stockouts[dept_col].value_counts().head(10)
                                print(f"\n📂 TOP AFFECTED DEPARTMENTS:")
                                for dept, count in dept_counts.items():
                                    print(f"  {dept}: {count} SKUs")
                        
                        stockout_data = df_stockouts
                        break
        
        if stockout_data is None:
            print("⚠️ No stockout data found in this file")
            
    except Exception as e:
        print(f"❌ Error reading {file_path}: {e}")

print(f"\n{'='*80}")
print("ANALYSIS COMPLETE")
print("="*80)
print("\nNext Steps:")
print("1. Identify common patterns in stockout SKUs")
print("2. Check if fixes are being applied (look for [VELOCITY FLOOR], [JIT FRESH+WEEKEND] in reasoning)")
print("3. Verify allocation depths match updated logic")
