"""
Quick Diagnostic: Check if Stockout Fixes Are Applied
======================================================
Analyzes the most recent allocation to verify fixes are active.
"""

import pandas as pd
import sys
from glob import glob
from datetime import datetime

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

print("=" * 80)
print("CHECKING IF FIXES ARE APPLIED")
print("=" * 80)

# Find most recent allocation/simulation result
results = glob("simulation_results_*.xlsx")
if results:
    latest = max(results, key=lambda x: datetime.strptime(x.split('_')[2] + '_' + x.split('_')[3].replace('.xlsx', ''), '%Y%m%d_%H%M%S'))
    
    print(f"\nMost recent file: {latest}")
    print(f"Time: {datetime.strptime(latest.split('_')[2] + '_' + latest.split('_')[3].replace('.xlsx', ''), '%Y%m%d_%H%M%S')}")
    
    # Load and check
    try:
        df_sku = pd.read_excel(latest, sheet_name='SKU Performance')
        
        print(f"\nTotal SKUs: {len(df_sku)}")
        
        # Check stockouts
        if 'First Stockout Day' in df_sku.columns:
            stockouts = df_sku[df_sku['First Stockout Day'].notna()]
            early = stockouts[stockouts['First Stockout Day'] <= 10]
            
            print(f"Total Stockouts: {len(stockouts)} ({len(stockouts)/len(df_sku)*100:.1f}%)")
            print(f"Early Stockouts (Day 1-10): {len(early)} ({len(early)/len(df_sku)*100:.1f}%)")
            
            if len(early) > 0:
                print("\nDepartment Breakdown of Early Stockouts:")
                if 'Department' in early.columns:
                    dept_counts = early['Department'].value_counts()
                    for dept, count in dept_counts.items():
                        pct = count/len(early)*100
                        print(f"  {dept}: {count} SKUs ({pct:.1f}%)")
                
                print("\nDay of First Stockout:")
                day_counts = early['First Stockout Day'].value_counts().sort_index()
                for day, count in day_counts.items():
                    print(f"  Day {int(day)}: {count} SKUs")
        
        # Check KPI Summary
        df_kpi = pd.read_excel(latest, sheet_name='KPI Summary')
        print(f"\nOVERALL METRICS:")
        for col in df_kpi.columns:
            print(f"  {col}: {df_kpi[col].iloc[0]}")
            
    except Exception as e:
        print(f"Error: {e}")

# Check if allocation results exist (to verify reasoning tags)
allocation_files = glob("allocation_results_*.xlsx")
if allocation_files:
    latest_alloc = max(allocation_files, key=lambda x: datetime.strptime(x.split('_')[2] + '_' + x.split('_')[3].replace('.xlsx', ''), '%Y%m%d_%H%M%S'))
    
    print(f"\n{'='*80}")
    print(f"CHECKING ALLOCATION REASONING (Verify fixes applied)")
    print(f"{'='*80}")
    print(f"\nMost recent allocation: {latest_alloc}")
    
    try:
        df_alloc = pd.read_excel(latest_alloc)
        
        # Check for reasoning column
        if 'reasoning' in df_alloc.columns or 'Reasoning' in df_alloc.columns:
            reasoning_col = 'reasoning' if 'reasoning' in df_alloc.columns else 'Reasoning'
            
            # Count fix indicators
            velocity_floor_count = df_alloc[reasoning_col].str.contains('VELOCITY FLOOR', na=False).sum()
            jit_weekend_count = df_alloc[reasoning_col].str.contains('JIT FRESH\\+WEEKEND', na=False, regex=True).sum()
            sim_adjusted_count = df_alloc[reasoning_col].str.contains('simulation_adjusted', na=False).sum()
            
            print(f"\nFix Application Counts:")
            print(f"  [VELOCITY FLOOR 5D]: {velocity_floor_count} SKUs")
            print(f"  [JIT FRESH+WEEKEND]: {jit_weekend_count} SKUs")
            print(f"  Simulation Adjusted: {sim_adjusted_count} SKUs")
            
            if velocity_floor_count == 0 and jit_weekend_count == 0:
                print("\n⚠️ WARNING: Fixes may not be applying!")
            else:
                print("\n✓ Fixes are being applied")
                
    except Exception as e:
        print(f"Error: {e}")

print(f"\n{'='*80}")
