"""
Analyze Real Supplier Lead Times
=================================
"""

import pandas as pd
import sys

if sys.platform == 'win32':
    sys.stdout.reconfigure(encoding='utf-8')

print("=" * 80)
print("SUPPLIER LEAD TIME ANALYSIS")
print("=" * 80)

# Load supplier intelligence
file1 = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx"
file2 = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Fulfillment_Summary.xlsx"

try:
    df_intel = pd.read_excel(file1)
    print(f"\nLoaded: Supplier_Intelligence_Report_2025_v3.xlsx")
    print(f"Columns: {list(df_intel.columns)}")
    
    # Check for lead time columns
    lead_time_cols = [c for c in df_intel.columns if 'lead' in c.lower() or 'delivery' in c.lower() or 'days' in c.lower()]
    print(f"Lead time related columns: {lead_time_cols}")
    
    if lead_time_cols:
        for col in lead_time_cols[:3]:  # Show first 3
            print(f"\n{col}:")
            print(f"  Mean: {df_intel[col].mean():.1f}")
            print(f"  Median: {df_intel[col].median():.1f}")
            print(f"  Min: {df_intel[col].min():.1f}")
            print(f"  Max: {df_intel[col].max():.1f}")
            
except Exception as e:
    print(f"Error loading file 1: {e}")

try:
    df_fulfill = pd.read_excel(file2)
    print(f"\n\nLoaded: Supplier_Fulfillment_Summary.xlsx")
    print(f"Columns: {list(df_fulfill.columns)}")
    
    # Check for lead time columns
    lead_time_cols = [c for c in df_fulfill.columns if 'lead' in c.lower() or 'delivery' in c.lower() or 'days' in c.lower()]
    print(f"Lead time related columns: {lead_time_cols}")
    
    if lead_time_cols:
        for col in lead_time_cols[:3]:
            print(f"\n{col}:")
            print(f"  Mean: {df_fulfill[col].mean():.1f}")
            print(f"  Median: {df_fulfill[col].median():.1f}")
            print(f"  Min: {df_fulfill[col].min():.1f}")
            print(f"  Max: {df_fulfill[col].max():.1f}")
            
except Exception as e:
    print(f"Error loading file 2: {e}")

print("\n" + "=" * 80)
print("RECOMMENDED ALLOCATION DEPTHS")
print("=" * 80)

print("""
Based on lead times, recommended initial allocation:

Formula: Target Days = Lead Time + ROP Safety + Cycle Stock + Weekend Buffer

For standard items:
  - Lead Time: 2-7 days (from supplier data)
  - ROP Safety: 3 days
  - Cycle Stock: 14 days (2 weeks)
  - Weekend Buffer: 2 days (for Fri-Sat spike)
  
  Total: 21-26 days minimum

For high-velocity (>5 ADS):
  - Add +7 days buffer
  Total: 28-33 days

For fresh items:
  - Cap at 10 days (spoilage risk)
  Total: 10 days maximum
""")

print("=" * 80)
