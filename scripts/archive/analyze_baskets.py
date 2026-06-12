import pandas as pd
import os
import glob

# Files to analyze
files = [
    r"C:\Users\iLink\Downloads\Allocated_Basket_v2 (15).csv",
    r"C:\Users\iLink\Downloads\Allocated_Basket_v2 (16).csv",
    r"C:\Users\iLink\Downloads\Allocated_Basket_v2 (17).csv",
    r"C:\Users\iLink\Downloads\Allocated_Basket_v2 (18).csv"
]

print("=== DEEP ANALYSIS OF ALLOCATION BASKETS ===")

total_loose_packs = 0
total_small_qty = 0

for file_path in files:
    if not os.path.exists(file_path):
        print(f"File not found: {file_path}")
        continue
        
    df = pd.read_csv(file_path)
    
    total_cost = df['Allocated_Cost'].sum()
    total_items = len(df)
    
    qty_1_items = df[df['Qty'] == 1.0]
    qty_small_items = df[df['Qty'] <= 3.0]
    
    print(f"\n--- {os.path.basename(file_path)} ---")
    print(f"Total Allocated Cost: {total_cost:,.2f}")
    print(f"Total Unique Items: {total_items}")
    print(f"Items with Qty == 1: {len(qty_1_items)} ({(len(qty_1_items)/total_items)*100:.1f}%)")
    print(f"Items with Qty <= 3: {len(qty_small_items)} ({(len(qty_small_items)/total_items)*100:.1f}%)")
    
    # Analyze reasoning for small qty
    print("Reasoning sample for Qty == 1:")
    print(qty_1_items['Reasoning'].head(3).tolist())
    
    total_loose_packs += len(qty_1_items)
    total_small_qty += len(qty_small_items)

print("\n=== SUMMARY ===")
print(f"Total Qty=1 across files: {total_loose_packs}")
print(f"Total Qty<=3 across files: {total_small_qty}")
