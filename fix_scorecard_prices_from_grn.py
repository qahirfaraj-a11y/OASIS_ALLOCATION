"""
Scorecard Price Fix from GRN Data
==================================
Extracts actual pricing from GRN Excel files to fix imputed prices.
"""

import pandas as pd
import os
import glob
from difflib import get_close_matches

# Configuration
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v4.csv"
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v5.csv"

# Imputed price threshold
IMPUTED_PRICE = 108.20  

print("Loading scorecard...")
df = pd.read_csv(SCORECARD_FILE)
print(f"Loaded {len(df)} rows")

# Identify items needing price fix
imputed_mask = df['Unit_Price'].round(2) == IMPUTED_PRICE
items_needing_fix = df[imputed_mask]['Product'].tolist()
print(f"\nItems with imputed price ({IMPUTED_PRICE}): {len(items_needing_fix)}")

# Extract prices from GRN files
print("\n=== Extracting Prices from GRN Data ===")
grn_files = glob.glob(os.path.join(DATA_DIR, "grnds_*.xlsx"))
print(f"Found {len(grn_files)} GRN files")

# Build price lookup from GRN data (use median SP per item)
price_lookup = {}
all_grn_data = []

for grn_file in grn_files:
    try:
        grn_df = pd.read_excel(grn_file, usecols=['Item Name', 'Cost Price', 'SP'])
        all_grn_data.append(grn_df)
    except Exception as e:
        print(f"Error reading {grn_file}: {e}")

if all_grn_data:
    combined_grn = pd.concat(all_grn_data, ignore_index=True)
    print(f"Total GRN records: {len(combined_grn)}")
    
    # Clean and normalize
    combined_grn['Item_Upper'] = combined_grn['Item Name'].str.strip().str.upper()
    combined_grn = combined_grn[combined_grn['SP'] > 0]  # Filter out zero prices
    
    # Calculate median SP per item
    item_prices = combined_grn.groupby('Item_Upper')['SP'].median().to_dict()
    print(f"Unique items with prices: {len(item_prices)}")
    
    price_lookup = item_prices
else:
    print("Warning: No GRN data loaded!")

# Also extract from PRTS files (contains selling price data)
print("\n=== Extracting Prices from PRTS Data ===")
prts_files = glob.glob(os.path.join(DATA_DIR, "prts_*.xlsx"))
print(f"Found {len(prts_files)} PRTS files")

prts_data = []
for prts_file in prts_files:
    try:
        prts_df = pd.read_excel(prts_file)
        if 'Description' in prts_df.columns and 'SP' in prts_df.columns:
            prts_data.append(prts_df[['Description', 'SP']])
        elif 'Item Name' in prts_df.columns and 'SP' in prts_df.columns:
            prts_data.append(prts_df[['Item Name', 'SP']].rename(columns={'Item Name': 'Description'}))
    except Exception as e:
        print(f"Error reading {prts_file}: {e}")

if prts_data:
    combined_prts = pd.concat(prts_data, ignore_index=True)
    combined_prts['Item_Upper'] = combined_prts['Description'].str.strip().str.upper()
    combined_prts = combined_prts[combined_prts['SP'] > 0]
    
    prts_prices = combined_prts.groupby('Item_Upper')['SP'].median().to_dict()
    print(f"PRTS unique items with prices: {len(prts_prices)}")
    
    # Merge into price_lookup (GRN takes priority)
    for item, price in prts_prices.items():
        if item not in price_lookup:
            price_lookup[item] = price
            
    print(f"Total unique items with prices: {len(price_lookup)}")

# Build index for fuzzy matching
price_items = list(price_lookup.keys())

# Fix prices
print("\n=== Fixing Prices ===")
fixed_count = 0
fuzzy_fixed = 0

for idx in df[imputed_mask].index:
    product = df.at[idx, 'Product'].strip().upper()
    
    # Try exact match
    if product in price_lookup:
        df.at[idx, 'Unit_Price'] = price_lookup[product]
        fixed_count += 1
        continue
    
    # Try fuzzy match
    matches = get_close_matches(product, price_items, n=1, cutoff=0.80)
    if matches:
        df.at[idx, 'Unit_Price'] = price_lookup[matches[0]]
        fuzzy_fixed += 1
        fixed_count += 1

print(f"Prices fixed (exact match): {fixed_count - fuzzy_fixed}")
print(f"Prices fixed (fuzzy match): {fuzzy_fixed}")
print(f"Total prices fixed: {fixed_count}")

# Update derived columns
print("\n=== Updating Derived Columns ===")
# Recalculate Capital_Required based on new prices
df['Capital_Required'] = df['Recommended_Qty'] * df['Unit_Price']
df['Total_Revenue'] = (df['Avg_Daily_Sales'] * 30) * df['Unit_Price']

# Verification
print("\n=== VERIFICATION ===")
remaining_imputed = len(df[df['Unit_Price'].round(2) == IMPUTED_PRICE])
print(f"Remaining items with imputed price: {remaining_imputed}")

print("\nNew price distribution:")
print(df['Unit_Price'].round(2).value_counts().head(10))

# Save
print(f"\nSaving to {OUTPUT_FILE}...")
df.to_csv(OUTPUT_FILE, index=False)
print("Done!")

# Summary
print("\n=== SUMMARY ===")
print(f"Total rows: {len(df)}")
print(f"Started with {len(items_needing_fix)} items needing price fix")
print(f"Fixed {fixed_count} prices")
print(f"Remaining with imputed price: {remaining_imputed}")
print(f"Fix rate: {(fixed_count/len(items_needing_fix)*100):.1f}%")
