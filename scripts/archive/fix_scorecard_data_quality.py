"""
Scorecard Data Quality Fixer
============================
Fixes two issues:
1. Price imputation - Many items show same fallback price
2. Wrong department mapping - Items in wrong categories
"""

import pandas as pd
import json
import os
from difflib import get_close_matches

# Configuration
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
OUTPUT_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v4.csv"

# Load data
print("Loading scorecard...")
df = pd.read_csv(SCORECARD_FILE)
print(f"Loaded {len(df)} rows")

# --- FIX 1: Identify and flag imputed prices ---
print("\n=== FIX 1: Price Imputation Analysis ===")

# Find the suspicious repeated price values
price_counts = df['Unit_Price'].round(2).value_counts().head(10)
print("Top 10 most common prices:")
print(price_counts)

# Find items with the most common (likely imputed) price
most_common_price = price_counts.index[0]
imputed_mask = df['Unit_Price'].round(2) == most_common_price
imputed_items = df[imputed_mask]
print(f"\nItems with suspected imputed price ({most_common_price:.2f}): {len(imputed_items)}")

# Load profitability data for better price lookup
profit_path = os.path.join(DATA_DIR, "sales_profitability_intelligence_2025.json")
if os.path.exists(profit_path):
    with open(profit_path, 'r') as f:
        profit_data = json.load(f)
    print(f"Loaded {len(profit_data)} items from profitability database")
else:
    print(f"Warning: {profit_path} not found")
    profit_data = {}

# Fix prices using profitability data
fixed_prices = 0
for idx in imputed_items.index:
    product = df.at[idx, 'Product']
    product_upper = product.strip().upper()
    
    # Try exact match
    if product_upper in profit_data:
        p_data = profit_data[product_upper]
        price = p_data.get('unit_selling_price', 0)
        if price <= 0 and p_data.get('total_qty_sold', 0) > 0:
            price = p_data.get('revenue', 0) / p_data.get('total_qty_sold')
        if price > 0:
            df.at[idx, 'Unit_Price'] = price
            fixed_prices += 1
            continue
    
    # Try fuzzy match
    matches = get_close_matches(product_upper, list(profit_data.keys()), n=1, cutoff=0.85)
    if matches:
        p_data = profit_data[matches[0]]
        price = p_data.get('unit_selling_price', 0)
        if price <= 0 and p_data.get('total_qty_sold', 0) > 0:
            price = p_data.get('revenue', 0) / p_data.get('total_qty_sold')
        if price > 0:
            df.at[idx, 'Unit_Price'] = price
            fixed_prices += 1

print(f"Fixed {fixed_prices} prices from profitability data")

# --- FIX 2: Correct Wrong Department Mappings ---
print("\n=== FIX 2: Department Mapping Corrections ===")

# Define explicit corrections for known mismatches
DEPARTMENT_CORRECTIONS = {
    # Pattern: (Product keyword, Wrong Dept, Correct Dept)
    "TOILET TISSUE": ("COOKING OIL", "TOILET ROLL"),
    "TOILET CISTERN": ("COOKING OIL", "ALL CLEANERS"),
    "KITCHEN TOWEL": ("COOKING OIL", "TISSUE PAPER"),
    "TISSUE": ("COOKING OIL", "TISSUE PAPER"),
    "TOILET ROLL": ("COOKING OIL", "TOILET ROLL"),
    "CLEANROL": ("COOKING OIL", "TISSUE PAPER"),
    "VELVEX": ("COOKING OIL", "TISSUE PAPER"),
    "SAFISHA": ("COOKING OIL", "ALL CLEANERS"),
}

dept_fixes = 0
for idx, row in df.iterrows():
    product = str(row.get('Product', '')).upper()
    current_dept = row.get('Department', '')
    
    for keyword, (wrong_dept, correct_dept) in DEPARTMENT_CORRECTIONS.items():
        if keyword in product and current_dept == wrong_dept:
            df.at[idx, 'Department'] = correct_dept
            dept_fixes += 1
            break

print(f"Fixed {dept_fixes} department mappings using explicit corrections")

# --- Additional: Fix departments based on product keywords ---
KEYWORD_DEPT_MAP = {
    # Household / Cleaning
    'TISSUE': 'TISSUE PAPER',
    'TOILET ROLL': 'TOILET ROLL',
    'SERVIETTE': 'SERVIETTES',
    'KITCHEN TOWEL': 'TISSUE PAPER',
    'DETERGENT': 'DETERGENTS',
    'BLEACH': 'ALL CLEANERS',
    'DISINFECT': 'DISINFECTANT',
    
    # Food categories
    'RICE': 'RICE',
    'SUGAR': 'SUGAR',
    'SALT': 'SALT',
    'FLOUR': 'FLOUR',
    'OAT': 'OATS',
    'CEREAL': 'BREAKFAST CEREALS',
    'NOODLE': 'NOODLES',
    'PASTA': 'PASTA',
    'SPAGHETTI': 'PASTA',
    
    # Beverages
    'MILK': 'FRESH MILK',
    'YOGHURT': 'YOGHURT',
    'YOGURT': 'YOGHURT',
    'JUICE': 'TETRA PACK JUICE',
    'SODA': 'SODA',
    'WATER': 'MINERAL WATER',
    
    # Cooking
    'COOKING OIL': 'COOKING OIL',
    'SUNFLOWER OIL': 'COOKING OIL',
    'VEGETABLE OIL': 'COOKING OIL',
}

# Only fix items in suspicious "COOKING OIL" or "General Merchandise" departments
suspicious_depts = ['COOKING OIL', 'General Merchandise']
suspicious_items = df[df['Department'].isin(suspicious_depts)]

keyword_fixes = 0
for idx in suspicious_items.index:
    product = str(df.at[idx, 'Product']).upper()
    current_dept = df.at[idx, 'Department']
    
    # Skip actual cooking oils
    if 'OIL' in product and any(x in product for x in ['COOKING', 'VEGETABLE', 'SUNFLOWER', 'CORN', 'OLIVE', 'FRESH FRI', 'ELIANTO', 'RINA', 'GOLDEN']):
        continue
    
    for keyword, correct_dept in KEYWORD_DEPT_MAP.items():
        if keyword in product and current_dept != correct_dept:
            # Don't change if it would make COOKING OIL items into something else incorrectly
            if correct_dept != current_dept:
                df.at[idx, 'Department'] = correct_dept
                keyword_fixes += 1
                break

print(f"Fixed {keyword_fixes} additional department mappings using keywords")

# --- VERIFICATION ---
print("\n=== VERIFICATION ===")

# Check COOKING OIL department
cooking_oil_items = df[df['Department'] == 'COOKING OIL'][['Product', 'Unit_Price']].head(15)
print("\nCOOKING OIL items (after fix):")
print(cooking_oil_items.to_string())

# Check price distribution
print("\n\nPrice distribution after fix:")
new_price_counts = df['Unit_Price'].round(2).value_counts().head(10)
print(new_price_counts)

# Check if imputed price is still dominant
if new_price_counts.index[0] == most_common_price:
    remaining_imputed = new_price_counts.iloc[0]
    print(f"\nWarning: Still have {remaining_imputed} items with imputed price")
else:
    print("\nImputed price is no longer the most common - improvement!")

# Save fixed scorecard
print(f"\nSaving fixed scorecard to {OUTPUT_FILE}...")
df.to_csv(OUTPUT_FILE, index=False)
print("Done!")

# Summary
print("\n=== SUMMARY ===")
print(f"Total rows: {len(df)}")
print(f"Prices fixed: {fixed_prices}")
print(f"Department fixes (explicit): {dept_fixes}")
print(f"Department fixes (keyword): {keyword_fixes}")
print(f"Total fixes: {fixed_prices + dept_fixes + keyword_fixes}")
