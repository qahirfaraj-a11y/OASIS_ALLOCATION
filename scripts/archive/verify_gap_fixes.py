"""
Quick verification of GAP-E, F, I fixes in order_engine.py
"""
from pathlib import Path
import pandas as pd
from oasis.logic.order_engine import OrderEngine
import logging

logging.basicConfig(level=logging.INFO)

data_dir = Path(r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data')
engine = OrderEngine(data_dir)

# Load products from scorecard CSV
scorecard_path = Path(r'C:\Users\iLink\.gemini\antigravity\scratch') / "Full_Product_Allocation_Scorecard_v7.csv"
df = pd.read_csv(scorecard_path)

# Rename columns to match expected format
df = df.rename(columns={
    'Product': 'product_name',
    'Department': 'product_category',
    'Supplier': 'supplier_name',
    'Unit_Price': 'selling_price',
    'Avg_Daily_Sales': 'avg_daily_sales',
    'Lead_Time_Days': 'estimated_delivery_days',
    'Margin_Pct': 'profit_margin',
    'ABC_Class': 'ABC_Class',
    'Priority_Score': 'Priority_Score'
})

# Convert to list of dicts (first 500 for speed)
products = df.head(500).to_dict('records')
products = [p for p in products if p.get('product_name') and str(p.get('product_name', '')).strip()]

print(f"Loaded {len(products)} products")

# Enrich products
enriched = engine.enrich_product_data(products)

print("\n" + "="*70)
print("GAP-E, F, I VERIFICATION TEST")
print("="*70)

# Test with Small_200k budget
budget = 200000
print(f"\nTest: Small Store Budget = KES {budget:,}")
print("-"*70)

result = engine.apply_greenfield_allocation(enriched, budget)
summary = result['summary']

print(f"\nALLOCATION SUMMARY:")
print(f"  Pass 1 (Width):     KES {summary.get('pass1_cash', 0):,.0f}")
print(f"  Pass 2 (Depth):     KES {summary.get('pass2_cash', 0):,.0f}")
print(f"  Pass 2B (Flex):     KES {summary.get('pass2b_cash', 0):,.0f}")
print(f"  Pass 4 (Mop-Up):    KES {summary.get('mop_up_cash', 0):,.0f}")
print(f"  " + "-"*40)
print(f"  TOTAL USED:         KES {summary.get('total_cash_used', 0):,.0f}")
print(f"  UNUSED:             KES {summary.get('unused_budget', 0):,.0f}")
print(f"  UTILIZATION:        {summary.get('utilization_pct', 0):.1f}%")

# Check for anchor override (GAP-F)
anchor_items = [r for r in result['recommendations'] 
                if r.get('product_category', '').upper() in ['COOKING OIL', 'FLOUR', 'SUGAR']
                and r['recommended_quantity'] > 0]

print(f"\nGAP-F CHECK (Anchor Override):")
print(f"  Anchor items allocated: {len(anchor_items)}")

if anchor_items:
    top_anchor = max(anchor_items, key=lambda x: x['recommended_quantity'])
    print(f"  Top anchor: {top_anchor['product_name'][:40]}")
    print(f"  Quantity: {top_anchor['recommended_quantity']} units")

# Check for mop-up items (GAP-E)
mop_up_items = [r for r in result['recommendations'] if '[MOP-UP' in r.get('reasoning', '')]
print(f"\nGAP-E CHECK (Mop-Up Pass):")
print(f"  Items enhanced by mop-up: {len(mop_up_items)}")

if mop_up_items:
    for item in mop_up_items[:3]:
        print(f"    - {item['product_name'][:35]}")

print("\n" + "="*70)
print("VERIFICATION COMPLETE")
print("="*70)

# Final assessment
if summary.get('utilization_pct', 0) > 95:
    print("SUCCESS: Budget utilization > 95% (GAP-E fixed!)")
elif summary.get('utilization_pct', 0) > 90:
    print(f"GOOD: Budget utilization at {summary.get('utilization_pct', 0):.1f}%")
else:
    print(f"WARNING: Budget utilization at {summary.get('utilization_pct', 0):.1f}% - may need review")
