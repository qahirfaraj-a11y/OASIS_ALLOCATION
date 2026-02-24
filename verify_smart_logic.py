"""
Verification Script: Smart Greenfield Logic
===========================================
Tests:
1. Freshness Constraint (Milk: ~2-3 days)
2. Long Life Efficiency (UHT: should start HIGH, e.g. 14-30 days)
3. Slow Mover Efficiency (Wine: Low Frequency but High Value?)
"""

from oasis.logic.order_engine import OrderEngine
import os
import pandas as pd

# 1. Setup
DATA_DIR = os.path.join(os.getcwd(), 'oasis', 'data')
engine = OrderEngine(DATA_DIR)

# Mock Data for Smart Allocation Test
mock_recs = [
    # Fresh Milk: High Velocity, Daily Delivery -> Target ~2-3 days
    {
        'product_name': 'FRESH MILK 500ML',
        'product_category': 'FRESH MILK',
        'avg_daily_sales': 100.0,
        'estimated_delivery_days': 1,
        'supplier_frequency': 'daily',
        'demand_cv': 0.2, # Low volatility
        'selling_price': 60,
        'is_fresh': True,
        'recommended_quantity': 0, 'reasoning': '', 'pass1_allocated': True, 'profit_margin': 0.1
    },
    # UHT Milk: High Velocity, Weekly Delivery, Dry -> Target ~7+1+Safety
    {
        'product_name': 'UHT MILK 500ML',
        'product_category': 'MILK', 
        'avg_daily_sales': 100.0,
        'estimated_delivery_days': 7,
        'supplier_frequency': 'weekly',
        'demand_cv': 0.5, # Moderate volatility
        'selling_price': 65,
        'is_fresh': False,
        'recommended_quantity': 0, 'reasoning': '', 'pass1_allocated': True, 'profit_margin': 0.15
    },
    # Imported Wine: Low Velocity, Monthly Delivery -> Target ~30+7+Safety
    {
        'product_name': 'IMPORTED RED WINE',
        'product_category': 'WINE', 
        'avg_daily_sales': 2.0,
        'estimated_delivery_days': 7,
        'supplier_frequency': 'monthly',
        'demand_cv': 0.8, # High volatility
        'selling_price': 1000, # Lowered from 1500 to pass Micro+ ceiling (1250)
        'is_fresh': False,
        'recommended_quantity': 0, 'reasoning': '', 'pass1_allocated': True, 'profit_margin': 0.30
    }
]

print("=" * 60)
print("TEST 1: SMART REPLENISHMENT TARGETS")
print("=" * 60)

# Init Budget Manager to avoid errors
from oasis.logic.budget_manager import BudgetManager
engine.budget_manager = BudgetManager(DATA_DIR) 

# Run Allocation
budget = 500000
result = engine.apply_greenfield_allocation(mock_recs, budget)

for r in result['recommendations']:
    name = r['product_name']
    qty = r['recommended_quantity']
    ads = r['avg_daily_sales']
    days_coverage = qty / ads if ads > 0 else 0
    print(f"Product: {name:<20} | Qty: {qty:<5} | Days: {days_coverage:.1f} | Reason: {r['reasoning']}")

# Verify
fresh_milk = next(r for r in mock_recs if 'FRESH' in r['product_name'])
uht_milk = next(r for r in mock_recs if 'UHT' in r['product_name'])
wine = next(r for r in mock_recs if 'WINE' in r['product_name'])

fresh_days = fresh_milk['recommended_quantity'] / fresh_milk['avg_daily_sales']
uht_days = uht_milk['recommended_quantity'] / uht_milk['avg_daily_sales']
wine_days = wine['recommended_quantity'] / wine['avg_daily_sales']

# Assertions
fresh_ok = fresh_days <= 3.1 # Strict Cap
uht_ok = uht_days >= 10.0 # Weekly + Lead + Safety should be > 10
wine_ok = wine_days >= 30.0 # Monthly review period implied > 30

if fresh_ok and uht_ok and wine_ok:
    print("\n[OK] SUCCESS: Smart Allocation Logic Verified")
    print(f"  Milk: {fresh_days:.1f}d (Exp <=3)")
    print(f"  UHT:  {uht_days:.1f}d (Exp >=10)")
    print(f"  Wine: {wine_days:.1f}d (Exp >=30)")
else:
    print("\n[FAIL] FAILURE: Targets missed")
    print(f"  Milk: {fresh_days:.1f}d (Exp <=3) -> {'OK' if fresh_ok else 'FAIL'}")
    print(f"  UHT:  {uht_days:.1f}d (Exp >=10) -> {'OK' if uht_ok else 'FAIL'}")
    print(f"  Wine: {wine_days:.1f}d (Exp >=30) -> {'OK' if wine_ok else 'FAIL'}")

print("\n" + "=" * 60)
print("TEST 2: REAL DATA (Small_200k) - UNIFORMITY CHECK")
print("=" * 60)
from retail_simulator import load_scorecard_data, SKUState

# Need to init engine with DBs
engine.load_local_databases()

# Load Real Data Scaled to Micro (200k)
# Note: retail_simulator.load_scorecard_data calls engine.apply_greenfield_allocation
budget_micro = 200000
tier = "Small_200k"
demand_scale = 0.003 # From retail_simulator config

print(f"Loading Real Data for {tier} ($200k)...")
# We can't easily call retail_simulator.load... because it needs CLI args usually.
# Let's verify manually using engine direct calls like load_scorecard_data does.

import pandas as pd
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
raw_df = pd.read_csv(SCORECARD_FILE)

products = []
for _, row in raw_df.iterrows():
    products.append({
        'product_name': str(row.get('Product', 'Unknown')),
        'supplier_name': str(row.get('Supplier', 'Unknown')),
        'product_category': str(row.get('Department', 'GENERAL')),
        'selling_price': float(row.get('Unit_Price', 0) or 0),
        'margin_pct': float(row.get('Margin_Pct', 25) or 25),
        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) or 0), # Raw Demand
        'current_stocks': 0.0,
        'pack_size': 1,
        'ABC_Class': str(row.get('ABC_Class', 'C')),
        'reliability_score': 90,
        'is_consignment': False
    })

# Enrich
enriched = engine.enrich_product_data(products)

# Scale Demand MANUALLY for Micro
for p in enriched:
    p['avg_daily_sales'] = p['avg_daily_sales'] * demand_scale
    p['units_sold_last_month'] = (p['avg_daily_sales'] * 30)

# Run Allocation
print(f"Applying Allocation with Scale Factor {demand_scale:.4f}...")
result_micro = engine.apply_greenfield_allocation(enriched, budget_micro)

# ANALYZE DISTRIBUTION
recs = result_micro['recommendations']
allocated = [r for r in recs if r['recommended_quantity'] > 0]
qtys = [r['recommended_quantity'] for r in allocated]

from collections import Counter
qty_counts = Counter(qtys)

print(f"\nTotal Items: {len(recs)}")
print(f"Allocated Items: {len(allocated)}")
print(f"Distribution of Quantities (Top 10):")
for q, count in qty_counts.most_common(10):
    print(f"  Qty {q}: {count} items")

# Check if "Pass 2" ran
pass2_count = sum(1 for r in allocated if "[PASS 2]" in r.get('reasoning', ''))
print(f"Items with Pass 2 Depth: {pass2_count}")

if pass2_count < 10:
    print("\n[FAIL] Startling Uniformity: Pass 2 (Smart Depth) barely ran!")
else:
    print(f"\n[OK] Smart Depth Active on {pass2_count} items.")
