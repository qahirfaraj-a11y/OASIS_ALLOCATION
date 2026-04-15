import sys
import os
import pandas as pd

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation, get_engine
from oasis.logic.order_logic_guards import apply_safety_guards

budget = 31550000

print("1. Running up to Engine Pass 4...")
# We emulate the first part of allocation_app.py
from oasis.simulation.data_loader import HistoricalDataLoader
loader = HistoricalDataLoader(os.path.join(os.getcwd(), 'allocation_app.py'))
DATA_DIR = os.path.dirname(os.path.join(os.getcwd(), 'allocation_app.py'))

SCORECARD_FILE = os.path.join(os.getcwd(), "Full_Product_Allocation_Scorecard_v7.csv")
df = pd.read_csv(SCORECARD_FILE)

recommendations = []
for _, row in df.iterrows():
    rec = {
        'product_name': row.get('Product'),
        'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
        'product_category': row.get('Department', 'GENERAL'),
        'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size', None)) else 1),
        'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
        'recommended_quantity': 0,
        'reasoning': ''
    }
    recommendations.append(rec)

engine = get_engine()
engine.enrich_product_data(recommendations, is_greenfield=True)

# Run Engine Logic
result = engine.apply_greenfield_allocation(recommendations, budget)
engine_recs = result['recommendations']

# Re-calculate Engine "Cash Spent" from engine_recs using the same function the engine uses
engine_recalc_cost = 0.0
for r in engine_recs:
    qty = r.get('recommended_quantity', 0)
    if qty > 0 and not r.get('is_consignment', False):
        c_p = engine._get_actual_cost_price(r, r.get('selling_price', 0))
        engine_recalc_cost += (qty * c_p)

print(f"Engine Internal Summary Total Cash: {result['summary']['total_cash_used']:,.2f}")
print(f"Engine Recs Re-calculated Cash: {engine_recalc_cost:,.2f}")

# Guard Logic
products_map = {r['product_name']: r for r in recommendations}
final_recs = apply_safety_guards(engine_recs, products_map, allocation_mode="initial_load")

post_guard_cost = 0.0
guard_diffs = []
for r in final_recs:
    qty = r.get('recommended_quantity', 0)
    if qty > 0 and not r.get('is_consignment', False):
        c_p = engine._get_actual_cost_price(r, r.get('selling_price', 0))
        cost = qty * c_p
        post_guard_cost += cost
        
print(f"Post Guard Cash Value: {post_guard_cost:,.2f}")

# Next, see what the App computes
basket_df, cash_spend, consignment_val, alloc_summary, _ = load_and_run_allocation(budget, "JAN")

print(f"Final App Cash Spend: {cash_spend:,.2f}")

