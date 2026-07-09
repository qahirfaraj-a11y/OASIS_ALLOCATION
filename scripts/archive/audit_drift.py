import os
import sys
import pandas as pd
import math

# Add project root to sys.path
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT)

from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards

def audit_tier(budget):
    engine = OrderEngine(ROOT)
    scorecard_path = os.path.join(ROOT, "Full_Product_Allocation_Scorecard_v3.csv")
    df = pd.read_csv(scorecard_path)
    recommendations = []
    for _, row in df.iterrows():
        p_name = row.get('Product')
        if not p_name or pd.isna(p_name): continue
        rec = {
            'product_name': p_name,
            'selling_price': float(row.get('Unit_Price', 0)),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0)),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size')) else 1),
            'margin_pct': float(row.get('Margin_Pct', 20)),
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    engine.enrich_product_data(recommendations, is_greenfield=True)
    products_map = {r['product_name']: r for r in recommendations}
    result = engine.apply_greenfield_allocation(recommendations, budget)
    raw_recs = result['recommendations']
    summary = result['summary']
    
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
    
    # compare
    engine_cost_sum = 0
    final_cash_sum = 0
    consignment_mismatches = 0
    
    for raw, final in zip(raw_recs, final_recs):
        q_engine = raw['recommended_quantity']
        q_final = final['recommended_quantity']
        is_cons_engine = raw.get('is_consignment', False)
        is_cons_final = final.get('is_consignment', False)
        
        c_p = engine._get_actual_cost_price(raw, float(raw.get('selling_price', 0)))
        
        if q_engine > 0 and not is_cons_engine:
            engine_cost_sum += round(q_engine * c_p, 2)
            
        if q_final > 0 and not is_cons_final:
            final_cash_sum += round(q_final * c_p, 2)
            
        if is_cons_engine != is_cons_final:
            consignment_mismatches += 1

    print(f"\n[AUDIT RESULTS FOR {budget:,.0f} KES]")
    print(f"Engine Summary Cost: {summary.get('total_cash_used', 0):,.2f}")
    print(f"Engine Loop Sum:     {engine_cost_sum:,.2f}")
    print(f"Final Guard Sum:     {final_cash_sum:,.2f}")
    print(f"Drift (Engine-Guard): {engine_cost_sum - final_cash_sum:,.2f}")
    print(f"Consignment Mismatches: {consignment_mismatches}")
            
if __name__ == "__main__":
    audit_tier(1_350_000)
