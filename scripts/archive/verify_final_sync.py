import os
import sys
import pandas as pd
import logging
import math

# Add project root to sys.path
ROOT = os.path.dirname(os.path.abspath(__file__))
sys.path.append(ROOT)

from oasis.logic.order_engine import OrderEngine
from oasis.simulation.data_loader import HistoricalDataLoader
from oasis.logic.order_logic_guards import apply_safety_guards

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VERIFY")

def verify_tier(budget):
    print(f"\n[VERIFYING TIER: {budget:,.0f} KES]")
    
    engine = OrderEngine(ROOT)
    
    # Load Seasonal Data
    oasis_data_dir = os.path.join(ROOT, 'oasis', 'data')
    loader = HistoricalDataLoader(oasis_data_dir)
    seasonal_map = loader.load_monthly_demand("JAN")
    
    # Load Scorecard
    scorecard_path = os.path.join(ROOT, "Full_Product_Allocation_Scorecard_v3.csv")
    if not os.path.exists(scorecard_path):
        import glob
        cands = glob.glob(os.path.join(ROOT, "Full_Product_Allocation_Scorecard_v*.csv"))
        if cands: scorecard_path = cands[0]
        else: raise FileNotFoundError("Scorecard not found")

    df = pd.read_csv(scorecard_path)
    recommendations = []
    
    # v10.9: Hardened column mapping to match Engine requirements
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

    # 1. Enrichment
    engine.enrich_product_data(recommendations, is_greenfield=True)
    products_map = {r['product_name']: r for r in recommendations}
    
    # 2. Engine Allocation
    result = engine.apply_greenfield_allocation(recommendations, budget, seasonal_demand_map=seasonal_map)
    raw_recs = result['recommendations']
    
    # 3. Safety Guards
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
    
    # 4. Final Calculation
    final_cash = 0.0
    for r in final_recs:
        q = r.get('recommended_quantity', 0)
        if q > 0:
            c_est = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
            cost = round(float(q) * float(c_est), 2)
            if not r.get('is_consignment'):
                 final_cash += cost
                 
    utilization = (final_cash / budget) * 100 if budget > 0 else 0
    engine_reported = result['summary'].get('total_cash_used', 0)
    
    print(f"   Budget (KES) |   SKUs |   Util % |   UI Cash Spent |     Engine Cost")
    print(f"--------------------------------------------------------------------------------")
    print(f" {budget:14,d} | {len(final_recs):6d} | {utilization:7.2f}% | {final_cash:15,.2f} | {engine_reported:15,.2f}")
    
    return final_cash

if __name__ == "__main__":
    budgets = [326_000, 1_350_000, 5_214_000, 17_254_321, 80_000_000]
    for b in budgets:
        verify_tier(b)
