import os
import asyncio
import pandas as pd
from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards

async def run_stress_test():
    # Setup Engine
    data_dir = os.getcwd()
    engine = OrderEngine(data_dir)
    await engine.load_databases_async()
    
    # Load latest scorecard
    scorecard_file = "Full_Product_Allocation_Scorecard_v7.csv"
    if not os.path.exists(scorecard_file):
        print(f"Error: {scorecard_file} not found.")
        return
        
    df = pd.read_csv(scorecard_file)
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0)),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0)),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size')) else 1),
            'margin_pct': float(row.get('Margin_Pct', 20))
        }
        recommendations.append(rec)
    
    products_map = {r['product_name']: r for r in recommendations}

    # TEST 1: Standard Tier (10M Budget)
    budget_standard = 10_000_000
    print(f"\n--- Testing Standard Tier (Budget: KES {budget_standard:,}) ---")
    result_s = engine.apply_greenfield_allocation(recommendations, budget_standard)
    
    # Apply Guards (This is where rounding/caps happens)
    final_recs_s = apply_safety_guards(result_s['recommendations'], products_map, allocation_mode="initial_load")
    
    # Calculate final cash spend manually after guards
    final_cash_s = 0.0
    for r in final_recs_s:
        if not r.get('is_consignment', False):
            qty = r.get('recommended_quantity', 0)
            cost_p = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
            final_cash_s = round(final_cash_s + round(qty * cost_p, 2), 2)
            
    print(f"Utilization (Post-Guards): {(final_cash_s / budget_standard * 100):.2f}%")
    print(f"Total Cash Used: KES {final_cash_s:,.2f}")
    
    # TEST 2: Mega Tier (50M Budget)
    budget_mega = 50_000_000
    print(f"\n--- Testing Mega Tier (Budget: KES {budget_mega:,}) ---")
    result_m = engine.apply_greenfield_allocation(recommendations, budget_mega)
    
    final_recs_m = apply_safety_guards(result_m['recommendations'], products_map, allocation_mode="initial_load")
    final_cash_m = 0.0
    for r in final_recs_m:
        if not r.get('is_consignment', False):
            qty = r.get('recommended_quantity', 0)
            cost_p = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
            final_cash_m = round(final_cash_m + round(qty * cost_p, 2), 2)

    print(f"Utilization (Post-Guards): {(final_cash_m / budget_mega * 100):.2f}%")
    print(f"Total Cash Used: KES {final_cash_m:,.2f}")

    # TEST 7: 21.77M Case (Reported 827k Drift)
    budget_drift_2 = 21_770_000
    print(f"\n--- Testing 21.77M Case (Budget: KES {budget_drift_2:,}) ---")
    result_d2 = engine.apply_greenfield_allocation(recommendations, budget_drift_2)
    
    final_recs_d2 = apply_safety_guards(result_d2['recommendations'], products_map, allocation_mode="initial_load")
    final_cash_d2 = 0.0
    for r in final_recs_d2:
        qty = r.get('recommended_quantity', 0)
        if not r.get('is_consignment', False):
            cost_p = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
            final_cash_d2 = round(final_cash_d2 + round(qty * cost_p, 2), 2)

    print(f"Utilization (Post-Guards): {(final_cash_d2 / budget_drift_2 * 100):.2f}%")
    print(f"Total Cash Used: KES {final_cash_d2:,.2f}")
    print(f"Drift: KES {(final_cash_d2 - budget_drift_2):,.2f}")

if __name__ == "__main__":
    asyncio.run(run_stress_test())
