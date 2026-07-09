import os
import asyncio
import pandas as pd
from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards

async def diagnose_consignment_drift():
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
        # Check if is_consignment is in the source data
        is_consignment = str(row.get('Is_Consignment', 'False')).upper() == 'TRUE'
        
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0)),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0)),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size')) else 1),
            'margin_pct': float(row.get('Margin_Pct', 20)),
            'is_consignment': is_consignment
        }
        recommendations.append(rec)
    
    products_map = {r['product_name']: r for r in recommendations}

    # Test with a specific budget
    target_budget = 300_000
    print(f"\n--- Diagnosing Drift at KES {target_budget:,} ---")
    
    result = engine.apply_greenfield_allocation(recommendations, target_budget)
    
    # Look at the engine's internal summary
    summary = result['summary']
    print(f"Engine Summary:")
    print(f"  Pass 1 Cash: {summary.get('pass1_cash', 0):,.2f}")
    print(f"  Pass 2 Cash: {summary.get('pass2_cash', 0):,.2f}")
    print(f"  Pass 2B Cash: {summary.get('pass2b_cash', 0):,.2f}")
    print(f"  Mop Up Cash: {summary.get('mop_up_cash', 0):,.2f}")
    print(f"  Total Cash Used: {summary.get('total_cash_used', 0):,.2f}")
    print(f"  Utilization: {summary.get('utilization_pct', 0):.4f}%")
    
    # Analyze Consignment
    recs = result['recommendations']
    total_consignment = sum(r['recommended_quantity'] * engine._get_actual_cost_price(r, r['selling_price']) for r in recs if r.get('is_consignment'))
    print(f"  Total Consignment Value: {total_consignment:,.2f}")
    
    # Now check post-guards
    final_recs = apply_safety_guards(recs, products_map, allocation_mode="initial_load")
    
    final_cash = 0.0
    for r in final_recs:
        if not r.get('is_consignment', False):
            qty = r.get('recommended_quantity', 0)
            cost_p = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
            final_cash = round(final_cash + round(qty * cost_p, 2), 2)
            
    print(f"\nPost-Guards Utilization: {(final_cash / target_budget * 100):.2f}%")
    print(f"Post-Guards Cash Used: KES {final_cash:,.2f}")
    print(f"Post-Guards Drift: KES {target_budget - final_cash:,.2f}")

    # Trace Wallet spending
    # Since we don't have direct access to wallets after the run easily, 
    # we'll look at the return value's dept_utilization if available.
    print("\nDept Utilization:")
    for dept, util in summary.get('dept_utilization', {}).items():
        if util > 0:
            print(f"  {dept}: {util}%")

if __name__ == "__main__":
    asyncio.run(diagnose_consignment_drift())
