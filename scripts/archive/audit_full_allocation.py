import os
import asyncio
import pandas as pd
from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards

async def audit():
    data_dir = os.getcwd()
    engine = OrderEngine(data_dir)
    await engine.load_databases_async()
    
    scorecard_file = "Full_Product_Allocation_Scorecard_v7.csv"
    df = pd.read_csv(scorecard_file)
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size')) else 1),
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else 20.0,
            'supplier_name': row.get('Supplier')
        }
        recommendations.append(rec)
    
    products_map = {r['product_name']: r for r in recommendations}

    # Test the 100M case to stress safety ceilings
    budget = 100_000_000
    print(f"\n--- Auditing Budget: KES {budget:,} ---")
    
    engine.enrich_product_data(recommendations, is_greenfield=True)
    result = engine.apply_greenfield_allocation(recommendations, budget)
    raw_recs = result['recommendations']
    engine_summary = result['summary']
    
    final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
    
    engine_realized = engine_summary['total_cash_used']
    script_realized = 0.0
    
    mismatches = []
    
    for r in final_recs:
        q = r.get('recommended_quantity', 0)
        if q > 0:
            # Use engine's internal cost price logic
            cost_p = r.get('cost_price')
            if cost_p is None:
                cost_p = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                
            item_total = round(float(q) * float(cost_p), 4)
            
            is_consignment = r.get('is_consignment', False)
            if not is_consignment:
                script_realized = round(script_realized + item_total, 4)
                
    print(f"Engine Internal Total: KES {engine_realized:,.2f}")
    print(f"Script Re-calculated: KES {script_realized:,.2f}")
    print(f"Difference: KES {engine_realized - script_realized:,.2f}")
    
    # v10.9: New Exhaustive Mismatch Audit
    print("\nDeep-Scanning for Mismatched Items...")
    total_found_diff = 0.0
    
    # We need to know what the engine THOUGHT was cash vs consignment
    # Since the engine doesn't return the cash/consignment breakdown per-item in its summary,
    # we have to infer it from the flags.
    
    for r in final_recs:
        p_name = r['product_name']
        q = r.get('recommended_quantity', 0)
        if q > 0:
            is_cons = r.get('is_consignment', False)
            cost_p = r.get('cost_price', 0)
            item_total = round(q * cost_p, 2)
            
            # Check if this item is in the 'no_grn_suppliers' list
            supplier = str(r.get('supplier_name', 'UNKNOWN')).upper().strip()
            manual_cons = (supplier in engine.no_grn_suppliers) or ("PLU" in p_name.upper())
            
            if is_cons != manual_cons:
                print(f"  FLAG MISMATCH: {p_name} | Engine is_cons: {is_cons} | Manual check: {manual_cons}")
                if is_cons:
                    total_found_diff -= item_total
                else:
                    total_found_diff += item_total

    print(f"\nTotal Detected Flag Drift: KES {total_found_diff:,.2f}")
    
    print("\nGuard Impact Audit...")
    total_guard_reduction = 0.0
    engine_recs_map = {r['product_name']: r.get('recommended_quantity', 0) for r in raw_recs}
    
    for r in final_recs:
        p_name = r['product_name']
        engine_q = engine_recs_map.get(p_name, 0)
        guard_q = r.get('recommended_quantity', 0)
        
        if guard_q < engine_q:
            cost_p = r.get('cost_price')
            if cost_p is None:
                cost_p = float(engine._get_actual_cost_price(r, float(r.get('selling_price', 0))))
            
            diff_q = engine_q - guard_q
            red_cost = round(diff_q * cost_p, 2)
            if not r.get('is_consignment', False):
                total_guard_reduction += red_cost
                if red_cost > 1000:
                    print(f"  REDUCTION: {p_name} ({engine_q} -> {guard_q}) | Cost: KES {red_cost:,.2f} | Reasoning: {r.get('reasoning')}")

    print(f"\nTotal Guard Reduction (Cash): KES {total_guard_reduction:,.2f}")

if __name__ == "__main__":
    asyncio.run(audit())
