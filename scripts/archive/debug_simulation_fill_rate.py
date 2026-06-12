
import sys
import os
import pandas as pd
from oasis.logic.order_engine import OrderEngine
from oasis.logic.simulation_bridge import SimulationOrderUtil
from retail_simulator import RetailSimulator, SKUState, STORE_UNIVERSES

# Ensure app path
sys.path.append(os.getcwd())

def get_engine():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    print(f"DEBUG: OrderEngine Source: {OrderEngine.__module__} -> {sys.modules[OrderEngine.__module__].__file__}")
    return OrderEngine(data_path)

def get_bridge():
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    return SimulationOrderUtil(data_path)

def convert_recommendations_to_skustate_debug(recommendations):
    """
    Copy of the fixed logic from integrated_app.py for debugging
    """
    sku_states = []
    bridge = get_bridge() 
    
    print("Enriching data...")
    # Pre-process into list for enrichment
    raw_items = []
    for rec in recommendations:
        if rec['Qty'] > 0:
            raw_items.append({
                'product_name': rec['Product'],
                'department': rec['Department'],
                'avg_daily_sales': rec.get('Avg_Daily_Sales', 0)
            })
            
    # Enrich Data (Bulk Operation)
    enriched_items = bridge.engine.enrich_product_data(raw_items)
    enriched_map = {item['product_name']: item for item in enriched_items}
    
    for rec in recommendations:
        qty = rec['Qty']
        if qty > 0:
            p_name = rec['Product']
            enriched = enriched_map.get(p_name, {})
            
            price = rec['Expected_Revenue'] / qty if qty > 0 else 0
            cost = rec['Allocated_Cost'] / qty if qty > 0 else 0
            ads = rec.get('Avg_Daily_Sales', 0)
            
            sku = SKUState(
                product_name=p_name,
                supplier=enriched.get('supplier_name', "Unknown"),
                department=rec['Department'],
                unit_price=price,
                cost_price=cost,
                avg_daily_sales=ads,
                demand_cv=enriched.get('demand_cv', 0.5),
                lead_time_days=enriched.get('lead_time_days', 4),
                current_stock=qty, 
                is_fresh=enriched.get('is_fresh', False),
                reorder_point_override=enriched.get('reorder_point') # Override with OrderEngine logic
            )
            sku_states.append(sku)
            
    return sku_states

def run_debug():
    budget = 200000
    print(f"Generating Allocation for Budget: ${budget}")
    
    # 1. Generate Allocation
    from allocation_app import load_and_run_allocation
    basket_df, _, _, _ = load_and_run_allocation(budget)
    print(f"Basket Items: {len(basket_df)}")
    
    bridge = get_bridge()

    # 2. Convert & Enrich
    initial_skus = convert_recommendations_to_skustate_debug(basket_df.to_dict('records'))
    
    # Verify Enrichment
    cv_sum = sum(s.demand_cv for s in initial_skus)
    lt_sum = sum(s.lead_time_days for s in initial_skus)
    print(f"Avg CV: {cv_sum/len(initial_skus):.2f}")
    print(f"Avg Lead Time: {lt_sum/len(initial_skus):.1f}")
    
    # 3. Run Simulation
    config = STORE_UNIVERSES["Small_200k"]
    config["budget"] = budget
    
    print("Starting Simulation...")
    
    sim = RetailSimulator("Debug_Run", config, seed=42, bridge=get_bridge(), initial_skus=initial_skus)
    result = sim.run(60)
    
    print(f"\nResults:")
    print(f"Fill Rate: {result.avg_fill_rate:.1f}%")
    print(f"Stockout Rate: {result.stockout_rate:.2f}%")
    
    # 4. Diagnose Top Failures
    print("\nTop 5 Items by Lost Sales:")
    sorted_skus = sorted(result.final_sku_states.values(), key=lambda s: s.lost_sales, reverse=True)[:5]
    for s in sorted_skus:
        print(f" - {s.product_name} | Lost: {s.lost_sales:.0f} | Stockouts: {s.stockout_days} days | LT: {s.lead_time_days} | CV: {s.demand_cv}")
        # Investigate why?
        coverage = s.current_stock / s.avg_daily_sales if s.avg_daily_sales > 0 else 99
        print(f"   Debug: Final Stock {s.current_stock}, ADS {s.avg_daily_sales:.2f}, Supplier {s.supplier}")

if __name__ == "__main__":
    run_debug()
