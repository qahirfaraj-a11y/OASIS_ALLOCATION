import sys
import os
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

def test_consignment_logic():
    engine = OrderEngine(os.getcwd())
    
    # Mock Recommendations
    # 1. Cash Item (Expensive)
    # 2. Consignment Item (Expensive)
    # 3. Cash Item (Cheap)
    
    # Budget: $200
    # Cash Item 1: $150 Cost -> Fits
    # Consignment Item: $500 Cost -> Should fit (Free)
    # Cash Item 2: $150 Cost -> Should FAIL (Cash > 200)
    
    # NOTE: with a micro budget (<200k) Pass 1 admits ONLY essentials/staples/
    # A-class — and enrich_product_data recomputes ABC_Class (demoting these tiny
    # fixtures to B). Use ESSENTIAL departments so the items qualify and the
    # consignment-vs-cash budget logic is actually exercised.
    recommendations = [
        {
            "product_name": "AFFORDABLE CASH WIDGET",
            "product_category": "BREAD",           # essential dept
            "supplier_name": "CASH SUPPLIER",
            "pack_size": 1,
            "selling_price": 800.0, # order value must clear the 1500 anchor-prune floor
            "avg_daily_sales": 10.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        },
        {
            "product_name": "EXPENSIVE CONSIGNMENT BUT FREE",
            "product_category": "FRESH MILK",      # essential dept
            "supplier_name": "FRESH KENCHIC", # Should match No GRN list
            "pack_size": 1,
            "selling_price": 500.0, # expensive: proves consignment bypasses the cash budget
            "avg_daily_sales": 10.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        },
        {
            "product_name": "CASH WIDGET 2",
            "product_category": "BREAD",           # essential dept
            "supplier_name": "CASH SUPPLIER",
            "pack_size": 1,
            "selling_price": 800.0, # order value must clear the 1500 anchor-prune floor
            "avg_daily_sales": 5.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        }
    ]
    
    budget = 50_000.0   # realistic scale: anchor prune (>=1500/order) + 85% cap apply
    
    # Pre-enrich to make sure flags are set if engine doesn't automatically do it inside alloc
    # Actually engine.apply_greenfield_allocation expects enriched data usually? 
    # Or does it trust the input? 
    # The input to apply_greenfield_allocation is usually fresh AI output.
    # We should run enrich first.
    
    # Mock databases for enrichment
    engine.databases['supplier_patterns'] = {}
    engine.load_no_grn_suppliers()
    print(f"Loaded No GRN Suppliers: {engine.no_grn_suppliers}")
    
    engine.enrich_product_data(recommendations)
    
    print("\n--- Enriched items ---")
    for r in recommendations:
        print(f"{r['product_name']}: Is Consignment? {r.get('is_consignment')}")
        
    print("\n--- Running Allocation (Budget: 1000) ---")
    # apply_greenfield_allocation returns {"recommendations": [...], "summary": {...}}
    # since the pass decomposition — iterate the recommendations list.
    results = engine.apply_greenfield_allocation(recommendations, budget)["recommendations"]

    print("\n--- Results ---")
    total_cash = 0
    total_consignment = 0

    for r in results:
        qty = r.get('recommended_quantity', 0)
        cost = qty * r['selling_price'] * 0.75
        is_con = r.get('is_consignment', False)
        print(f"{r['product_name']}: Qty {qty}, Cost {cost}, Type: {'CONSIGNMENT' if is_con else 'CASH'}")
        print(f"  Reason: {r.get('reasoning')}")
        
        if qty > 0:
            if is_con: total_consignment += cost
            else: total_cash += cost
            
    print(f"\nTotal Cash Used: {total_cash}")
    print(f"Total Consignment: {total_consignment}")
    
    # consignment must be allocated without consuming the cash budget
    assert total_cash <= budget * 1.1, f"cash budget breached: {total_cash}"  # 10% buffer
    assert total_consignment > 0, "no consignment items were allocated"
    print("\nSUCCESS: Consignment items allocated without breaking cash budget.")

if __name__ == "__main__":
    test_consignment_logic()
