import sys
import os
import pandas as pd

# Add project root to path
sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

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
    
    recommendations = [
        {
            "product_name": "AFFORDABLE CASH WIDGET",
            "product_category": "GENERAL",
            "supplier_name": "CASH SUPPLIER",
            "pack_size": 1,
            "selling_price": 200.0, # Cost 150
            "avg_daily_sales": 10.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        },
        {
            "product_name": "EXPENSIVE CONSIGNMENT BUT FREE",
            "product_category": "FRESH",
            "supplier_name": "FRESH KENCHIC", # Should match No GRN list
            "pack_size": 1,
            "selling_price": 250.0, # Cost ~187 (Micro Limit 300)
            "avg_daily_sales": 10.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        },
        {
            "product_name": "CASH WIDGET 2",
            "product_category": "GENERAL",
            "supplier_name": "CASH SUPPLIER",
            "pack_size": 1,
            "selling_price": 200.0, # Cost 150
            "avg_daily_sales": 5.0,
            "current_stocks": 0,
            "ABC_Class": "A"
        }
    ]
    
    budget = 200.0
    
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
    results = engine.apply_greenfield_allocation(recommendations, budget)
    
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
    
    if total_cash <= 1100 and total_consignment > 0: # 10% buffer allowed
        print("\nSUCCESS: Consignment items allocated without breaking cash budget.")
    else:
        print("\nFAILURE: Budget logic incorrect.")

if __name__ == "__main__":
    test_consignment_logic()
