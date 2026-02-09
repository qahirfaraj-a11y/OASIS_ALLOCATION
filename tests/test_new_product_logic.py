import sys
import os
import asyncio

sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

# Mock data: Mix of established and new products
MOCK_PRODUCTS = [
    # Established high-seller
    {"product_name": "Established Winner", "avg_daily_sales": 50, "selling_price": 100, "pack_size": 1, "product_category": "SUGAR", "ABC_Class": "A", "reliability_score": 95, "demand_cv": 0.2},
    
    # New product with lookalike
    {"product_name": "New Product (Lookalike)", "avg_daily_sales": 0, "selling_price": 120, "pack_size": 1, "product_category": "GENERAL", "lookalike_demand": 10.0, "ABC_Class": "A", "reliability_score": 90, "is_fresh": False},
    
    # New product without lookalike
    {"product_name": "New Product (Baseline)", "avg_daily_sales": 0, "selling_price": 80, "pack_size": 1, "product_category": "GENERAL", "ABC_Class": "B", "reliability_score": 85, "is_fresh": False},
    
    # New fresh product
    {"product_name": "New Fresh Item", "avg_daily_sales": 0, "selling_price": 150, "pack_size": 1, "product_category": "FRESH MILK", "ABC_Class": "A", "is_fresh": True},
]

async def run_test():
    engine = OrderEngine(os.getcwd())
    
    print("--- v2.5 New Product & Budget Redistribution Test ---\n")
    
    # Mock enrichment
    for p in MOCK_PRODUCTS:
        p['is_consignment'] = False
        
    # Run allocation with moderate budget
    result = engine.apply_greenfield_allocation(MOCK_PRODUCTS.copy(), 500_000)
    res = result['recommendations']  # Extract recommendations from returned dict
    
    print("Allocation Results:")
    print("-" * 100)
    for r in res:
        name = r['product_name']
        qty = r['recommended_quantity']
        sales = r.get('avg_daily_sales', 0)
        reason = r['reasoning']
        status = "ALLOCATED" if qty > 0 else "SKIPPED"
        print(f"{name:30} | {status:10} | Qty: {qty:3} | Sales: {sales:4.1f}/day | {reason}")
    
    print("\n" + "="*100)
    print("Verification Points:")
    print("1. New products should show '[NEW PRODUCT:...]' tag")
    print("2. New products should have conservative quantities (7-14 days max)")
    print("3. High priority items may show '[PASS 2B: REDISTRIBUTED]' if budget was reallocated")

if __name__ == "__main__":
    asyncio.run(run_test())
