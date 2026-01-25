import sys
import os
import asyncio
import pandas as pd

sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

# Mock data
# Same Sales, Same Price, Different Risk Profiles
MOCK_PRODUCTS = [
    {
        "product_name": "Safe Item (Stable)", 
        "avg_daily_sales": 1, "selling_price": 100, "pack_size": 1, 
        "product_category": "GENERAL",
        "reliability_score": 95, "demand_cv": 0.2
    },
    {
        "product_name": "Risky Item (Unreliable)", 
        "avg_daily_sales": 1, "selling_price": 100, "pack_size": 1, 
        "product_category": "GENERAL",
        "reliability_score": 50, "demand_cv": 0.2
    },
    {
        "product_name": "Volatile Item (High CV)", 
        "avg_daily_sales": 1, "selling_price": 100, "pack_size": 1, 
        "product_category": "GENERAL",
        "reliability_score": 95, "demand_cv": 0.9
    }
]

async def run_simulation():
    engine = OrderEngine(os.getcwd())
    
    print("--- Smart Depth Validation ---")
    
    # Enrich (Mocking enrichment to keep our values)
    # We will just pass our mock directly since we pre-set the keys
    enriched = MOCK_PRODUCTS # Normally enrichment adds these keys, we added them manually
    for p in enriched:
        p['is_consignment'] = False # Ensure tested as cash items
        p['pass1_allocated'] = True # Force into Pass 2
        p['recommended_quantity'] = 1 # Start with random pass 1 qty
        p['ABC_Class'] = 'A'
    
    # Run Allocation with ample budget to check Depth Limits
    # Budget 1M -> "Mini-Mart" -> Depth 14 Days
    res = engine.apply_greenfield_allocation(enriched.copy(), 1_000_000)
    
    print(f"\nResults (Base Store Depth: 14 Days):")
    for r in res:
        name = r['product_name']
        qty = r['recommended_quantity']
        # Days Covered = Qty / Daily Sales (10)
        days = qty / 10.0
        reason = r['reasoning']
        print(f"- {name}: {qty} Units ({days} Days) | {reason}")

if __name__ == "__main__":
    asyncio.run(run_simulation())
