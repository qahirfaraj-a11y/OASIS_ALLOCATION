import sys
import os
import asyncio
import pandas as pd
import time

sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

# Mock data
MOCK_PRODUCTS = [
    {"product_name": "Product A", "avg_daily_sales": 10, "selling_price": 250, "pack_size": 1, "product_category": "GENERAL"},
    {"product_name": "Product B (Staple)", "avg_daily_sales": 50, "selling_price": 100, "pack_size": 12, "product_category": "SUGAR"},
    {"product_name": "Product C (Expensive)", "avg_daily_sales": 2, "selling_price": 5000, "pack_size": 1, "product_category": "ELECTRONICS"},
    {"product_name": "Product D (Micro Cap)", "avg_daily_sales": 5, "selling_price": 400, "pack_size": 1, "product_category": "GENERAL"},
]

async def run_simulation():
    engine = OrderEngine(os.getcwd())
    
    # 1. Test Dynamic Tiers
    print("--- 1. Dynamic Tier Simulation ---")
    budgets = [150_000, 250_000, 1_000_000, 10_000_000, 200_000_000]
    
    for b in budgets:
        profile = engine.profile_manager.get_profile(b)
        print(f"Budget: ${b:,.0f} -> Tier: {profile['tier_name']}")
        print(f"  Ceiling: {profile['price_ceiling']}, Depth: {profile['depth_days']}d, Max Packs: {profile['max_packs']}")
        
    # 2. Test Allocation Logic (Bypass AI)
    print("\n--- 2. Logic Verification (AI Bypass Mode) ---")
    engine.load_no_grn_suppliers()
    
    # Mock databases for enrichment
    engine.databases['supplier_patterns'] = {}
    
    # Enrich
    enriched = engine.enrich_product_data(MOCK_PRODUCTS)
    
    # Run Allocation with Micro Budget (should trigger strict logic)
    res_micro = engine.apply_greenfield_allocation(enriched.copy(), 150_000)
    print(f"\nMicro Store Results ($150k):")
    for r in res_micro:
        if r.get('recommended_quantity', 0) > 0:
            print(f"- {r['product_name']}: {r['recommended_quantity']} ({r['reasoning']})")
        else:
            print(f"- {r['product_name']}: SKIPPED ({r['reasoning']})")
            
    # Run Allocation with Mid Budget (should allow expensive items)
    res_mid = engine.apply_greenfield_allocation(enriched.copy(), 10_000_000)
    print(f"\nMid Store Results ($10M):")
    for r in res_mid:
        if r.get('recommended_quantity', 0) > 0:
            print(f"- {r['product_name']}: {r['recommended_quantity']} ({r['reasoning']})")
        else:
            print(f"- {r['product_name']}: SKIPPED ({r['reasoning']})")

if __name__ == "__main__":
    asyncio.run(run_simulation())
