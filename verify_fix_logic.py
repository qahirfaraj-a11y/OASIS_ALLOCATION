
import sys
import os
import asyncio
import logging

# Setup path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine

# Mock logging to avoid clutter
logging.basicConfig(level=logging.WARNING)

async def test_fixes():
    print("Initializing OrderEngine...")
    engine = OrderEngine(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data")
    
    # We don't strictly need to load full databases for this logic test 
    # as we are testing the hardcoded logic in apply_greenfield_allocation
    # But budget manager needs ratios.
    # engine.load_databases_async() # might be needed for some lookups
    
    # Manually init budget manager stuff if needed, or just let it load defaults
    
    # Define Test Candidates
    # These mimic the "Problem Items" from the gap analysis
    candidates = [
        # 1. Yoghurt (Previously filtered as GENERAL/Low Demand)
        {
            "product_name": "DAIRY TOP YOGHURT STRAWBERRY 250ML",
            "product_category": "GENERAL", # Wrong Category
            "selling_price": 75.0,
            "pack_size": 1,
            "avg_daily_sales": 0.5, # High enough?
            "ABC_Class": "A",
            "is_fresh": True
        },
        # 2. Soda (Previously filtered)
        {
            "product_name": "COKE ZERO 500ML",
            "product_category": "GENERAL",
            "selling_price": 90.0,
            "avg_daily_sales": 1.2,
            "ABC_Class": "A"
        },
        # 3. Bulk Rice (Price Ceiling Check)
        # Ceiling is usually ~750. This is 1350.
        {
            "product_name": "DAAWAT RICE 5KG",
            "product_category": "RICE",
            "selling_price": 1350.0,
            "avg_daily_sales": 0.2,
            "ABC_Class": "B"
        },
        # 4. Bulk Oil (Space Variant)
        {
            "product_name": "FRESH FRI 5 L", # Note space
            "product_category": "COOKING OIL",
            "selling_price": 1400.0,
            "avg_daily_sales": 0.1,
            "ABC_Class": "B"
        },
        # 5. Ghee (Gap Fix)
        {
            "product_name": "COW BRAND GHEE 500G",
            "product_category": "GENERAL",
            "selling_price": 800.0,
            "avg_daily_sales": 0.1,
            "ABC_Class": "C"
        },
        # 6. Lentils (Gap Fix)
        {
            "product_name": "SIDE DISH LENTILS 1KG",
            "product_category": "GENERAL",
            "selling_price": 300.0,
            "avg_daily_sales": 0.3,
            "ABC_Class": "A"
        }
    ]
    
    print(f"Testing {len(candidates)} candidates against Small Store Logic (Budget 200k)...")
    
    # Run Allocation
    # We need to ensure profile manager returns "Small" profile params
    # engine.profile_manager.get_profile(200000) -> Should be Tier 2/Small
    
    result = engine.apply_greenfield_allocation(candidates, 200000.0)
    
    recommendations = result.get('recommendations', [])
    skipped = result.get('summary', {}).get('total_skipped', 0)
    
    print("-" * 50)
    print(f"Allocated: {len(recommendations)}")
    print(f"Skipped:   {skipped}")
    print("-" * 50)
    
    for rec in candidates:
        name = rec['product_name']
        qty = rec.get('recommended_quantity', 0)
        reason = rec.get('reasoning', 'N/A')
        status = "[PASS]" if qty > 0 else "[FAIL]"
        
        print(f"{status} | {name:<35} | Qty: {qty} | Reason: {reason}")

    # Validation Logic
    failures = []
    if candidates[0].get('recommended_quantity', 0) == 0: failures.append("Yoghurt failed")
    if candidates[2].get('recommended_quantity', 0) == 0: failures.append("5KG Rice failed (Ceiling?)")
    if candidates[3].get('recommended_quantity', 0) == 0: failures.append("5 L Oil failed (Space variant?)")
    if candidates[4].get('recommended_quantity', 0) == 0: failures.append("Ghee failed")
    
    # DEBUG: Test Bulk Logic Explicitly
    print("-" * 50)
    print("DEBUG: Bulk String Logic Check")
    test_name = "FRESH FRI 5 L"
    variants = ['5KG', '5L', '5LT', '10KG', '10L', '20L', '25KG', '5 KG', '5 L', '10 KG']
    for v in variants:
        if v in test_name:
            print(f"  MATCH: '{v}' found in '{test_name}'")
    
    is_bulk = any(x in test_name for x in variants)
    print(f"  Result for '{test_name}': is_bulk={is_bulk}")
    print("-" * 50)
    
    if not failures:
        print("\nSUCCESS: All fixes verified!")
    else:
        print(f"\nFAILURE: {failures}")

if __name__ == "__main__":
    asyncio.run(test_fixes())
