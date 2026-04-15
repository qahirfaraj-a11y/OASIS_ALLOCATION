
import sys
import os
import json
import asyncio
import math

# Setup path to include the current directory
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

async def test_high_budget_allocation():
    print("Testing 27.6M Budget Allocation (Variety vs Depth)...")
    
    # Create mock data directory
    data_dir = "test_data"
    os.makedirs(data_dir, exist_ok=True)
    
    engine = OrderEngine(data_dir)
    # Mock databases
    engine.databases = {
        'supplier_patterns': {},
        'product_supplier_map': {},
        'sales_forecasting': {f"Product_{i}": {"avg_daily_sales": 10.0} for i in range(100)},
        'supplier_quality': {},
        'sales_profitability': {},
        'simulation_feedback': {}
    }
    engine.no_grn_suppliers = []
    
    # Test Greenfield Allocation (Initial Load)
    # 27.6M Budget
    total_budget = 27_600_000
    
    print(f"Running allocation for {total_budget} budget...")
    
    # Create 200 items to test variety
    recommendations = []
    for i in range(200):
        # Items 0-100 are Staples, 101-200 are General
        is_staple = i < 100
        recommendations.append({
            "product_name": f"Product_{i}",
            "avg_daily_sales": 50.0 if i < 10 else 2.0, # Some high-velocity, some low
            "selling_price": 100.0,
            "pack_size": 12,
            "current_stocks": 0.0,
            "supplier_name": "SUPPLIER_A",
            "product_category": "SUGAR" if i < 10 else "GENERAL",
            "reasoning": "",
            "ABC_Class": "A" if i < 50 else "B"
        })
    
    # Mock budget manager staple check
    engine.budget_manager.is_staple = lambda name, cat, ads: cat == "SUGAR"
    
    print("\n--- Testing 27.6M Budget: Variety Priority ---")
    res = engine.apply_greenfield_allocation(recommendations, total_budget=total_budget)
    recs = res['recommendations']
    summary = res['summary']
    
    # Verify variety (Breadth)
    total_allocated = sum(1 for r in recs if r['recommended_quantity'] > 0)
    print(f"Total Unique SKUs Allocated: {total_allocated}/{len(recommendations)}")
    
    # Verify Pass 1 logic (MDQ focus)
    pass1_recs = [r for r in recs if r.get('pass1_allocated')]
    print(f"Items allocated in Pass 1: {len(pass1_recs)}")
    
    # Check quantities of high-velocity items after Pass 1
    # Note: Pass 1 now only does MDQ (Mega min_display_qty = 24)
    hv_item = next(r for r in recs if r['product_name'] == "Product_0")
    print(f"High Velocity Item (Product_0) Qty: {hv_item['recommended_quantity']} units")
    
    # Verify that it exceeded MDQ (it should be in Pass 2 since it's a staple)
    # MDQ for Mega interpolated around 27M should be ~15-20. 
    # Let's check profile_manager min_display_qty interpolation
    profile = engine.profile_manager.get_profile(total_budget)
    print(f"Profile MDQ: {profile['min_display_qty']}, Max Packs: {profile['max_packs']}")
    
    # Logic: Pass 1 buys MDQ. Pass 2 buys depth.
    # Product_0 (50 ADS) target coverage 14 days = 700 units.
    # Pass 1 buys max(MDQ, Pack) = 24.
    # Pass 2 buys up to 700.
    
    if hv_item['recommended_quantity'] >= 700:
        print(f"SUCCESS: High velocity item invested to depth: {hv_item['recommended_quantity']}")
    else:
        print(f"FAILURE: High velocity item not invested enough: {hv_item['recommended_quantity']}")
        
    # Verify that variety items (low velocity) were not skipped
    lv_item = next(r for r in recs if r['product_name'] == "Product_150")
    if lv_item['recommended_quantity'] > 0:
        print(f"SUCCESS: Low velocity variety item allocated: {lv_item['recommended_quantity']}")
    else:
        print(f"FAILURE: Low velocity variety item skipped.")

if __name__ == "__main__":
    asyncio.run(test_high_budget_allocation())
