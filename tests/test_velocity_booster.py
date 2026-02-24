import sys
import os
import asyncio
from unittest.mock import MagicMock

sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

# Mock data: High velocity items vs Low velocity
MOCK_PRODUCTS = [
    # High Velocity Item (Should get booster)
    {"product_name": "Fast Water 500ml", "avg_daily_sales": 50, "selling_price": 50, "pack_size": 12, 
     "product_category": "MINERAL WATER", "is_fresh": False, "ABC_Class": "A", "reliability_score": 95, "demand_cv": 0.1},
    
    # Moderate Velocity (Borderline?)
    {"product_name": "Medium Soda", "avg_daily_sales": 5, "selling_price": 60, "pack_size": 24, 
     "product_category": "SODA", "is_fresh": False, "ABC_Class": "A", "reliability_score": 95, "demand_cv": 0.1},
     
    # Low Velocity (No booster)
    {"product_name": "Slow Spice", "avg_daily_sales": 0.5, "selling_price": 200, "pack_size": 6, 
     "product_category": "SPICES", "is_fresh": False, "ABC_Class": "C", "reliability_score": 95, "demand_cv": 0.1},
]

async def run_test():
    engine = OrderEngine(os.getcwd())
    
    # Mock budget manager and profile manager basics to avoid full setup
    engine.profile_manager.get_profile = MagicMock(return_value={
        'tier_name': 'Test Tier',
        'is_small': False,
        'depth_days': 10, # Base depth
        'max_packs': 100,
        'price_ceiling': 5000,
        'min_display_qty': 1,
        'allow_c_class': True,
        'wallet_buffer_pct': 0.1
    })
    
    # Run allocation with enough budget to trigger Pass 2
    result = engine.apply_greenfield_allocation(MOCK_PRODUCTS.copy(), 100_000)
    
    print("="*60)
    print("TESTING VELOCITY BOOSTER")
    print("="*60)
    
    for r in result['recommendations']:
        name = r['product_name']
        qty = r['recommended_quantity']
        reason = r['reasoning']
        
        print(f"Product: {name}")
        print(f"  Qty: {qty}")
        print(f"  Reason: {reason}")
        
        if "Fast Water" in name:
            if "VELOCITY SAFETY" in reason or "High Velocity" in reason:
                print("  [PASS] Booster Applied")
            else:
                print("  [FAIL] Booster NOT Applied")
                
        print("-" * 20)

if __name__ == "__main__":
    asyncio.run(run_test())
