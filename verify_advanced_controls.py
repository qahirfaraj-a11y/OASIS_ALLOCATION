import asyncio
import logging
import os
import json
from app.logic.order_engine import OrderEngine
from app.llm.inference import RuleBasedLLM

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyAdvancedControls")

async def test_advanced_controls():
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Mock data for testing
    products = [
        # 1. Overstocked Item (No Reason -> Should be 0)
        {
            'product_name': 'OVERSTOCKED ITEM (DRY)',
            'current_stocks': 1000,
            'avg_daily_sales': 10, # 100 days cover
            'upper_coverage_days': 45,
            'is_key_sku': True,
            'demand_cv': 0.1,
            'is_promo': False,
            'moq_floor': 0
        },
        # 2. Overstocked Item (Promo Reason -> Should be Recommended)
        {
            'product_name': 'OVERSTOCKED ITEM (PROMO)',
            'current_stocks': 1000,
            'avg_daily_sales': 10, # 100 days cover
            'upper_coverage_days': 45,
            'is_promo': True, # Overrides Guard
            'is_key_sku': True,
            'demand_cv': 0.1,
            'moq_floor': 0
        },
        # 3. New Item (Lookalike Cycle 1)
        {
            'product_name': '7UP CHERRY 500ML', # "7UP" brand exist in DB
            'current_stocks': 0,
            'is_lookalike_forecast': True,
            'lookalike_demand': 5.0, # Median from 7UP family
            'avg_daily_sales': 5.0,
            'order_cycle_count': 0,
            'new_item_aggression_cap': 21,
            'is_fresh': False
        },
        # 4. New Item (Fresh Aggression Cap)
        {
            'product_name': 'NEW MILK POUCH',
            'current_stocks': 0,
            'is_lookalike_forecast': True,
            'lookalike_demand': 100.0,
            'avg_daily_sales': 100.0,
            'order_cycle_count': 0,
            'new_item_aggression_cap': 7,
            'is_fresh': True
        }
    ]
    
    # Enrichment simulation
    for p in products:
        p['selling_price'] = 100.0
        p['last_days_since_last_delivery'] = 5
        p['reorder_point'] = p.get('avg_daily_sales', 0) * 7

    print("\n--- Testing Advanced Inventory Controls Decision Matrix ---\n")
    
    llm = RuleBasedLLM()
    recommendations = await llm.analyze(products)
    
    for rec in recommendations:
        name = rec.get('product_name')
        qty = rec.get('recommended_quantity')
        reason = rec.get('reasoning')
        print(f"Product: {name}")
        # Find original data
        orig = next(p for p in products if p['product_name'] == name)
        if orig.get('current_stocks', 0) > 0 and orig.get('avg_daily_sales', 0) > 0:
            cover = orig['current_stocks'] / orig['avg_daily_sales']
            print(f"  Current Coverage: {cover:.1f} days")
        
        print(f"  Rec Qty: {qty}")
        print(f"  Reasoning: {reason}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(test_advanced_controls())
