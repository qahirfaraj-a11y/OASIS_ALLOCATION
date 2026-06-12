import asyncio
import logging
from app.llm.inference import RuleBasedLLM

logging.basicConfig(level=logging.INFO)

async def verify_fresh_logic():
    llm = RuleBasedLLM()
    
    products = [
        # 1. Stale Fresh (Category based, 125 days) -> Should be 0
        {
            'product_name': 'FRESH MILK 1L',
            'is_fresh': True,
            'last_days_since_last_delivery': 125,
            'current_stocks': 0,
            'avg_daily_sales': 10,
            'abc_rank': 'A'
        },
        # 2. Stale Fresh (Daily Supplier based, 150 days) -> Should be 0
        {
            'product_name': 'ANY DAILY PRODUCT',
            'is_fresh': True, # Derived in OrderEngine
            'last_days_since_last_delivery': 150,
            'current_stocks': 0,
            'avg_daily_sales': 5,
            'abc_rank': 'B'
        },
        # 3. Dry Slow Mover (210 days) -> Should be 12 (Capped)
        {
            'product_name': 'DRY BISCUITS',
            'is_fresh': False,
            'last_days_since_last_delivery': 210,
            'current_stocks': 2,
            'avg_daily_sales': 1,
            'abc_rank': 'C'
        },
        # 4. Dry "In-Between" (150 days) -> Normal logic (not zeroed out)
        {
            'product_name': 'DRY PASTA',
            'is_fresh': False,
            'last_days_since_last_delivery': 150,
            'current_stocks': 5,
            'avg_daily_sales': 2,
            'abc_rank': 'B',
            'historical_avg_order_qty': 24
        }
    ]
    
    print("\n--- Verifying Dynamic Freshness & 120-Day Stale Rule ---\n")
    results = await llm.analyze(products)
    
    for r in results:
        print(f"Product: {r['product_name']}")
        print(f"  Rec Qty: {r['recommended_quantity']}")
        print(f"  Reasoning: {r['reasoning']}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(verify_fresh_logic())
