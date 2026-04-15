import asyncio
import os
import sys

# Add logic directory to path for testing
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine

async def test_engine_init():
    print("--- [O.A.S.I.S.] Final Sanity Check ---")
    data_dir = os.path.abspath("./data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        print(f"Created dummy data dir: {data_dir}")
        
    engine = OrderEngine(data_dir)
    print("SUCCESS: OrderEngine initialized successfully.")
    
    mock_products = [
        {"product_name": "TEST ITEM 1", "category": "GENERAL", "abc_rank": "A", "avg_daily_sales": 5.0, "current_stocks": 0, "is_fresh": False, "target_coverage_days": 14, "cost_price": 10},
        {"product_name": "TEST ITEM 2", "category": "GENERAL", "abc_rank": "C", "avg_daily_sales": 0.5, "current_stocks": 10, "is_fresh": False, "target_coverage_days": 14, "cost_price": 5},
    ]
    
    from oasis.llm.inference import RuleBasedLLM
    llm = RuleBasedLLM()
    # Enrich first
    enriched = engine.enrich_product_data(mock_products)
    results = await llm.analyze(enriched)
    for r in results:
        print(f"Result for {r['product_name']}: Qty={r['recommended_quantity']}, Reason={r['reasoning']}")

if __name__ == "__main__":
    asyncio.run(test_engine_init())
