import asyncio
import logging
from app.llm.inference import RuleBasedLLM

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyV2RuleEngine")

async def test_v2_rule_engine():
    llm = RuleBasedLLM()
    
    # Test Scenarios
    products = [
        {
            "product_name": "KEY SKU - LOW STOCK",
            "is_key_sku": True,
            "current_stocks": 10,
            "reorder_point": 20,
            "historical_avg_order_qty": 50,
            "avg_daily_sales": 5,
            "sales_trend": "stable",
            "is_fresh": False,
            "last_days_since_last_delivery": 10,
            "moq_floor": 24
        },
        {
            "product_name": "KEY SKU - ADEQUATE STOCK (TEST ANTI-CONSERVATISM)",
            "is_key_sku": True,
            "current_stocks": 25, # Above reorder point
            "reorder_point": 20,
            "historical_avg_order_qty": 50,
            "avg_daily_sales": 5,
            "sales_trend": "stable",
            "is_fresh": False,
            "last_days_since_last_delivery": 10,
            "moq_floor": 12
        },
        {
            "product_name": "REGULAR SKU - HIGH STOCK",
            "is_key_sku": False,
            "current_stocks": 50,
            "reorder_point": 20,
            "historical_avg_order_qty": 50,
            "avg_daily_sales": 5,
            "sales_trend": "stable",
            "is_fresh": False,
            "last_days_since_last_delivery": 10
        }
    ]
    
    print("\n--- Testing v2 Rule Engine Logic ---\n")
    results = await llm.analyze(products)
    
    for r in results:
        print(f"Product: {r['product_name']}")
        print(f"  Rec Qty: {r['recommended_quantity']}")
        print(f"  Reasoning: {r['reasoning']}")
        print("-" * 60)

if __name__ == "__main__":
    asyncio.run(test_v2_rule_engine())
