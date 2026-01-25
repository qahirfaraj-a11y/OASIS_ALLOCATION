import asyncio
import logging
import os
import json
from app.logic.order_engine import OrderEngine

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyV2Logic")

async def test_v2_logic():
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Load databases (minimal for speed)
    await engine.load_databases_async()
    
    # Test products: 
    # 1. Top SKU with stock slightly above reorder point (Test Anti-Conservatism)
    # 2. Item with MOQ floor (Test Floor respect)
    # 3. Fast mover needing safety buffer (Test Net Requirement)
    products = [
        {
            'product_name': 'BROOKSIDE 500ML DAIRY BEST (POUCH)', 
            'supplier_name': 'BROOKSIDE DAIRY LIMITED',
            'current_stocks': 150,  # Slightly above reorder point
            'units_sold_last_month': 3000,
            'last_days_since_last_delivery': 2
        },
        {
            'product_name': 'DETTOL 50ML', 
            'supplier_name': 'UNILEVER',
            'current_stocks': 5,
            'units_sold_last_month': 60,
            'last_days_since_last_delivery': 10
        }
    ]
    
    # Manually inject some constraints for testing
    enriched = engine.enrich_product_data(products)
    enriched[1]['moq_floor'] = 24  # Set a floor for Dettol
    
    print("\n--- Testing v2 Logic Analysis ---\n")
    # Only analyze batch of 2
    recommendations = await engine.analyze_batch_ai(enriched, 1, 1)
    
    for rec in recommendations:
        name = rec.get('product_name')
        qty = rec.get('recommended_order_qty')
        reason = rec.get('reasoning')
        print(f"Product: {name}")
        print(f"  Rec Qty: {qty}")
        print(f"  Reasoning: {reason}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(test_v2_logic())
