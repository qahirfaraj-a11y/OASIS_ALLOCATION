import asyncio
import logging
import os
import json
from app.logic.order_engine import OrderEngine

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyProfitability")

async def test_profitability_enrichment():
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Load databases
    await engine.load_databases_async()
    
    # Test products: 
    # 1. Exact match for top SKU
    # 2. Fuzzy match for top SKU
    # 3. Non-top SKU
    products = [
        {'product_name': 'V22 BAG 10 X 15 X 22 30G', 'supplier_name': 'UNKNOWN'},
        {'product_name': 'CHANDARANA CARRIER BAG GREEN', 'supplier_name': 'UNKNOWN'},
        {'product_name': 'RANDOM ITEM XYZ', 'supplier_name': 'UNKNOWN'},
    ]
    
    print("\n--- Testing Top 500 SKU Enrichment ---\n")
    enriched = engine.enrich_product_data(products)
    
    for p in enriched:
        print(f"Product: {p.get('product_name')}")
        print(f"  Sales Rank: {p.get('sales_rank')}")
        print(f"  Margin %: {p.get('margin_pct')}")
        print(f"  Is Top SKU: {p.get('is_top_sku')}")
        print("-" * 30)

if __name__ == "__main__":
    asyncio.run(test_profitability_enrichment())
