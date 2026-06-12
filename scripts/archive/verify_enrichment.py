import asyncio
import logging
from app.logic.order_engine import OrderEngine

# Setup logging
logging.basicConfig(level=logging.INFO)

async def test_enrichment():
    import os
    # Point to the real data directory or a dummy one
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Mock databases with simple data if load fails or checks specific logic
    # But better to test actual load if possible. 
    # Let's assume databases load from disk as per OrderEngine.__init__
    
    # Determine absolute path for data if needed or rely on relative
    import os
    print(f"CWD: {os.getcwd()}")
    
    # Create mock products that need fuzzy matching
    products = [
        {'product_name': 'Bread White 400g', 'supplier_name': 'Sup A'}, # DB might have "White Bread 400g"
        {'product_name': 'Milk 500ml', 'supplier_name': 'Sup B'},
        {'product_name': 'Tomatos', 'supplier_name': 'Sup C'}, # Misspelled
    ]
    
    # We need to manually inject data into engine.databases if we don't want to rely on real files
    # Or just let it load. Let's try to inject for determinstic test first.
    engine.databases = {
        'product_intelligence': {
            'White Bread 400g': {'suppliers': {'Sup A': {'orders': 100, 'qty_ordered': 5000}}}, # Avg 50
            'Milk 500ml': {'suppliers': {'Sup B': {'orders': 10, 'qty_ordered': 100}}}, # Avg 10
            'Tomato': {'suppliers': {'Sup C': {'orders': 50, 'qty_ordered': 250}}} # Avg 5
        },
        'supplier_patterns': {},
        'sales_forecasting': {},
        'supplier_quality': {}
    }
    
    print("\n--- Testing Enrichment Logic ---\n")
    enriched = engine.enrich_product_data(products)
    
    for p in enriched:
        print(f"Product: {p['product_name']}")
        print(f"  Hist Avg: {p.get('historical_avg_order_qty')}")
        print(f"  Confidence: {p.get('confidence_level')}")
        print("-" * 30)

if __name__ == "__main__":
    asyncio.run(test_enrichment())
