import asyncio
import logging
import os
from app.logic.order_engine import OrderEngine
from app.llm.inference import RuleBasedLLM

# Setup logging
logging.basicConfig(level=logging.INFO)

async def test_forecast_logic():
    # 1. Initialize Engine (REAL DATA LOAD)
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Pre-load databases (simulating main.py startup)
    engine.load_local_databases()
    
    # 2. Test Product (Fuzzy Name of "151 AIRWICK 375ML FRESH WATER AIR FRESHNER AIR223")
    # Real avg_daily_sales = 0.13
    p_name_input = "Airwick 375ml Fresh Water Air Freshner" 
    
    products = [{
        'product_name': p_name_input,
        'supplier_name': 'Unknown Supplier', # Test if it works without supplier pattern first
        'current_stocks': 0,
        'blocked_open_for_order': 'open',
        'last_days_since_last_delivery': 10
    },
    {
        'product_name': 'Unknown Product (Input Data)',
        'supplier_name': 'Unknown',
        'current_stocks': 0,
        'blocked_open_for_order': 'open',
        'last_days_since_last_delivery': 0,
        'units_sold_last_month': 300 # Should trigger repair: 300/30 = 10 daily
    }]
    
    # 3. Enrich
    print("\n--- 1. Enriching Data ---")
    enriched_products = engine.enrich_product_data(products)
    p = enriched_products[0]
    
    print(f"Product Input: {p_name_input}")
    print(f"Enriched Avg Daily Sales: {p.get('avg_daily_sales')}")
    print(f"Enriched Trend: {p.get('sales_trend')}")
    
    # 4. Analyze
    print("\n--- 2. Running Analysis ---")
    llm = RuleBasedLLM()
    results = await llm.analyze(enriched_products)
    r = results[0]
    
    print(f"Recommendation: {r['recommended_quantity']}")
    print(f"Reason: {r['reasoning']}")
    
    # Check PASS/FAIL
    if "FORECAST" in r['reasoning']:
        print("\n[PASS] Logic correctly prioritized Forecast.")
    else:
        print("\n[FAIL] Logic defaulted to Baseline (Historical/Calcluated).")

if __name__ == "__main__":
    asyncio.run(test_forecast_logic())
