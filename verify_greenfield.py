import os
import sys
import logging
import json

# Add scratch to python path
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine

logging.basicConfig(level=logging.INFO)

def main():
    engine = OrderEngine(data_dir=os.path.join(os.path.dirname(__file__), "data"))
    
    # Needs some basic databases for it to not crash
    engine.databases['supplier_patterns'] = {}
    engine.databases['sales_forecasting'] = {}
    engine.databases['supplier_quality'] = {}
    engine.databases['sales_profitability'] = {}
    
    # Mock some data for greenfield
    test_products = [
        {
            "product_name": "TEST MILK",
            "item_code": "001",
            "barcode": "111",
            "current_stocks": 0,
            "units_sold_last_month": 300, # 10/day
            "supplier_name": "DAIRY CO",
            "pack_size": 1,
            "selling_price": 50.0, # staple
            "department": "FRESH MILK",
            "product_category": "FRESH MILK"
        },
        {
            "product_name": "TEST DISCRETIONARY",
            "item_code": "002",
            "barcode": "222",
            "current_stocks": 0,
            "units_sold_last_month": 30, # 1/day
            "supplier_name": "SNACK CO",
            "pack_size": 12,
            "selling_price": 200.0,
            "department": "SNACKS",
            "product_category": "SNACKS"
        }
    ]
    
    enriched = engine.enrich_product_data(test_products)
    
    print("\n--- Running Greenfield Allocation ---")
    results = engine.apply_greenfield_allocation(enriched, total_budget=150000.0)
    
    for r in results['recommendations']:
        print(f"Product: {r['product_name']}, Qty: {r.get('recommended_quantity', 0)}, Reason: {r.get('reasoning', '')}")
        
    print("\nSummary:")
    print(json.dumps(results['summary'], indent=2))

if __name__ == "__main__":
    main()
