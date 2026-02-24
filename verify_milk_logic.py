
import sys
import os
from pathlib import Path

# Add project root to path
sys.path.insert(0, os.getcwd())

from oasis.logic.order_engine import OrderEngine

def test_milk_logic():
    print("Initializing OrderEngine...")
    engine = OrderEngine(os.path.join(os.getcwd(), 'oasis', 'data'))
    
    # Mock Supplier Patterns for testing
    engine.databases['supplier_patterns'] = {
        "BROOKSIDE DAIRY": {
            "order_frequency": "daily",
            "median_gap_days": 1,
            "estimated_delivery_days": 1
        },
        "GOLD CROWN FOODS": {
            "order_frequency": "weekly",
            "median_gap_days": 7,
            "estimated_delivery_days": 3
        }
    }
    
    # Mock Products
    products = [
        {
            "product_name": "BROOKSIDE FRESH MILK 500ML",
            "supplier_name": "BROOKSIDE DAIRY",
            "avg_daily_sales": 10.0,
            "product_category": "FRESH MILK"
        },
        {
            "product_name": "KCC UHT LONG LIFE MILK 500ML",
            "supplier_name": "GOLD CROWN FOODS", # Assuming they supply this for test
            "avg_daily_sales": 10.0,
            "product_category": "DAIRY"
        },
        {
            "product_name": "DAIRY TOP YOGHURT 150ML",
            "supplier_name": "BROOKSIDE DAIRY",
            "avg_daily_sales": 5.0,
            "product_category": "DAIRY"
        }
    ]
    
    print("\n--- Running Enrichment ---")
    engine.enrich_product_data(products)
    
    tier_profile = {'depth_days': 14}
    
    for p in products:
        name = p['product_name']
        is_fresh = p.get('is_fresh')
        freq = p.get('supplier_frequency')
        
        print(f"\nProduct: {name}")
        print(f"  Is Fresh: {is_fresh}")
        print(f"  Freq: {freq}")
        
        target_days = engine.calculate_replenishment_target_stock(p, tier_profile)
        print(f"  Target Days: {target_days}")
        
        # Assertions
        if "FRESH MILK" in name:
            if not is_fresh or target_days != 1.2:
                print("  [FAIL] Fresh Milk logic incorrectly applied!")
            else:
                print("  [PASS] Fresh Milk Logic Correct (1.2 days)")
                
        if "UHT" in name:
            if is_fresh:
                print("  [FAIL] UHT Milk incorrectly flagged as Fresh!")
            elif target_days <= 1.2:
                print(f"  [FAIL] UHT Milk target too low ({target_days})!")
            else:
                print(f"  [PASS] UHT Milk Logic Correct (Target {target_days:.2f} days)")

if __name__ == "__main__":
    test_milk_logic()
