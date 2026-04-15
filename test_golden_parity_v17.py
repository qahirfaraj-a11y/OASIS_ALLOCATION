import os
import sys
import json
from datetime import datetime

# Add scratch to path
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

def test_parity():
    # Mock data dir
    data_dir = os.path.join(os.getcwd(), "test_data")
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)
        
    # Mock SKU Frequency
    with open(os.path.join(data_dir, "sku_grn_frequency.json"), "w") as f:
        json.dump({"BREAD": 1.0, "MILK": 1.0}, f)
        
    engine = OrderEngine(data_dir)
    
    # Mock product
    product = {
        "product_name": "TEST ITEM",
        "avg_daily_sales": 2.1,  # > 2 for 1.2x multiplier
        "estimated_delivery_days": 4,
        "is_fresh": False,
        "product_category": "GENERAL"
    }
    
    # Mock tier profile (small store)
    tier_profile = {
        "tier_name": "Small",
        "depth_days": 7, # This was the old cap
        "max_packs": 10
    }
    
    # Calculate target stock
    # Formula: (4 + 2 + 3) * 1.15 * 1.2 (velocity) = 9 * 1.15 * 1.2 = 12.42
    # Old logic would cap at 7. Golden logic caps at 25.
    target_days = engine.calculate_replenishment_target_stock(product, tier_profile)
    
    print(f"Calculated Target Days: {target_days}")
    assert 12 <= target_days <= 13, f"Expected ~12.42, got {target_days}"
    print("[PASS] Parity Test Passed: Standard item exceeds old profile cap, adhering to Golden 25d limit.")

    # Test Fresh Item
    fresh_product = {
        "product_name": "FRESH MILK",
        "avg_daily_sales": 10.0,
        "estimated_delivery_days": 1,
        "is_fresh": True,
        "product_category": "FRESH"
    }
    # Cycle(1.0) + 1.2 = 2.2 days
    fresh_days = engine.calculate_replenishment_target_stock(fresh_product, tier_profile)
    print(f"Fresh Target Days: {fresh_days}")
    assert 2.1 <= fresh_days <= 2.3, f"Expected 2.2, got {fresh_days}"
    print("[PASS] Parity Test Passed: Fresh item correctly calculated at Cycle + 1.2.")

if __name__ == "__main__":
    try:
        test_parity()
    except Exception as e:
        print(f"ERROR: {e}")
        sys.exit(1)
