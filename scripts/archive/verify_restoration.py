import sys
import os
from datetime import datetime

# Add scratch to path
sys.path.append(r'C:\Users\iLink\.gemini\antigravity\scratch')

from oasis.logic.order_engine import OrderEngine

def test_replenishment_precision():
    engine = OrderEngine(r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data')
    
    # Mock some data for verification
    product_high_velocity = {
        'product_name': 'INDOMIE CHICKEN 70G',
        'avg_daily_sales': 15.0, # High velocity (>10) -> 1.4x
        'estimated_delivery_days': 4.0,
        'is_fresh': False
    }
    
    # Tier profile (not small)
    profile_large = {'is_small': False, 'depth_days': 14.0}
    
    # Base = (4 + 2 + 3) * 1.15 = 10.35
    # Velocity Multiplier = 1.4
    # Expected = 10.35 * 1.4 = 14.49
    
    target_large = engine.calculate_replenishment_target_stock(product_high_velocity, profile_large)
    print(f"High Velocity Target (Large Store): {target_large:.2f} (Expected: ~14.49)")
    
    # Test Fresh with Stockout History
    # We need to mock simulation_feedback
    engine.databases['simulation_feedback'] = {
        'sku_feedback': {
            'FRESH MILK 500ML': {'stockout_frequency': 0.5, 'avg_first_stockout_day': 4.0}
        }
    }
    
    product_fresh_stockout = {
        'product_name': 'FRESH MILK 500ML',
        'avg_daily_sales': 2.0, # Med-high velocity -> 1.2x
        'estimated_delivery_days': 1.0, # Fresh usually daily
        'is_fresh': True
    }
    
    # Base = (1 + 2 + 3) * 1.15 = 6.9
    # Velocity Multiplier = 1.2
    # Pre-boost Target = 6.9 * 1.2 = 8.28
    # Stockout boost (v9.6) = 8.28 * 1.2 = 9.936
    # Fresh Cap (Relaxed) = 6.0
    # Expected = 6.0 (due to cap)
    
    target_fresh = engine.calculate_replenishment_target_stock(product_fresh_stockout, profile_large)
    print(f"Fresh Stockout Target: {target_fresh:.2f} (Expected: ~6.00 due to cap relaxation)")

    # Test UHT Floor
    product_uht = {
        'product_name': 'BROOKSIDE UHT MILK 1L',
        'avg_daily_sales': 0.5, # Low velocity -> 0.8x
        'estimated_delivery_days': 1.0,
        'is_fresh': True # Treated as fresh for loop
    }
    
    # Base = (1 + 2 + 3) * 1.15 = 6.9
    # Velocity Multiplier = 0.8
    # Pre-boost Target = 6.9 * 0.8 = 5.52
    # UHT Floor (v8.1) = max(7.0, 5.52) = 7.0
    # Expected = 7.0
    
    target_uht = engine.calculate_replenishment_target_stock(product_uht, profile_large)
    print(f"UHT Floor Target: {target_uht:.2f} (Expected: ~7.00)")

    # Test Category Boosts
    products = [
        {'product_name': 'LOLLIPOP GIANT 10G', 'estimated_daily_sales': 1.0, 'estimated_delivery_days': 4.0, 'is_fresh': False},
        {'product_name': 'KENSALT 1KG', 'estimated_daily_sales': 5.0, 'estimated_delivery_days': 4.0, 'is_fresh': False}
    ]
    # Lollipops get 2.5x boost
    # Kensalt gets 1.4x boost
    
    enriched = engine.enrich_product_data(products)
    for p in enriched:
        name = p['product_name']
        boosted = p['target_coverage_days']
        boost = p.get('category_boost', 1.0)
        print(f"Product: {name} | Boosted Coverage: {boosted} | Multiplier: {boost}")

if __name__ == "__main__":
    try:
        test_replenishment_precision()
    except Exception as e:
        print(f"Verification Failed: {e}")
