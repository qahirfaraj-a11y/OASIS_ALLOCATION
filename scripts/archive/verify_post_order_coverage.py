import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_post_order_coverage():
    """Test post-order coverage awareness in soft guard"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("POST-ORDER COVERAGE AWARENESS TEST")
    print("=" * 70)
    print()
    
    # Test 1: Item at 11d coverage, wants to order to 15d target
    # Current: 11d (yellow zone: 10-12d), Post-order: 15d (would exceed)
    # Should be capped to bring post-order to 10d
    item_would_exceed = {
        'product_name': 'WOULD EXCEED POST-ORDER',
        'current_stocks': 55,  # 11 days coverage
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 100,  # Would order large qty
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'margin_pct': 0.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    # Test 2: Item at 11d coverage, wants small order to 11.5d
    # Current: 11d (yellow zone), Post-order: 11.5d (still in yellow, within 12d)
    # Should be ALLOWED (post-order doesn't exceed target)
    item_within_bounds = {
        'product_name': 'POST-ORDER WITHIN BOUNDS',
        'current_stocks': 55,  # 11 days coverage
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 2,  # Small order (2-3 units)
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'margin_pct': 0.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    # Test 3: Item at 9d coverage (green zone), wants to order to 15d
    # Current: 9d (green), Post-order: 15d (would exceed yellow)
    # Should be allowed since current is in green zone
    item_green_zone = {
        'product_name': 'GREEN ZONE ITEM',
        'current_stocks': 45,  # 9 days coverage
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 50,
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'margin_pct': 0.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    # Test 4: Item at 11d trending down, small top-up
    # Current: 11d (yellow), wants 5 units → post-order: 12d (at yellow limit)
    # Should be ALLOWED (prevents cliff later)
    item_trending_down = {
        'product_name': 'TRENDING DOWN NEEDS TOP-UP',
        'current_stocks': 55,  # 11 days coverage
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 5,  # Small top-up
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'margin_pct': 0.0,
        'sales_trend': 'declining',
        'sales_trend_pct': -15.0,  # Trending down
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    results = await llm.analyze([item_would_exceed, item_within_bounds, item_green_zone, item_trending_down])
    
    print("TEST RESULTS:")
    print("-" * 70)
    print()
    
    for i, r in enumerate(results):
        print(f"Test {i+1}: {r['product_name']}")
        print(f"  Recommended Qty: {r['recommended_quantity']}")
        print(f"  Confidence: {r['confidence']}")
        print(f"  Reasoning: {r['reasoning'][:140]}...")
        print()
    
    print("VALIDATION:")
    print("-" * 70)
    
    test_passed = True
    
    # Test 1: Should be capped (post-order would exceed) - may get health check min
    if 'SOFT GUARD' in results[0]['reasoning'] or 'HEALTH CHECK' in results[0]['reasoning'] or results[0]['recommended_quantity'] <= 1:
        print(f"[PASS] Test 1: Large order capped (post-order would exceed, got {results[0]['recommended_quantity']})")
    else:
        print(f"[FAIL] Test 1: Should be capped (got {results[0]['recommended_quantity']})")
        test_passed = False
    
    # Test 2: Should be allowed (post-order within bounds)
    if results[1]['recommended_quantity'] > 0:
        print(f"[PASS] Test 2: Small order allowed (post-order within bounds, got {results[1]['recommended_quantity']})")
    else:
        print(f"[FAIL] Test 2: Should allow small order (got {results[1]['recommended_quantity']})")
        test_passed = False
    
    # Test 3: Green zone should allow ordering
    if results[2]['recommended_quantity'] > 0:
        print(f"[PASS] Test 3: Green zone allows ordering (got {results[2]['recommended_quantity']})")
    else:
        print("[FAIL] Test 3: Green zone should allow ordering")
        test_passed = False
    
    # Test 4: Trending down should allow small top-up
    if results[3]['recommended_quantity'] > 0:
        print(f"[PASS] Test 4: Trending down gets top-up (prevents cliff, got {results[3]['recommended_quantity']})")
    else:
        print("[FAIL] Test 4: Trending down should allow top-up")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL POST-ORDER COVERAGE TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_post_order_coverage())
    sys.exit(0 if success else 1)
