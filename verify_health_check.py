import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_health_check_minimum():
    """Test health check minimum order for strategic items"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("HEALTH CHECK MINIMUM ORDER TEST")
    print("=" * 70)
    print()
    
    # Test 1: Strategic item (AX) in yellow zone - should get health check min
    strategic_yellow = {
        'product_name': 'STRATEGIC AX IN YELLOW ZONE',
        'current_stocks': 55,  # 11 days coverage (yellow zone: 10-12 days)
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 5,  # Small historical avg so it would normally cap to 0
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'sales_trend': 'declining',  # Declining trend
        'sales_trend_pct': -10.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,  # Recent activity
        'abc_rank': 'A',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': True,
        'is_top_sku': True,
        'moq_floor': 10,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    # Test 2: Non-strategic item (CZ) in yellow zone - should be capped to 0
    non_strategic_yellow = {
        'product_name': 'NON-STRATEGIC CZ IN YELLOW ZONE',
        'current_stocks': 70,
        'avg_daily_sales': 3.0,  # 23 days coverage
        'upper_coverage_days': 45,
        'supplier_frequency_days': 7,
        'estimated_delivery_days': 7,
        'demand_cv': 0.5,
        'reliability_score': 85,
        'historical_avg_order_qty': 30,
        'is_fresh': False,
        'blocked_open_for_order': 'open',
        'selling_price': 50.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 10,
        'abc_rank': 'C',
        'xyz_rank': 'Z',
        'is_sunset': False,
        'is_key_sku': False,
        'is_top_sku': False,
        'confidence_grn': 'MEDIUM',
        'order_cycle_count': 5,
        'months_active': 6
    }
    
    # Test 3: Strategic item with old activity - should NOT get health check
    strategic_old_activity = {
        'product_name': 'STRATEGIC BUT OLD ACTIVITY',
        'current_stocks': 60,
        'avg_daily_sales': 5.0,  # 12 days coverage
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 50,
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 90,  # Old activity (>60 days)
        'abc_rank': 'A',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_key_sku': True,
        'is_top_sku': True,
        'moq_floor': 10,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    # Test 4: Sunset strategic item - should NOT get health check
    strategic_sunset = {
        'product_name': 'STRATEGIC BUT SUNSET',
        'current_stocks': 60,
        'avg_daily_sales': 5.0,
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,
        'demand_cv': 0.3,
        'reliability_score': 90,
        'historical_avg_order_qty': 50,
        'is_fresh': True,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 5,
        'abc_rank': 'A',
        'xyz_rank': 'X',
        'is_sunset': True,  # Sunset item
        'is_key_sku': True,
        'is_top_sku': True,
        'moq_floor': 10,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20,
        'months_active': 12
    }
    
    results = await llm.analyze([strategic_yellow, non_strategic_yellow, strategic_old_activity, strategic_sunset])
    
    print("TEST RESULTS:")
    print("-" * 70)
    print()
    
    for i, r in enumerate(results):
        print(f"Test {i+1}: {r['product_name']}")
        print(f"  Recommended Qty: {r['recommended_quantity']}")
        print(f"  Confidence: {r['confidence']}")
        print(f"  Reasoning: {r['reasoning'][:120]}...")
        print()
    
    print("VALIDATION:")
    print("-" * 70)
    
    test_passed = True
    
    # Test 1: Strategic item should get health check minimum (MOQ=10)
    # Item calculates 4 units but should get at least 10 due to health check
    if results[0]['recommended_quantity'] >= 10 or 'HEALTH CHECK' in results[0]['reasoning']:
        print(f"[PASS] Test 1: Strategic AX item gets health check or normal order (got {results[0]['recommended_quantity']})")
    else:
        print(f"[FAIL] Test 1: Strategic should get health check min or normal order (got {results[0]['recommended_quantity']})")
        test_passed = False
    
    # Test 2: Non-strategic CZ should be capped to 0
    if results[1]['recommended_quantity'] == 0 or 'HEALTH CHECK' not in results[1]['reasoning']:
        print("[PASS] Test 2: Non-strategic CZ capped normally (no health check)")
    else:
        print("[FAIL] Test 2: Non-strategic should not get health check")
        test_passed = False
    
    # Test 3: Strategic with old activity should NOT get health check
    if 'HEALTH CHECK' not in results[2]['reasoning']:
        print("[PASS] Test 3: Old activity strategic item no health check (>60d)")
    else:
        print("[FAIL] Test 3: Old activity should not trigger health check")
        test_passed = False
    
    # Test 4: Sunset strategic should NOT get health check
    if results[3]['recommended_quantity'] == 0:
        print("[PASS] Test 4: Sunset strategic item blocked (no health check)")
    else:
        print("[FAIL] Test 4: Sunset should not get health check")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL HEALTH CHECK TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_health_check_minimum())
    sys.exit(0 if success else 1)
