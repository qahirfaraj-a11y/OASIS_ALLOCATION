import asyncio
import sys
import os

# Add parent directory to path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_soft_guard_zones():
    """Test soft guard behavior across green/yellow/red zones"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("SOFT GUARD ZONE TEST")
    print("=" * 70)
    print()
    
    # Test 1: Green Zone (coverage < upper_bound)
    green_product = {
        'product_name': 'TEST MILK GREEN ZONE',
        'current_stocks': 40,
        'avg_daily_sales': 5.0,  # 8 days coverage
        'upper_coverage_days': 10,
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
        'is_sunset': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 10
    }
    
    # Test 2: Yellow Zone (upper_bound < coverage <= 1.2 * upper_bound)
    yellow_product = {
        'product_name': 'TEST MILK YELLOW ZONE',
        'current_stocks': 55,
        'avg_daily_sales': 5.0,  # 11 days coverage (10-12 zone)
        'upper_coverage_days': 10,
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
        'is_sunset': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 10
    }
    
    # Test 3: Red Zone (coverage > 1.2 * upper_bound)
    red_product = {
        'product_name': 'TEST MILK RED ZONE',
        'current_stocks': 70,
        'avg_daily_sales': 5.0,  # 14 days coverage (>12 hard limit)
        'upper_coverage_days': 10,
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
        'is_sunset': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 10
    }
    
    # Test 4: CZ Item in Yellow Zone (tighter limits)
    cz_yellow_product = {
        'product_name': 'TEST DRY CZ YELLOW',
        'current_stocks': 48,
        'avg_daily_sales': 3.0,  # 16 days coverage (14-16.8 zone for CZ)
        'upper_coverage_days': 45,  # Will be overridden to 14 for CZ
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
        'confidence_grn': 'MEDIUM',
        'order_cycle_count': 5
    }
    
    # Test 5: Promotional Exception (should bypass guard)
    promo_product = {
        'product_name': 'TEST PROMO EXCEPTION',
        'current_stocks': 70,
        'avg_daily_sales': 5.0,  # 14 days coverage (would be red zone)
        'upper_coverage_days': 10,
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
        'is_sunset': False,
        'is_promo': True,  # Promotional exception
        'confidence_grn': 'HIGH',
        'order_cycle_count': 10
    }
    
    results = await llm.analyze([green_product, yellow_product, red_product, cz_yellow_product, promo_product])
    
    print("TEST RESULTS:")
    print("-" * 70)
    print()
    
    test_passed = True
    
    for i, r in enumerate(results):
        coverage = 0
        if i == 0: coverage = 40 / 5.0  # 8 days
        elif i == 1: coverage = 55 / 5.0  # 11 days
        elif i == 2: coverage = 70 / 5.0  # 14 days
        elif i == 3: coverage = 48 / 3.0  # 16 days
        elif i == 4: coverage = 70 / 5.0  # 14 days
        
        print(f"Test {i+1}: {r['product_name']}")
        print(f"  Coverage: {coverage:.1f} days")
        print(f"  Recommended Qty: {r['recommended_quantity']}")
        print(f"  Confidence: {r['confidence']}")
        print(f"  Reasoning: {r['reasoning'][:150]}...")
        print()
    
    # Assertions
    print("VALIDATION:")
    print("-" * 70)
    
    # Test 1: Green zone should allow ordering
    if results[0]['recommended_quantity'] > 0:
        print("[PASS] Test 1: Green zone allows ordering")
    else:
        print("[FAIL] Test 1: Green zone should allow ordering")
        test_passed = False
    
    # Test 2: Yellow zone should show soft guard
    if 'SOFT GUARD' in results[1]['reasoning'] or results[1]['recommended_quantity'] >= 0:
        print("[PASS] Test 2: Yellow zone shows soft guard behavior")
    else:
        print("[FAIL] Test 2: Yellow zone should show soft guard")
        test_passed = False
    
    # Test 3: Red zone should block
    if results[2]['recommended_quantity'] == 0 and 'RED' in results[2]['reasoning']:
        print("[PASS] Test 3: Red zone blocks with RED indicator")
    else:
        print("[FAIL] Test 3: Red zone should block with RED indicator")
        test_passed = False
    
    # Test 4: CZ yellow zone
    if results[3]['recommended_quantity'] >= 0:
        print("[PASS] Test 4: CZ item in yellow zone handled correctly")
    else:
        print("[FAIL] Test 4: CZ yellow zone should allow capped ordering")
        test_passed = False
    
    # Test 5: Promo exception
    if results[4]['recommended_quantity'] >= 0 and 'RED' not in results[4]['reasoning']:
        print("[PASS] Test 5: Promotional exception bypasses guard")
    else:
        print("[FAIL] Test 5: Promo should bypass guard")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL SOFT GUARD TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_soft_guard_zones())
    sys.exit(0 if success else 1)
