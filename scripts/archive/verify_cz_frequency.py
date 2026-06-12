import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_cz_frequency_based_bounds():
    """Test CZ items with different supplier frequencies"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("CZ FREQUENCY-BASED UPPER BOUND TEST")
    print("=" * 70)
    print()
    
    # Test 1: CZ item with weekly supplier (should get 21-day base)
    cz_weekly = {
        'product_name': 'CZ WEEKLY SUPPLIER',
        'current_stocks': 60,
        'avg_daily_sales': 3.0,  # 20 days coverage
        'upper_coverage_days': 45,  # Will be overridden
        'supplier_frequency_days': 7,  # Weekly
        'estimated_delivery_days': 7,
        'demand_cv': 0.3,
        'reliability_score': 90,
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
    
    # Test 2: CZ item with bi-weekly supplier (should get 28-day base)
    cz_biweekly = {
        'product_name': 'CZ BI-WEEKLY SUPPLIER',
        'current_stocks': 75,
        'avg_daily_sales': 3.0,  # 25 days coverage
        'upper_coverage_days': 45,
        'supplier_frequency_days': 14,  # Bi-weekly
        'estimated_delivery_days': 10,
        'demand_cv': 0.4,
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
        'confidence_grn': 'MEDIUM',
        'order_cycle_count': 5
    }
    
    # Test 3: CZ item with monthly supplier (should get 35-day base)
    cz_monthly = {
        'product_name': 'CZ MONTHLY SUPPLIER',
        'current_stocks': 100,
        'avg_daily_sales': 3.0,  # 33 days coverage
        'upper_coverage_days': 45,
        'supplier_frequency_days': 30,  # Monthly
        'estimated_delivery_days': 14,
        'demand_cv': 0.5,
        'reliability_score': 80,
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
    
    # Test 4: Old hard limit comparison (14 days) - should now be 21
    cz_old_limit = {
        'product_name': 'CZ OLD HARD LIMIT (Weekly)',
        'current_stocks': 50,
        'avg_daily_sales': 3.0,  # 16.7 days coverage
        'upper_coverage_days': 45,
        'supplier_frequency_days': 7,
        'estimated_delivery_days': 5,
        'demand_cv': 0.3,
        'reliability_score': 90,
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
    
    results = await llm.analyze([cz_weekly, cz_biweekly, cz_monthly, cz_old_limit])
    
    print("TEST RESULTS:")
    print("-" * 70)
    print()
    
    scenarios = [
        ("Weekly (7d freq)", 21, 20),
        ("Bi-weekly (14d freq)", 28, 25),
        ("Monthly (30d freq)", 35, 33),
        ("Old Limit Test (7d freq)", 21, 16.7),
    ]
    
    for i, (scenario, expected_base, coverage) in enumerate(scenarios):
        r = results[i]
        
        print(f"Test {i+1}: {scenario}")
        print(f"  Expected Base Upper: {expected_base} days")
        print(f"  Current Coverage: {coverage} days")
        print(f"  Recommended Qty: {r['recommended_quantity']}")
        print(f"  Reasoning: {r['reasoning'][:120]}...")
        print()
    
    print("VALIDATION:")
    print("-" * 70)
    
    test_passed = True
    
    # Test 1: Weekly CZ at 20d coverage should be in green zone (base=21)
    if 'RED' not in results[0]['reasoning']:
        print("[PASS] Test 1: Weekly CZ gets 21-day base (20d coverage allowed)")
    else:
        print("[FAIL] Test 1: Weekly CZ should have 21-day base")
        test_passed = False
    
    # Test 2: Bi-weekly CZ at 25d coverage should be in green zone (base=28)
    if 'RED' not in results[1]['reasoning']:
        print("[PASS] Test 2: Bi-weekly CZ gets 28-day base (25d coverage allowed)")
    else:
        print("[FAIL] Test 2: Bi-weekly CZ should have 28-day base")
        test_passed = False
    
    # Test 3: Monthly CZ at 33d coverage should be in green zone (base=35)
    if 'RED' not in results[2]['reasoning']:
        print("[PASS] Test 3: Monthly CZ gets 35-day base (33d coverage allowed)")
    else:
        print("[FAIL] Test 3: Monthly CZ should have 35-day base")
        test_passed = False
    
    # Test 4: Old 14-day limit would have blocked at 16.7d, new 21-day allows it
    if 'RED' not in results[3]['reasoning']:
        print("[PASS] Test 4: Old hard limit relaxed (16.7d now allowed vs old 14d)")
    else:
        print("[FAIL] Test 4: Should be more relaxed than old 14-day limit")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL CZ FREQUENCY-BASED TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_cz_frequency_based_bounds())
    sys.exit(0 if success else 1)
