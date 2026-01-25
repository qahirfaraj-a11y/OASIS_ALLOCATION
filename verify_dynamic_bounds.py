import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_dynamic_upper_bounds():
    """Test dynamic upper bound adjustments based on lead time and CV"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("DYNAMIC UPPER BOUND TEST")
    print("=" * 70)
    print()
    
    # Test 1: Normal item (short lead time, low CV)
    normal_item = {
        'product_name': 'NORMAL ITEM (Short Lead, Low CV)',
        'current_stocks': 50,
        'avg_daily_sales': 5.0,  # 10 days coverage
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,  # Short lead time
        'demand_cv': 0.3,  # Low variability
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
    
    # Test 2: Long lead time item (should get +25% upper bound)
    long_lead_item = {
        'product_name': 'LONG LEAD ITEM (10 days)',
        'current_stocks': 60,
        'avg_daily_sales': 5.0,  # 12 days coverage
        'upper_coverage_days': 10,
        'estimated_delivery_days': 10,  # Long lead time
        'demand_cv': 0.3,  # Low variability
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
    
    # Test 3: High volatility item (should get +15% upper bound)
    volatile_item = {
        'product_name': 'VOLATILE ITEM (CV=0.8)',
        'current_stocks': 55,
        'avg_daily_sales': 5.0,  # 11 days coverage
        'upper_coverage_days': 10,
        'estimated_delivery_days': 2,  # Short lead time
        'demand_cv': 0.8,  # High variability
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
    
    # Test 4: Both long lead AND high volatility (should get both adjustments)
    double_adjustment_item = {
        'product_name': 'DOUBLE ADJUSTMENT (Long Lead + High CV)',
        'current_stocks': 70,
        'avg_daily_sales': 5.0,  # 14 days coverage
        'upper_coverage_days': 10,
        'estimated_delivery_days': 10,  # Long lead time
        'demand_cv': 0.8,  # High variability
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
    
    results = await llm.analyze([normal_item, long_lead_item, volatile_item, double_adjustment_item])
    
    print("TEST RESULTS:")
    print("-" * 70)
    print()
    
    scenarios = [
        ("Normal (2d lead, CV=0.3)", 10, 10),  # Base 10, no adjustment
        ("Long Lead (10d lead, CV=0.3)", 10, 12),  # Base 10 * 1.25 = 12.5 → 12
        ("Volatile (2d lead, CV=0.8)", 10, 11),  # Base 10 * 1.15 = 11.5 → 11
        ("Both (10d lead, CV=0.8)", 10, 14),  # Base 10 * 1.25 * 1.15 = 14.375 → 14
    ]
    
    for i, (scenario, base, expected_upper) in enumerate(scenarios):
        r = results[i]
        coverage = [10, 12, 11, 14][i]
        
        print(f"Test {i+1}: {scenario}")
        print(f"  Base Upper Bound: {base} days")
        print(f"  Expected Dynamic Upper: ~{expected_upper} days")
        print(f"  Current Coverage: {coverage} days")
        print(f"  Recommended Qty: {r['recommended_quantity']}")
        print(f"  Reasoning: {r['reasoning'][:120]}...")
        print()
    
    print("VALIDATION:")
    print("-" * 70)
    
    test_passed = True
    
    # Test 1: Normal item at 10d coverage should be in green zone (at limit, not over)
    if results[0]['recommended_quantity'] > 0:
        print("[PASS] Test 1: Normal item in green zone (10d coverage at 10d limit)")
    else:
        print("[FAIL] Test 1: Normal item at limit should still order")
        test_passed = False
    
    # Test 2: Long lead item at 12d coverage should be in green/yellow zone (adjusted to ~12d)
    if 'RED' not in results[1]['reasoning']:
        print("[PASS] Test 2: Long lead item gets relaxed upper bound (12d coverage allowed)")
    else:
        print("[FAIL] Test 2: Long lead should have higher limit")
        test_passed = False
    
    # Test 3: Volatile item at 11d coverage should be in green/yellow zone (adjusted to ~11d)
    if 'RED' not in results[2]['reasoning']:
        print("[PASS] Test 3: Volatile item gets relaxed upper bound (11d coverage allowed)")
    else:
        print("[FAIL] Test 3: Volatile should have higher limit")
        test_passed = False
    
    # Test 4: Double adjustment at 14d should be in green/yellow zone (adjusted to ~14d)
    if 'RED' not in results[3]['reasoning']:
        print("[PASS] Test 4: Double adjustment item gets both bonuses (14d coverage allowed)")
    else:
        print("[FAIL] Test 4: Double adjustment should have much higher limit")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL DYNAMIC UPPER BOUND TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_dynamic_upper_bounds())
    sys.exit(0 if success else 1)
