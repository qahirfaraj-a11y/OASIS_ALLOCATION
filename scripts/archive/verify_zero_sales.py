import asyncio
import sys
import os

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from app.llm.inference import RuleBasedLLM

async def test_zero_sales_handling():
    """Test intelligent zero-sales handling"""
    llm = RuleBasedLLM()
    
    print("=" * 70)
    print("ZERO-SALES INTELLIGENT HANDLING TEST")
    print("=" * 70)
    print()
    
    # Test 1: Dead SKU (no sales in 6+ months)
    dead_sku = {
        'product_name': 'DEAD SKU (No Sales 6+ Months)',
        'current_stocks': 1,  # Minimal stock to avoid anti-overstock guard
        'avg_daily_sales': 0,  # Zero sales
        'months_active': 8,  # 8 months of tracking
        'upper_coverage_days': 45,
        'estimated_delivery_days': 7,
        'demand_cv': 0.5,
        'reliability_score': 90,
        'historical_avg_order_qty': 0,  # No historical orders
        'is_fresh': False,
        'blocked_open_for_order': 'open',
        'selling_price': 50.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 200,
        'abc_rank': 'C',
        'xyz_rank': 'Z',
        'is_sunset': False,
        'confidence_grn': 'LOW',
        'order_cycle_count': 0
    }
    
    # Test 2: Recently out of stock (has historical sales)
    recently_oos = {
        'product_name': 'RECENTLY OUT OF STOCK (Has History)',
        'current_stocks': 0,
        'avg_daily_sales': 0,  # Currently zero
        'months_active': 3,
        'upper_coverage_days': 45,
        'estimated_delivery_days': 7,
        'demand_cv': 0.4,
        'reliability_score': 90,
        'historical_avg_order_qty': 50,  # Has historical orders!
        'is_fresh': False,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 30,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 20
    }
    
    # Test 3: Seasonal item (zero sales but active in past)
    seasonal_item = {
        'product_name': 'SEASONAL ITEM (Zero Now, Active Before)',
        'current_stocks': 5,
        'avg_daily_sales': 0,  # Currently zero (off-season)
        'months_active': 12,
        'upper_coverage_days': 45,
        'estimated_delivery_days': 7,
        'demand_cv': 0.8,  # High variability (seasonal)
        'reliability_score': 90,
        'historical_avg_order_qty': 100,  # Strong historical orders
        'is_fresh': False,
        'blocked_open_for_order': 'open',
        'selling_price': 150.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 90,
        'abc_rank': 'A',
        'xyz_rank': 'Z',  # Erratic (seasonal pattern)
        'is_sunset': False,
        'confidence_grn': 'HIGH',
        'order_cycle_count': 15
    }
    
    # Test 4: New item with lookalike forecast
    new_with_lookalike = {
        'product_name': 'NEW ITEM (Lookalike Forecast)',
        'current_stocks': 0,
        'avg_daily_sales': 0,  # No sales yet
        'months_active': 0,
        'upper_coverage_days': 45,
        'estimated_delivery_days': 7,
        'demand_cv': 0.5,
        'reliability_score': 90,
        'historical_avg_order_qty': 0,
        'is_fresh': False,
        'blocked_open_for_order': 'open',
        'selling_price': 100.0,
        'sales_trend': 'stable',
        'sales_trend_pct': 0.0,
        'supplier_expiry_returns': 0,
        'last_days_since_last_delivery': 0,
        'abc_rank': 'B',
        'xyz_rank': 'X',
        'is_sunset': False,
        'is_lookalike_forecast': True,
        'lookalike_demand': 2.5,  # Forecast from similar items
        'confidence_grn': 'LOW',
        'order_cycle_count': 0,
        'new_item_aggression_cap': 21
    }
    
    results = await llm.analyze([dead_sku, recently_oos, seasonal_item, new_with_lookalike])
    
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
    
    # Test 1: Dead SKU should be blocked
    if results[0]['recommended_quantity'] == 0 and 'DEAD SKU' in results[0]['reasoning']:
        print("[PASS] Test 1: Dead SKU blocked (no sales in 8 months)")
    else:
        print("[FAIL] Test 1: Dead SKU should be blocked")
        test_passed = False
    
    # Test 2: Recently OOS with history should order (not blocked by zero sales)
    if results[1]['recommended_quantity'] > 0:
        print("[PASS] Test 2: Recently OOS item orders (has historical avg 50)")
    else:
        print("[FAIL] Test 2: Recently OOS should order based on history")
        test_passed = False
    
    # Test 3: Seasonal item should not be treated as infinite coverage
    if 'DEAD SKU' not in results[2]['reasoning']:
        print("[PASS] Test 3: Seasonal item not flagged as dead (has 12mo history)")
    else:
        print("[FAIL] Test 3: Seasonal should not be dead SKU")
        test_passed = False
    
    # Test 4: New item with lookalike should order
    if results[3]['recommended_quantity'] > 0:
        print("[PASS] Test 4: New item with lookalike forecast orders")
    else:
        print("[FAIL] Test 4: New item should use lookalike demand")
        test_passed = False
    
    print()
    print("=" * 70)
    if test_passed:
        print("ALL ZERO-SALES HANDLING TESTS PASSED!")
    else:
        print("SOME TESTS FAILED - Review output above")
    print("=" * 70)
    
    return test_passed

if __name__ == '__main__':
    success = asyncio.run(test_zero_sales_handling())
    sys.exit(0 if success else 1)
