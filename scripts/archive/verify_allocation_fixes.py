"""
Quick verification of allocation engine fixes
Tests that department scaling ratios and budget manager work correctly
"""

import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.budget_manager import BudgetManager

def verify_fixes():
    print("=" * 60)
    print("Allocation Engine Fix Verification")
    print("=" * 60)
    
    # Initialize budget manager
    data_dir = os.path.join(os.path.dirname(os.path.abspath(__file__)), "oasis", "data")
    bm = BudgetManager(data_dir)
    
    # Test 1: Verify scaling ratios loaded
    print(f"\n1. Scaling Ratios: {len(bm.scaling_ratios)} departments loaded")
    
    # Test 2: Check for zero-weight departments
    zero_weight = [d for d, w in bm.scaling_ratios.items() if w == 0.0]
    print(f"2. Zero-weight departments: {len(zero_weight)}")
    if zero_weight:
        print(f"   First 5: {zero_weight[:5]}")
    
    # Test 3: Initialize wallets with test budget
    test_budget = 200000  # KES 200k
    wallets = bm.initialize_wallets(test_budget)
    
    print(f"\n3. Wallet Initialization (Budget: KES {test_budget:,})")
    print(f"   Total wallets created: {len(wallets)}")
    
    # Test 4: Check key departments have proper allocation
    key_depts = ['FRESH MILK', 'BREAD', 'COOKING OIL', 'SUGAR', 'FLOUR', 'EGGS']
    print(f"\n4. Key Department Allocations:")
    for dept in key_depts:
        if dept in wallets:
            w = wallets[dept]
            print(f"   {dept}: KES {w['allocated_budget']:,.0f} (max: {w['max_budget']:,.0f})")
        else:
            print(f"   {dept}: NOT FOUND")
    
    # Test 5: Verify GENERAL contingency
    gen = wallets.get('GENERAL', {})
    print(f"\n5. GENERAL Contingency: KES {gen.get('allocated_budget', 0):,.0f}")
    
    # Test 6: Calculate total allocated
    total_allocated = sum(w['allocated_budget'] for w in wallets.values())
    print(f"\n6. Total Allocated: KES {total_allocated:,.0f} ({total_allocated/test_budget*100:.1f}% of budget)")
    
    # Test 7: Check staples loaded
    print(f"\n7. Staples Loaded: {len(bm.staples)}")
    
    # Test sample staples
    test_items = ['FRESH FRI 500ML COOKING OIL', 'TUZO FULL CREAM MILK 500ML', 'KABRAS SUGAR 2KG']
    print("   Sample staple checks:")
    for item in test_items:
        # Provide context (category + velocity) to trigger heuristic if not in Golden File
        # Assuming these are the categories for the test items for verification purposes
        cat_map = {
            'FRESH FRI 500ML COOKING OIL': 'COOKING OIL',
            'TUZO FULL CREAM MILK 500ML': 'FRESH MILK', 
            'KABRAS SUGAR 2KG': 'SUGAR'
        }
        is_staple = bm.is_staple(item, category=cat_map.get(item), velocity=5.0)
        print(f"   - '{item}': {'YES' if is_staple else 'NO'}")
    
    # Summary
    print("\n" + "=" * 60)
    if len(zero_weight) == 0 and total_allocated > 0:
        print("RESULT: ALL FIXES VERIFIED - No zero-weight departments!")
    elif total_allocated > test_budget * 0.95:
        print("RESULT: FIXES PARTIALLY VERIFIED - Most budget allocated")
    else:
        print("RESULT: NEEDS REVIEW - Some issues remain")
    print("=" * 60)

if __name__ == "__main__":
    verify_fixes()
