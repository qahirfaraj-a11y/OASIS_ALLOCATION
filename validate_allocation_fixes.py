# -*- coding: utf-8 -*-
"""
Quick validation of under-allocation fixes.
Tests core allocation engine with various budgets and checks utilization.
"""
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from oasis.logic.order_engine import OrderEngine

def run_allocation_test(budget, label):
    engine = OrderEngine(os.getcwd())
    
    # Create realistic test recommendations
    recs = []
    # Fast Five staples
    for name, price, ads in [
        ("GOLDEN FRY COOKING OIL 1L", 350, 8.5),
        ("NDOVU MAIZE MEAL 2KG", 180, 12.0),
        ("MUMIAS SUGAR 1KG", 165, 15.0),
        ("EXE WHEAT FLOUR 2KG", 220, 7.0),
        ("DAIMA FRESH MILK 500ML", 65, 25.0),
    ]:
        recs.append({
            'product_name': name,
            'product_category': 'COOKING OIL' if 'OIL' in name else ('MAIZE MEAL' if 'MAIZE' in name else ('SUGAR' if 'SUGAR' in name else ('FLOUR' if 'FLOUR' in name else 'FRESH MILK'))),
            'selling_price': price,
            'avg_daily_sales': ads,
            'pack_size': 12 if 'OIL' in name else (6 if 'MILK' in name else 12),
            'current_stocks': 0,
            'ABC_Class': 'A',
            'margin_pct': 22.0,
            'supplier_name': 'TEST SUPPLIER',
            'is_staple_override': True,
        })
    
    # Medium-velocity discretionary
    for name, price, ads in [
        ("TROPICAL HEAT PILAU MASALA 100G", 85, 6.2),
        ("INDOMIE NOODLES CHICKEN 70G", 40, 9.5),
        ("CROWN TFA DETERGENT 500G", 120, 4.3),
        ("VASELINE JELLY 100ML", 180, 3.1),
        ("COLGATE TOOTHPASTE 100ML", 150, 2.8),
    ]:
        recs.append({
            'product_name': name, 'product_category': 'GENERAL',
            'selling_price': price, 'avg_daily_sales': ads,
            'pack_size': 12, 'current_stocks': 0,
            'ABC_Class': 'A', 'margin_pct': 25.0,
            'supplier_name': 'TEST SUPPLIER',
        })
    
    # Low-velocity items
    for name, price, ads in [
        ("ANCHOR BUTTER 227G", 450, 0.5),
        ("HEINZ KETCHUP 500ML", 520, 0.3),
        ("TABASCO SAUCE 150ML", 680, 0.1),
    ]:
        recs.append({
            'product_name': name, 'product_category': 'GENERAL',
            'selling_price': price, 'avg_daily_sales': ads,
            'pack_size': 6, 'current_stocks': 0,
            'ABC_Class': 'B', 'margin_pct': 30.0,
            'supplier_name': 'TEST SUPPLIER',
        })
    
    # Fresh items
    for name, price, ads in [
        ("FESTIVE BREAD 600G", 75, 18.0),
        ("BIO YOGHURT 250ML", 55, 8.5),
    ]:
        recs.append({
            'product_name': name, 'product_category': 'FRESH',
            'selling_price': price, 'avg_daily_sales': ads,
            'pack_size': 1, 'current_stocks': 0,
            'ABC_Class': 'A', 'margin_pct': 35.0,
            'supplier_name': 'BAKERY',
        })

    # Enrich
    engine.databases['supplier_patterns'] = {}
    engine.databases['sales_forecasting'] = {}
    engine.databases['sales_profitability'] = {}
    engine.databases['supplier_quality'] = {}
    engine.databases['product_supplier_map'] = {}
    engine.databases['product_department_map'] = {}
    engine.databases['simulation_feedback'] = {}
    engine.load_no_grn_suppliers()
    engine.enrich_product_data(recs, is_greenfield=True)
    
    result = engine.apply_greenfield_allocation(recs, budget)
    summary = result['summary']
    alloc_recs = result['recommendations']
    
    # Report
    total_allocated = sum(r['recommended_quantity'] for r in alloc_recs)
    total_items_allocated = sum(1 for r in alloc_recs if r['recommended_quantity'] > 0)
    utilization = summary.get('utilization_pct', 0)
    
    print(f"\n{'='*60}")
    print(f"  {label} -- Budget: KES {budget:,.0f}")
    print(f"{'='*60}")
    print(f"  Utilization:    {utilization:.1f}%")
    print(f"  Items Stocked:  {total_items_allocated}/{len(alloc_recs)}")
    print(f"  Total Units:    {total_allocated:,.0f}")
    print(f"  Pass 1 (Width): KES {summary.get('pass1_cash', 0):,.0f}")
    print(f"  Pass 2 (Depth): KES {summary.get('pass2_cash', 0):,.0f}")
    print(f"  Pass 2B (Flex): KES {summary.get('pass2b_cash', 0):,.0f}")
    print(f"  Pass 4 (MopUp): KES {summary.get('mop_up_cash', 0):,.0f}")
    print(f"  Total Cash:     KES {summary.get('total_cash_used', 0):,.0f}")
    print(f"  Skipped:        {summary.get('total_skipped', 0)}")
    
    # Show top allocations
    sorted_recs = sorted(alloc_recs, key=lambda x: x['recommended_quantity'], reverse=True)
    print(f"\n  Top 5 Allocations:")
    for r in sorted_recs[:5]:
        fresh_tag = " [FRESH]" if r.get('is_fresh') else ""
        print(f"    {r['product_name'][:35]:35s} Qty: {r['recommended_quantity']:>5}  {fresh_tag}")
    
    # Check fresh items specifically
    print(f"\n  Fresh Item Detail:")
    for r in alloc_recs:
        if r.get('is_fresh'):
            print(f"    {r['product_name'][:35]:35s} Qty: {r['recommended_quantity']:>5}  Coverage: {r.get('target_coverage_days', '?')}d")
    
    # Utilization check
    if utilization >= 80:
        print(f"\n  [PASS] Utilization {utilization:.1f}% >= 80%")
    else:
        print(f"\n  [FAIL] Utilization {utilization:.1f}% < 80% -- under-allocation detected")
    
    return utilization

print("=" * 60)
print("  UNDER-ALLOCATION FIX VALIDATION")
print("=" * 60)

u1 = run_allocation_test(300_000, "Micro Store (Duka)")
u2 = run_allocation_test(1_000_000, "Small Store (Mini-Mart)")
u3 = run_allocation_test(5_000_000, "Medium Store (Supermarket)")

print(f"\n{'='*60}")
print(f"  FINAL RESULTS")
print(f"{'='*60}")
all_pass = u1 >= 80 and u2 >= 80 and u3 >= 80
print(f"  Micro:   {u1:.1f}%  {'PASS' if u1 >= 80 else 'FAIL'}")
print(f"  Small:   {u2:.1f}%  {'PASS' if u2 >= 80 else 'FAIL'}")
print(f"  Medium:  {u3:.1f}%  {'PASS' if u3 >= 80 else 'FAIL'}")
print(f"  Overall: {'ALL PASS' if all_pass else 'SOME FAILED'}")
