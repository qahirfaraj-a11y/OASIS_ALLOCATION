"""
Allocation Gap Analysis
=======================
Analyzes allocation results to identify gaps, issues, and optimization opportunities.
"""

import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime
from collections import defaultdict

sys.path.append(os.getcwd())
from oasis.logic.order_engine import OrderEngine

# Configuration
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

# Test budget (Small_200k as default)
TEST_BUDGET = 200000
DEMAND_SCALE = 0.05  # 5% for Small store

def load_recommendations(demand_scale: float) -> list:
    """Load scorecard and create recommendations with scaled ADS."""
    df = pd.read_csv(SCORECARD_FILE)
    
    recommendations = []
    for _, row in df.iterrows():
        base_ads = float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0)
        scaled_ads = base_ads * demand_scale
        
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': scaled_ads,
            'base_avg_daily_sales': base_ads,
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1,
            'moq_floor': 0,
            'historical_order_count': 0,
            'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else 25.0,
            'ABC_Class': row.get('ABC_Class', 'B'),
            'velocity_tier': row.get('Velocity_Tier', 'C'),
            'reliability_score': float(row.get('Supplier_Reliability', 0.8) if pd.notnull(row.get('Supplier_Reliability')) else 0.8) * 100,
            'supplier': row.get('Supplier', 'Unknown'),
            'grn_freq': float(row.get('GRN_Frequency', 0) if pd.notnull(row.get('GRN_Frequency')) else 0),
            'is_fresh': row.get('Department', '').upper() in ['FRESH MILK', 'BREAD', 'YOGHURT', 'EGGS'],
            'is_consignment': False,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    return recommendations

def analyze_gaps(engine, recommendations, result):
    """Analyze allocation for gaps and issues."""
    
    final_recs = result['recommendations']
    summary = result['summary']
    
    gaps = {
        'high_velocity_skipped': [],
        'low_coverage_departments': [],
        'price_ceiling_blocked': [],
        'dead_stock_included': [],
        'staple_gaps': [],
        'under_allocated_high_margin': [],
        'supplier_concentration': defaultdict(float),
        'department_coverage': defaultdict(lambda: {'allocated': 0, 'skipped': 0, 'total_value': 0})
    }
    
    # Build lookup
    rec_map = {r['product_name']: r for r in final_recs}
    
    # Analyze each recommendation
    for rec in final_recs:
        name = rec['product_name']
        dept = rec.get('product_category', 'GENERAL')
        qty = rec.get('recommended_quantity', 0)
        reasoning = rec.get('reasoning', '')
        price = rec.get('selling_price', 0)
        ads = rec.get('avg_daily_sales', 0)
        base_ads = rec.get('base_avg_daily_sales', 0)
        abc = rec.get('ABC_Class', 'C')
        margin = rec.get('margin_pct', 0)
        supplier = rec.get('supplier', 'Unknown')
        is_staple = rec.get('is_staple_override', False)
        grn_freq = rec.get('grn_freq', 0)
        
        # Track department coverage
        if qty > 0:
            gaps['department_coverage'][dept]['allocated'] += 1
            gaps['department_coverage'][dept]['total_value'] += qty * price
            gaps['supplier_concentration'][supplier] += qty * price
        else:
            gaps['department_coverage'][dept]['skipped'] += 1
        
        # GAP 1: High velocity items being skipped
        if qty == 0 and base_ads > 5:  # High velocity (5+ units/day at Mega level)
            gaps['high_velocity_skipped'].append({
                'product': name[:50],
                'dept': dept,
                'base_ads': base_ads,
                'reason': reasoning[:80]
            })
        
        # GAP 2: Price ceiling blocks
        if 'PRICE >' in reasoning:
            gaps['price_ceiling_blocked'].append({
                'product': name[:50],
                'dept': dept,
                'price': price,
                'reason': reasoning[:80]
            })
        
        # GAP 3: Staples not allocated
        if is_staple and qty == 0:
            gaps['staple_gaps'].append({
                'product': name[:50],
                'dept': dept,
                'reason': reasoning[:80]
            })
        
        # GAP 4: High margin items under-allocated
        if margin > 30 and abc in ['A', 'B'] and qty == 0:
            gaps['under_allocated_high_margin'].append({
                'product': name[:50],
                'dept': dept,
                'margin': margin,
                'abc': abc,
                'reason': reasoning[:80]
            })
        
        # GAP 5: Dead stock included (low GRN freq)
        if qty > 0 and grn_freq < 0.1 and base_ads < 0.1:
            gaps['dead_stock_included'].append({
                'product': name[:50],
                'dept': dept,
                'grn_freq': grn_freq,
                'qty': qty
            })
    
    return gaps

def print_gap_report(gaps, summary):
    """Print formatted gap analysis report."""
    
    print("\n" + "="*70)
    print("ALLOCATION GAP ANALYSIS REPORT")
    print("="*70)
    
    # Summary stats
    total_allocated = sum(d['allocated'] for d in gaps['department_coverage'].values())
    total_skipped = sum(d['skipped'] for d in gaps['department_coverage'].values())
    
    print(f"\nOVERALL SUMMARY:")
    print(f"  SKUs Allocated: {total_allocated}")
    print(f"  SKUs Skipped:   {total_skipped}")
    print(f"  Utilization:    {summary.get('utilization_pct', 0):.1f}%")
    
    # GAP 1: High velocity skipped
    print(f"\n{'='*70}")
    print(f"GAP 1: HIGH VELOCITY ITEMS SKIPPED ({len(gaps['high_velocity_skipped'])} items)")
    print("-"*70)
    if gaps['high_velocity_skipped']:
        for item in gaps['high_velocity_skipped'][:10]:
            print(f"  {item['product']}")
            print(f"    Dept: {item['dept']}, Base ADS: {item['base_ads']:.1f}")
            print(f"    Reason: {item['reason']}")
    else:
        print("  [OK] No high-velocity items skipped")
    
    # GAP 2: Price ceiling blocks
    print(f"\n{'='*70}")
    print(f"GAP 2: PRICE CEILING BLOCKS ({len(gaps['price_ceiling_blocked'])} items)")
    print("-"*70)
    if gaps['price_ceiling_blocked']:
        for item in gaps['price_ceiling_blocked'][:10]:
            print(f"  {item['product']}")
            print(f"    Dept: {item['dept']}, Price: KES {item['price']:,.0f}")
    else:
        print("  [OK] No items blocked by price ceiling")
    
    # GAP 3: Staple gaps
    print(f"\n{'='*70}")
    print(f"GAP 3: STAPLES NOT ALLOCATED ({len(gaps['staple_gaps'])} items)")
    print("-"*70)
    if gaps['staple_gaps']:
        for item in gaps['staple_gaps'][:10]:
            print(f"  {item['product']}")
            print(f"    Dept: {item['dept']}")
            print(f"    Reason: {item['reason']}")
    else:
        print("  [OK] All staples allocated")
    
    # GAP 4: High margin under-allocated
    print(f"\n{'='*70}")
    print(f"GAP 4: HIGH MARGIN ITEMS NOT ALLOCATED ({len(gaps['under_allocated_high_margin'])} items)")
    print("-"*70)
    if gaps['under_allocated_high_margin']:
        for item in gaps['under_allocated_high_margin'][:10]:
            print(f"  {item['product']}")
            print(f"    Dept: {item['dept']}, Margin: {item['margin']:.0f}%, ABC: {item['abc']}")
    else:
        print("  [OK] All high-margin items allocated")
    
    # GAP 5: Dead stock included
    print(f"\n{'='*70}")
    print(f"GAP 5: POTENTIAL DEAD STOCK INCLUDED ({len(gaps['dead_stock_included'])} items)")
    print("-"*70)
    if gaps['dead_stock_included']:
        for item in gaps['dead_stock_included'][:10]:
            print(f"  {item['product']}")
            print(f"    Qty: {item['qty']}, GRN Freq: {item['grn_freq']:.2f}")
    else:
        print("  [OK] No dead stock detected")
    
    # Department coverage analysis
    print(f"\n{'='*70}")
    print("DEPARTMENT COVERAGE ANALYSIS")
    print("-"*70)
    
    dept_stats = []
    for dept, stats in gaps['department_coverage'].items():
        total = stats['allocated'] + stats['skipped']
        coverage = (stats['allocated'] / total * 100) if total > 0 else 0
        dept_stats.append({
            'dept': dept,
            'allocated': stats['allocated'],
            'skipped': stats['skipped'],
            'coverage': coverage,
            'value': stats['total_value']
        })
    
    # Sort by coverage (lowest first)
    dept_stats.sort(key=lambda x: x['coverage'])
    
    print(f"{'Department':<30} {'Alloc':<8} {'Skip':<8} {'Coverage':<10} {'Value KES':<15}")
    print("-"*70)
    
    # Show departments with <50% coverage
    low_coverage = [d for d in dept_stats if d['coverage'] < 50]
    for d in low_coverage[:15]:
        print(f"{d['dept'][:30]:<30} {d['allocated']:<8} {d['skipped']:<8} {d['coverage']:<10.0f}% {d['value']:<15,.0f}")
    
    if not low_coverage:
        print("  [OK] All departments have >50% coverage")
    
    # Supplier concentration (top 5)
    print(f"\n{'='*70}")
    print("SUPPLIER CONCENTRATION (Top 10)")
    print("-"*70)
    
    sorted_suppliers = sorted(gaps['supplier_concentration'].items(), key=lambda x: x[1], reverse=True)
    total_value = sum(gaps['supplier_concentration'].values())
    
    for supplier, value in sorted_suppliers[:10]:
        pct = (value / total_value * 100) if total_value > 0 else 0
        print(f"  {supplier[:40]:<40} KES {value:>12,.0f} ({pct:>5.1f}%)")
    
    # Check for over-concentration
    if sorted_suppliers and sorted_suppliers[0][1] / total_value > 0.3:
        print(f"\n  [WARNING] Highest supplier has {sorted_suppliers[0][1]/total_value*100:.1f}% concentration!")

def main():
    print("="*70)
    print(f"RUNNING ALLOCATION GAP ANALYSIS")
    print(f"Budget: KES {TEST_BUDGET:,}, Demand Scale: {DEMAND_SCALE:.0%}")
    print("="*70)
    
    # Initialize engine
    engine = OrderEngine(DATA_DIR)
    
    # Load recommendations
    recommendations = load_recommendations(DEMAND_SCALE)
    print(f"Loaded {len(recommendations)} SKUs from scorecard")
    
    # Run allocation
    result = engine.apply_greenfield_allocation(recommendations, TEST_BUDGET)
    
    # Analyze gaps
    gaps = analyze_gaps(engine, recommendations, result)
    
    # Print report
    print_gap_report(gaps, result['summary'])
    
    # Export detailed gap report
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"allocation_gap_analysis_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # High velocity skipped
        if gaps['high_velocity_skipped']:
            pd.DataFrame(gaps['high_velocity_skipped']).to_excel(
                writer, sheet_name='High Velocity Skipped', index=False)
        
        # Price ceiling blocked
        if gaps['price_ceiling_blocked']:
            pd.DataFrame(gaps['price_ceiling_blocked']).to_excel(
                writer, sheet_name='Price Ceiling Blocked', index=False)
        
        # Staple gaps
        if gaps['staple_gaps']:
            pd.DataFrame(gaps['staple_gaps']).to_excel(
                writer, sheet_name='Staple Gaps', index=False)
        
        # Department coverage
        dept_data = [{'Department': k, **v} for k, v in gaps['department_coverage'].items()]
        pd.DataFrame(dept_data).to_excel(writer, sheet_name='Department Coverage', index=False)
    
    print(f"\n[OK] Gap analysis exported to: {output_file}")

if __name__ == "__main__":
    main()
