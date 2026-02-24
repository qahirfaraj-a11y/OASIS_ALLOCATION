"""
Run Allocation Logic with Tier-Scaled Average Daily Sales
=========================================================
Generates allocation recommendations for each store tier using
demand scaling factors to match realistic store sizes.
"""

import sys
import os
import json
import pandas as pd
import numpy as np
from datetime import datetime

sys.path.append(os.getcwd())
from oasis.logic.order_engine import OrderEngine

# Configuration
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v3.csv"
DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

# Store Universe Configurations (matching simulator)
STORE_UNIVERSES = {
    "Micro_100k": {
        "budget": 100000,
        "demand_scale_factor": 0.02,
        "description": "Small Kiosk / Duka"
    },
    "Small_200k": {
        "budget": 200000,
        "demand_scale_factor": 0.05,
        "description": "Mini-Mart / Corner Store"
    },
    "Medium_1M": {
        "budget": 1000000,
        "demand_scale_factor": 0.15,
        "description": "Medium Supermarket"
    },
    "Large_10M": {
        "budget": 10000000,
        "demand_scale_factor": 0.40,
        "description": "Large Supermarket"
    },
    "Mega_100M": {
        "budget": 100000000,
        "demand_scale_factor": 1.0,
        "description": "Hypermarket / Mega Store"
    }
}

def load_and_scale_recommendations(demand_scale: float) -> list:
    """Load scorecard and create recommendations with scaled ADS."""
    df = pd.read_csv(SCORECARD_FILE)
    
    recommendations = []
    for _, row in df.iterrows():
        # Scale ADS by store tier
        base_ads = float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0)
        scaled_ads = base_ads * demand_scale
        
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': scaled_ads,  # Using scaled ADS
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1,
            'moq_floor': 0,
            'historical_order_count': 0,
            'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else 25.0,
            'ABC_Class': row.get('ABC_Class', 'B'),
            'reliability_score': float(row.get('Supplier_Reliability', 0.8) if pd.notnull(row.get('Supplier_Reliability')) else 0.8) * 100,
            'demand_cv': 0.5,  # Default CV
            'shelf_life_days': 7 if row.get('Department', '').upper() in ['FRESH MILK', 'BREAD', 'YOGHURT'] else 365,
            'is_fresh': row.get('Department', '').upper() in ['FRESH MILK', 'BREAD', 'YOGHURT', 'EGGS'],
            'is_consignment': False,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
    
    return recommendations

def run_allocation_for_tier(engine, tier_name: str, config: dict) -> dict:
    """Run allocation for a single tier and return results."""
    print(f"\n{'='*60}")
    print(f"ALLOCATION: {tier_name}")
    print(f"Budget: KES {config['budget']:,}")
    print(f"Demand Scale: {config['demand_scale_factor']:.0%} of Mega")
    print(f"{'='*60}")
    
    # Load recommendations with scaled ADS
    recommendations = load_and_scale_recommendations(config['demand_scale_factor'])
    
    # Run allocation
    result = engine.apply_greenfield_allocation(recommendations, config['budget'])
    
    summary = result['summary']
    final_recs = result['recommendations']
    
    # Calculate metrics
    total_revenue_potential = 0
    total_cost = 0
    items_allocated = 0
    dept_breakdown = {}
    
    for rec in final_recs:
        qty = rec.get('recommended_quantity', 0)
        if qty > 0:
            price = rec.get('selling_price', 0)
            margin_pct = rec.get('margin_pct', 25) or 25
            cost_price = price * (1 - margin_pct / 100)
            
            dept = rec.get('product_category', 'GENERAL')
            if dept not in dept_breakdown:
                dept_breakdown[dept] = {'skus': 0, 'cost': 0, 'revenue': 0}
            
            # Estimate monthly revenue based on scaled ADS
            ads = rec.get('avg_daily_sales', 0)
            monthly_units = ads * 30
            monthly_revenue = monthly_units * price
            
            total_revenue_potential += monthly_revenue
            total_cost += qty * cost_price
            items_allocated += 1
            
            dept_breakdown[dept]['skus'] += 1
            dept_breakdown[dept]['cost'] += qty * cost_price
            dept_breakdown[dept]['revenue'] += monthly_revenue
    
    # Days to ROI calculation
    if total_revenue_potential > 0:
        daily_revenue = total_revenue_potential / 30
        days_to_roi = total_cost / daily_revenue if daily_revenue > 0 else 999
    else:
        days_to_roi = 999
    
    # Print results
    print(f"\nALLOCATION SUMMARY:")
    print(f"  SKUs Allocated:       {items_allocated}")
    print(f"  Total Capital Used:   KES {summary.get('total_cash_used', 0):,.0f}")
    print(f"  Budget Utilization:   {summary.get('utilization_pct', 0):.1f}%")
    print(f"  Est. Monthly Revenue: KES {total_revenue_potential:,.0f}")
    print(f"  Days to ROI:          {days_to_roi:.1f} days")
    
    # Top departments
    print(f"\nTOP 5 DEPARTMENTS BY ALLOCATION:")
    sorted_depts = sorted(dept_breakdown.items(), key=lambda x: x[1]['cost'], reverse=True)[:5]
    for dept, stats in sorted_depts:
        print(f"  - {dept}: {stats['skus']} SKUs, KES {stats['cost']:,.0f}")
    
    return {
        'tier': tier_name,
        'budget': config['budget'],
        'demand_scale': config['demand_scale_factor'],
        'skus_allocated': items_allocated,
        'capital_used': summary.get('total_cash_used', 0),
        'utilization_pct': summary.get('utilization_pct', 0),
        'monthly_revenue': total_revenue_potential,
        'days_to_roi': days_to_roi,
        'recommendations': final_recs
    }

def main():
    print("=" * 60)
    print("ALLOCATION WITH SCALED DEMAND")
    print("=" * 60)
    
    # Initialize engine
    engine = OrderEngine(DATA_DIR)
    
    results = []
    
    # Run for each tier
    for tier_name, config in STORE_UNIVERSES.items():
        result = run_allocation_for_tier(engine, tier_name, config)
        results.append(result)
    
    # Summary comparison
    print(f"\n{'='*60}")
    print("TIER COMPARISON (Scaled ADS)")
    print(f"{'='*60}")
    print(f"{'Tier':<15} {'SKUs':<8} {'Utilization':<12} {'Days ROI':<12}")
    print("-" * 60)
    for r in results:
        print(f"{r['tier']:<15} {r['skus_allocated']:<8} {r['utilization_pct']:<12.1f}% {r['days_to_roi']:<12.1f}")
    
    # Export to Excel
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_file = f"allocation_scaled_demand_{timestamp}.xlsx"
    
    with pd.ExcelWriter(output_file, engine='openpyxl') as writer:
        # Summary sheet
        summary_data = [{
            'Tier': r['tier'],
            'Budget': r['budget'],
            'Demand Scale %': r['demand_scale'] * 100,
            'SKUs Allocated': r['skus_allocated'],
            'Capital Used': r['capital_used'],
            'Utilization %': r['utilization_pct'],
            'Est Monthly Revenue': r['monthly_revenue'],
            'Days to ROI': r['days_to_roi']
        } for r in results]
        pd.DataFrame(summary_data).to_excel(writer, sheet_name='Summary', index=False)
        
        # Per-tier allocation details (first 3 tiers)
        for r in results[:3]:
            tier_data = []
            for rec in r['recommendations']:
                if rec.get('recommended_quantity', 0) > 0:
                    tier_data.append({
                        'Product': rec['product_name'][:50],
                        'Department': rec.get('product_category', ''),
                        'Qty': rec['recommended_quantity'],
                        'Price': rec.get('selling_price', 0),
                        'Scaled ADS': rec.get('avg_daily_sales', 0),
                        'Reasoning': rec.get('reasoning', '')[:60]
                    })
            if tier_data:
                pd.DataFrame(tier_data).to_excel(writer, sheet_name=r['tier'][:31], index=False)
    
    print(f"\n[OK] Results exported to: {output_file}")

if __name__ == "__main__":
    main()
