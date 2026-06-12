
import sys
import os
import pandas as pd
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

def debug_3m_allocation():
    print("=" * 60)
    print("DEBUG: Day 1 Allocation Analysis (Budget: 3,570,000)")
    print("=" * 60)
    
    budget = 3_570_000
    
    # Initialize Engine
    engine = OrderEngine(os.getcwd())
    
    # Load Scorecard Data (Scenario Proxy)
    scorecard_path = os.path.join(os.getcwd(), "Full_Product_Allocation_Scorecard_v3.csv")
    if not os.path.exists(scorecard_path):
        print("ERROR: Scorecard file not found.")
        return
        
    df = pd.read_csv(scorecard_path)
    
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1, # Default 1 for debug
            'moq_floor': 0,
            'historical_order_count': 0,
            'is_consignment': False,
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
            'recommended_quantity': 0,
            'reasoning': ''
        }
        recommendations.append(rec)
        
    print(f"Loaded {len(recommendations)} candidates.")
    
    # Run Allocation
    result = engine.apply_greenfield_allocation(recommendations, budget)
    final_recs = result['recommendations']
    summary = result['summary']
    
    # Analyze Results
    allocated = [r for r in final_recs if r['recommended_quantity'] > 0]
    skipped = [r for r in final_recs if r['recommended_quantity'] == 0]
    
    print(f"\nRESULTS SUMMARY:")
    print(f"  Allocated: {len(allocated)} SKUs")
    print(f"  Skipped:   {len(skipped)} SKUs")
    print(f"  Spend:     KES {summary.get('pass1_cash',0) + summary.get('pass2_cash',0):,.0f}")
    
    # Deep Dive: High Velocity Depth (Milk/Bread)
    print(f"\nKEY ITEM DEPTH CHECK:")
    key_items = ['BROOKSIDE 500ML', 'FRESH MILK', 'BREAD', 'DAIRY']
    
    found_key = []
    for r in allocated:
        p_name = r['product_name'].upper()
        if any(k in p_name for k in key_items) and r.get('avg_daily_sales', 0) > 5.0:
            found_key.append(r)
            
    found_key.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
    
    for r in found_key[:10]:
        qty = r['recommended_quantity']
        sales = r.get('avg_daily_sales', 0.1)
        days = qty / sales if sales > 0 else 999
        print(f"  - {r['product_name']}: {qty} units ({days:.1f} days) | Sales: {sales:.1f}/day")

if __name__ == "__main__":
    debug_3m_allocation()
