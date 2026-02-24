
import sys
import os
import pandas as pd
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

def debug_5m_allocation():
    print("=" * 60)
    print("DEBUG: Day 1 Allocation Analysis (Budget: 5,000,000)")
    print("=" * 60)
    
    budget = 5_000_000
    
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
    
    # Deep Dive: Why Skipped?
    print(f"\nTOP SKIP REASONS:")
    reason_counts = {}
    for r in skipped:
        reason = r.get('reasoning', 'Unknown')
        # Extract main tag
        if "[" in reason:
            tag = reason.split("[")[1].split("]")[0]
        else:
            tag = reason
        reason_counts[tag] = reason_counts.get(tag, 0) + 1
        
    for tag, count in sorted(reason_counts.items(), key=lambda x: x[1], reverse=True)[:10]:
        print(f"  - {tag}: {count}")
        
    # Deep Dive: High Velocity Skips (The Pain Point)
    print(f"\nCRITICAL MISSES (High Velocity > 1.0 but Skipped):")
    critical_misses = [r for r in skipped if r.get('avg_daily_sales', 0) > 1.0]
    critical_misses.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
    
    for r in critical_misses[:10]:
        print(f"  - {r['product_name']} (Sales: {r['avg_daily_sales']:.1f}/day) [{r['product_category']}]")
        print(f"    Reason: {r['reasoning']}")
        
    # Deep Dive: Low Depth (Potential Stockouts)
    print(f"\nLOW DEPTH ALERTS (Allocated but < 3 days coverage):")
    low_depth = []
    for r in allocated:
        qty = r['recommended_quantity']
        sales = r.get('avg_daily_sales', 0.1)
        days = qty / sales if sales > 0 else 999
        if days < 3:
            low_depth.append((r['product_name'], days, qty, sales, r['reasoning']))
            
    low_depth.sort(key=lambda x: x[1])
    for item in low_depth[:10]:
         print(f"  - {item[0]}: {item[1]:.1f} days (Qty: {item[2]}, Sales: {item[3]:.1f})")
         # print(f"    {item[4]}")

if __name__ == "__main__":
    debug_5m_allocation()
