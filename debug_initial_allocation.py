
import logging
import sys
import os
import pandas as pd
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))
from oasis.logic.order_engine import OrderEngine

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugAlloc")

def analyze_initial_allocation():
    budget = 50_000_000.0 # Mega Store
    
    engine = OrderEngine("C:/Users/iLink/.gemini/antigravity/scratch")
    engine.load_local_databases()
    
    # Load Scorecard Data (Source of Truth for SKUs)
    scorecard_path = "C:/Users/iLink/.gemini/antigravity/scratch/Full_Product_Allocation_Scorecard_v3.csv"
    df = pd.read_csv(scorecard_path, encoding='utf-8-sig') 
    
    # Prepare Input
    recommendations_in = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'item_code': str(row.get('Product')),
            'product_category': row.get('Department'),
            'supplier_name': row.get('Supplier'),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0)),
            'selling_price': float(row.get('Unit_Price', 0)),
            'pack_size': 1,
            'is_consignment': False,
            'ABC_Class': row.get('ABC_Class', 'C'),
            'margin_pct': float(row.get('Margin_Pct', 0)),
             # Assume 7 days lead time for most to see if we cover it
            'estimated_delivery_days': 7 
        }
        recommendations_in.append(rec)
        
    print(f"Running Allocation for Budget: ${budget:,.0f}...")
    result = engine.apply_greenfield_allocation(recommendations_in, total_budget=budget)
    
    stocked_items = [r for r in result['recommendations'] if r['recommended_quantity'] > 0]
    
    print(f"Total SKUs Stocked: {len(stocked_items)}")
    
    # Analyze Top 20 High Velocity Items
    stocked_items.sort(key=lambda x: x['avg_daily_sales'], reverse=True)
    
    print("\n--- TOP 20 HIGH VELOCITY SKUs ANALYSIS ---")
    print(f"{'Product':<40} | {'DailySales':<10} | {'StockQty':<10} | {'Coverage(Days)':<15} | {'Status'}")
    print("-" * 100)
    
    low_coverage_count = 0
    for item in stocked_items[:50]:
        sales = item['avg_daily_sales']
        qty = item['recommended_quantity']
        coverage = qty / sales if sales > 0 else 999
        status = "OK"
        if coverage < 7:
            status = "DANGER (<7d)"
            low_coverage_count += 1
            
        if item in stocked_items[:20]:
            print(f"{item['product_name'][:40]:<40} | {sales:<10.1f} | {qty:<10.0f} | {coverage:<15.1f} | {status}")
            
    print(f"\nAnalysis: {low_coverage_count}/50 Top SKUs have < 7 days coverage.")
    print(f"Pass 2 Consumed: ${result['summary'].get('pass2_cash', 0):,.0f}")
    
if __name__ == "__main__":
    analyze_initial_allocation()
