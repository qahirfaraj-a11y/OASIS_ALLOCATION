
import logging
import sys
import os
import pandas as pd
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))
from oasis.logic.order_engine import OrderEngine

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DebugBridge")

def debug_bridge_gap():
    budget = 50_000_000.0 # Mega Store
    
    engine = OrderEngine("C:/Users/iLink/.gemini/antigravity/scratch")
    engine.load_local_databases()
    
    # Load Scorecard Data
    scorecard_path = "C:/Users/iLink/.gemini/antigravity/scratch/Full_Product_Allocation_Scorecard_v3.csv"
    df = pd.read_csv(scorecard_path, encoding='utf-8-sig') 
    
    # Prepare Input - Focus on High Velocity items that risk stocking out
    recommendations_in = []
    
    # Manually inject Lead Times for testing
    # Assume most dry goods have 7 days lead time
    # Assume fresh goods have 1 day lead time (daily delivery)
    
    for _, row in df.iterrows():
        p_name = str(row.get('Product')).upper()
        
        is_fresh = "MILK" in p_name or "BREAD" in p_name
        lead_time = 1 if is_fresh else 7
        
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
            'estimated_delivery_days': lead_time,
            'is_fresh': is_fresh
        }
        recommendations_in.append(rec)
        
    print(f"Running Allocation for Budget: ${budget:,.0f}...")
    result = engine.apply_greenfield_allocation(recommendations_in, total_budget=budget)
    
    stocked_items = [r for r in result['recommendations'] if r['recommended_quantity'] > 0]
    
    # Analyze Bridge Gap
    # Bridge Gap = Stock Coverage - (Lead Time + 2 Day Safety)
    # If Gap < 0, we will stockout before first order arrives.
    
    print("\n--- BRIDGE GAP ANALYSIS (Top 20 Critical) ---")
    print(f"{'Product':<40} | {'Sales':<6} | {'LeadT':<5} | {'Stock':<6} | {'Cover':<6} | {'Bridge Status'}")
    print("-" * 110)
    
    danger_count = 0
    stocked_items.sort(key=lambda x: x['avg_daily_sales'], reverse=True)
    
    for item in stocked_items[:50]:
        sales = item['avg_daily_sales']
        qty = item['recommended_quantity']
        lead_time = item['estimated_delivery_days']
        
        if sales == 0: continue
        
        coverage = qty / sales
        
        # Required Bridge = Lead Time + 1 Day Processing + 2 Day Safety
        required_days = lead_time + 3
        gap = coverage - required_days
        
        status = f"SAFE (+{gap:.1f}d)"
        if gap < 0:
            status = f"DANGER ({gap:.1f}d)"
            danger_count += 1
            
        if item in stocked_items[:20]:
            print(f"{item['product_name'][:40]:<40} | {sales:<6.1f} | {lead_time:<5} | {qty:<6.0f} | {coverage:<6.1f} | {status}")
            
    print(f"\nAnalysis: {danger_count}/50 Top SKUs fail the Bridge Test (Lead Time + 3 days).")

if __name__ == "__main__":
    debug_bridge_gap()
