import sys
import os
import pandas as pd

sys.path.append(os.getcwd())
from allocation_app import load_and_run_allocation, get_engine
from oasis.logic.order_logic_guards import apply_safety_guards

budget = 31550000

from oasis.simulation.data_loader import HistoricalDataLoader
loader = HistoricalDataLoader(os.path.join(os.getcwd(), 'allocation_app.py'))
SCORECARD_FILE = os.path.join(os.getcwd(), "Full_Product_Allocation_Scorecard_v7.csv")
df = pd.read_csv(SCORECARD_FILE)

product_data_map = {}
for _, row in df.iterrows():
    product_name = row.get('Product')
    if product_name:
        product_data_map[product_name] = {
            'margin_pct': row.get('Margin_Pct') if pd.notnull(row.get('Margin_Pct')) else None
        }

recommendations = []
for _, row in df.iterrows():
    rec = {
        'product_name': row.get('Product'),
        'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
        'product_category': row.get('Department', 'GENERAL'),
        'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size', None)) else 1),
        'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
        'recommended_quantity': 0,
        'reasoning': ''
    }
    recommendations.append(rec)

engine = get_engine()
engine.enrich_product_data(recommendations, is_greenfield=True)

# Post-engine cost differences
for r in recommendations[:1000]:
    qty = r.get('recommended_quantity', 1) 
    
    # Engine's version
    engine_cp = engine._get_actual_cost_price(r, float(r.get('selling_price', 0)))
    
    # App's version
    price = float(r.get('selling_price', 0))
    app_cp = 0.0
    if hasattr(engine, 'grn_db'):
        p_name = r['product_name']
        p_barcode = str(r.get('barcode', '')).strip()
        grn_key = p_barcode if p_barcode else engine.normalize_product_name(p_name)
        grn_stat = engine.grn_db.get(grn_key)
        if grn_stat and grn_stat.get('avg_cost'):
            app_cp = float(grn_stat['avg_cost'])
            
    if app_cp == 0.0:
        product_info = product_data_map.get(r['product_name'])
        if product_info:
            margin_pct = product_info['margin_pct']
            if margin_pct is not None:
                try:
                    m = float(margin_pct)
                    if 0 <= m < 100:
                        app_cp = price * (1 - m / 100.0)
                except:
                    pass
    if app_cp <= 0.0:
        app_cp = price * 0.75
        
    if abs(engine_cp - app_cp) > 0.1:
        print(f"Mismatch: {r['product_name']} | Engine CP: {engine_cp:.2f} | App CP: {app_cp:.2f} | GRN Key: {grn_key}")

print("Done checking top 1000.")
