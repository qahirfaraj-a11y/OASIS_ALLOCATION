import asyncio
import os
import json
import pandas as pd
import sys
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from app.logic.order_engine import OrderEngine

# Configuration
DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\app\data'
CATEGORY_FILES = {
    'Diapers': 'diapers.xlsx',
    'Wipes': 'wipes.xlsx',
    'Fabric Conditioner': 'fabricconditioner.xlsx',
    'Sanitary Towels': 'sanitarytowels.xlsx'
}
TARGET_SUPPLIER = 'HAYAT KIMYA  K  H PRODUCTS LTD'

async def run_hayat_oasis():
    print("Initializing O.A.S.I.S. Recommendation Engine...")
    engine = OrderEngine(DATA_DIR)
    
    # Load all intelligence databases (Phases 2 & 3)
    await engine.load_databases_async()
    
    all_hayat_products = []
    
    # Extract Hayat SKUs from category files
    for cat_name, filename in CATEGORY_FILES.items():
        path = os.path.join(DATA_DIR, filename)
        if not os.path.exists(path):
            continue
            
        df = pd.read_excel(path)
        # Filter for Hayat
        hayat_df = df[df['VENDOR_NAME'] == TARGET_SUPPLIER]
        
        for _, row in hayat_df.iterrows():
            # Map category data to O.A.S.I.S. expected format
            product = {
                "product_name": str(row['ITM_NAME']).strip(),
                "barcode": str(row['BARCODE']).strip(),
                "supplier_name": TARGET_SUPPLIER,
                "current_stocks": row['STOCK'] if not pd.isna(row['STOCK']) else 0,
                "product_category": cat_name.lower(),
                "last_days_since_last_delivery": 0, # Default if not known
                "units_sold_last_month": 0, # Will be filled from sales_forecasting
                "selling_price": row['SellPrice'] if not pd.isna(row['SellPrice']) else 0
            }
            all_hayat_products.append(product)

    print(f"Found {len(all_hayat_products)} Hayat SKUs. Enriching data...")
    
    # Enrich with historical sales and patterns
    enriched = engine.enrich_product_data(all_hayat_products)
    
    # Run the AI-powered recommendation logic (Phase 4)
    print("Running 8-Stage Intelligent Analysis...")
    batch_size = 20
    all_recommendations = []
    
    # Note: Using analyze_batch_ai which calls Claude-3-7-Sonnet
    for i in range(0, len(enriched), batch_size):
        batch = enriched[i:i + batch_size]
        print(f"Processing batch { (i // batch_size) + 1 }...")
        recs = await engine.analyze_batch_ai(batch, (i // batch_size) + 1, (len(enriched) + batch_size - 1) // batch_size)
        all_recommendations.extend(recs)
    
    # Save Recommendations
    output_df = pd.DataFrame(all_recommendations)
    
    # Final cleanup of columns for presentation
    cols_to_show = [
        'product_name', 'current_stock', 'recommended_quantity', 
        'sales_velocity', 'reorder_point', 'safety_stock_pct', 'reasoning'
    ]
    # Filter for valid columns present in df
    cols_to_show = [c for c in cols_to_show if c in output_df.columns]
    
    final_report_df = output_df[cols_to_show].sort_values('recommended_quantity', ascending=False)
    
    # Save to Excel and Markdown
    csv_path = 'hayat_po_recommendations.csv'
    final_report_df.to_csv(csv_path, index=False)
    
    with open('hayat_oasis_orders.md', 'w') as f:
        f.write("# O.A.S.I.S. Order Recommendations: Hayat Kimya\n\n")
        f.write("These recommendations were generated using the **8-Stage Intelligent Ordering Pipeline**.\n\n")
        f.write(final_report_df.to_markdown(index=False))
        f.write("\n\n## Recommendation Reasoning Summary\n")
        f.write("1. **Trend & Risk Tuning**: Applied the +20% growth/decline trend adjustments where detected.\n")
        f.write("2. **Defensive Guards**: Blocked orders for slow movers (>200 days) and stale fresh items.\n")
        f.write("3. **Strategic Shield**: Protected against overstocking by capping at upper coverage limits.\n")
        f.write("4. **Pack Rounding**: Final quantities are rounded to logical pack units.\n")

    print(f"PO Recommendations generated: hayat_oasis_orders.md and {csv_path}")

if __name__ == "__main__":
    asyncio.run(run_hayat_oasis())
