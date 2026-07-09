import os
import asyncio
import pandas as pd
from oasis.logic.order_engine import OrderEngine
from oasis.logic.order_logic_guards import apply_safety_guards

async def reproduce():
    data_dir = os.getcwd()
    engine = OrderEngine(data_dir)
    await engine.load_databases_async()
    
    scorecard_file = "Full_Product_Allocation_Scorecard_v7.csv"
    df = pd.read_csv(scorecard_file)
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': int(row.get('Pack_Size', 1) if pd.notnull(row.get('Pack_Size')) else 1),
            'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else 20.0,
            'supplier_name': row.get('Supplier')
        }
        recommendations.append(rec)
    
    products_map = {r['product_name']: r for r in recommendations}

    # Test several budget levels
    budgets = [100_000, 300_000, 1_000_000, 5_000_000, 20_000_000]
    
    for budget in budgets:
        print(f"\n--- Testing Budget: KES {budget:,} ---")
        
        # 1. Enrichment (important!)
        engine.enrich_product_data(recommendations, is_greenfield=True)
        
        # 2. Allocation
        result = engine.apply_greenfield_allocation(recommendations, budget)
        raw_recs = result['recommendations']
        
        # 3. Guards
        final_recs = apply_safety_guards(raw_recs, products_map, allocation_mode="initial_load")
        
        # 4. Utilization Calculation (as in app)
        final_cash = 0.0
        for r in final_recs:
            q = r.get('recommended_quantity', 0)
            if q > 0 and not r.get('is_consignment', False):
                cost_price = r.get('cost_price')
                if cost_price is None:
                    cost_price = float(engine._get_actual_cost_price(r, float(r.get('selling_price', 0))))
                final_cash += round(float(q) * float(cost_price), 2)
        
        utilization = (final_cash / budget) * 100
        print(f"Utilization: {utilization:.2f}%")
        print(f"Cash Spent: KES {final_cash:,.2f}")
        print(f"Residual: KES {budget - final_cash:,.2f}")

if __name__ == "__main__":
    asyncio.run(reproduce())
