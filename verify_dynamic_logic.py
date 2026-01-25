import asyncio
import logging
import os
import json
from datetime import datetime, timedelta
from app.logic.order_engine import OrderEngine

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyDynamicLogic")

async def test_dynamic_logic():
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Mock PO history for recency gating
    # We want to test a recent order vs an old one
    engine._po_history_dates = {
        'BROOKSIDE DAIRY LIMITED': [datetime.now() - timedelta(days=2)], # Recent
        'UNILEVER': [datetime.now() - timedelta(days=15)] # Old
    }

    # Test Case Scenarios
    products = [
        # 1. Stable Top SKU (Low CV)
        {
            'product_name': 'STABLE TOP SKU (MILK)', 
            'supplier_name': 'BROOKSIDE DAIRY LIMITED',
            'current_stocks': 100,
            'units_sold_last_month': 300, # 10/day
            'last_days_since_last_delivery': 2,
            'is_top_sku': True,
            'demand_cv': 0.1, # Stable
            'days_since_last_order': 2,
            'target_coverage_days': 2
        },
        # 2. Volatile Top SKU (High CV)
        {
            'product_name': 'VOLATILE TOP SKU (SODA)', 
            'supplier_name': 'UNILEVER',
            'current_stocks': 100,
            'units_sold_last_month': 300, # 10/day
            'last_days_since_last_delivery': 10,
            'is_top_sku': True,
            'demand_cv': 0.8, # Volatile
            'days_since_last_order': 15,
            'target_coverage_days': 7
        },
        # 3. Overstocked (Gating Test)
        {
            'product_name': 'OVERSTOCKED TOP SKU', 
            'supplier_name': 'UNILEVER',
            'current_stocks': 500, # High stock
            'units_sold_last_month': 300, # 10/day
            'last_days_since_last_delivery': 5,
            'is_top_sku': True,
            'demand_cv': 0.2,
            'days_since_last_order': 15,
            'target_coverage_days': 7
        },
        # 4. Low Stock Stable TOP SKU (Should get small bump)
        {
            'product_name': 'LOW STOCK STABLE TOP SKU', 
            'supplier_name': 'UNILEVER',
            'current_stocks': 10, # Low stock
            'units_sold_last_month': 300, # 10/day
            'last_days_since_last_delivery': 5,
            'is_top_sku': True,
            'demand_cv': 0.1,
            'days_since_last_order': 15,
            'target_coverage_days': 7
        },
        # 5. Low Stock Volatile TOP SKU (Should get large bump)
        {
            'product_name': 'LOW STOCK VOLATILE TOP SKU', 
            'supplier_name': 'UNILEVER',
            'current_stocks': 10, # Low stock
            'units_sold_last_month': 300, # 10/day
            'last_days_since_last_delivery': 5,
            'is_top_sku': True,
            'demand_cv': 0.8,
            'days_since_last_order': 15,
            'target_coverage_days': 7
        }
    ]
    
    # Enriched data (simulate enrichment result)
    for p in products:
        p['avg_daily_sales'] = p['units_sold_last_month'] / 30.0
        p['estimated_daily_sales'] = p['avg_daily_sales']
        p['last_delivery_quantity'] = 100
        p['product_category'] = 'fresh' if 'MILK' in p['product_name'] else 'general'
        p['shelf_life_days'] = 7 if p['product_category'] == 'fresh' else 365
        p['on_order_qty'] = 0
        p['reorder_point'] = p['avg_daily_sales'] * p['target_coverage_days']

    print("\n--- Testing Dynamic Anti-Zero Logic Decision Variation (Rule-Based) ---\n")
    
    # Use RuleBasedLLM for deterministic testing without API keys
    from app.llm.inference import RuleBasedLLM
    llm = RuleBasedLLM()
    recommendations = await llm.analyze(products)
    
    for rec in recommendations:
        name = rec.get('product_name')
        qty = rec.get('recommended_quantity')
        reason = rec.get('reasoning')
        print(f"Product: {name}")
        # Find original data for CV
        orig = next(p for p in products if p['product_name'] == name)
        print(f"  CV: {orig['demand_cv']}")
        print(f"  Stock: {orig['current_stocks']}")
        print(f"  Days Since Last Order: {orig['days_since_last_order']}")
        print(f"  Rec Qty: {qty}")
        print(f"  Reasoning: {reason}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(test_dynamic_logic())
