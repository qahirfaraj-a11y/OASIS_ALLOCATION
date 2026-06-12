import asyncio
import logging
import os
from app.llm.inference import RuleBasedLLM

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyStrategicLogic")

async def test_strategic_logic():
    # Mock data for testing ABC-XYZ and Sunset
    products = [
        # 1. AX Winner (High Margin, Stable, Low Stock)
        {
            'product_name': 'AX WINNER (BLUE BAND 500G)',
            'current_stocks': 10,
            'avg_daily_sales': 100, # Rapid stockout risk
            'sales_rank': 50, # A-class
            'margin_pct': 30, # High margin
            'demand_cv': 0.1, # X-class
            'abc_rank': 'A',
            'xyz_rank': 'X',
            'is_sunset': False,
            'moq_floor': 50
        },
        # 2. CZ Dud (Low Margin, Erratic, Sufficient Stock)
        {
            'product_name': 'CZ DUD (OFF-BRAND SOAP)',
            'current_stocks': 50,
            'avg_daily_sales': 5, # 10 days cover
            'sales_rank': 900, # C-class
            'margin_pct': 5, # Low margin
            'demand_cv': 0.8, # Z-class
            'abc_rank': 'C',
            'xyz_rank': 'Z',
            'is_sunset': False,
            'upper_coverage_days': 14 # Default for CZ
        },
        # 3. Sunset Item (A-class importance but winding down)
        {
            'product_name': 'SUNSET WINNER (OLD PACKAGING)',
            'current_stocks': 5,
            'avg_daily_sales': 10,
            'sales_rank': 100, # A-rank historical
            'abc_rank': 'A',
            'is_sunset': True # Wind down!
        },
        # 4. Empty Sunset (Minimal refill)
        {
            'product_name': 'EMPTY SUNSET (A-RANK)',
            'current_stocks': 0,
            'avg_daily_sales': 10,
            'sales_rank': 100,
            'abc_rank': 'A',
            'is_sunset': True
        }
    ]
    
    # Enrichment simulation
    for p in products:
        p['selling_price'] = 100.0
        p['last_days_since_last_delivery'] = 5
        p['reorder_point'] = p.get('avg_daily_sales', 0) * 7

    print("\n--- Testing Strategic Classification Decision Matrix ---\n")
    
    llm = RuleBasedLLM()
    recommendations = await llm.analyze(products)
    
    for rec in recommendations:
        name = rec.get('product_name')
        qty = rec.get('recommended_quantity')
        reason = rec.get('reasoning')
        print(f"Product: {name}")
        # Find original data
        orig = next(p for p in products if p['product_name'] == name)
        strategy = f"{orig.get('abc_rank')}{orig.get('xyz_rank', '')}"
        print(f"  Strategy: {strategy} | Sunset: {orig.get('is_sunset', False)}")
        print(f"  Rec Qty: {qty}")
        print(f"  Reasoning: {reason}")
        print("-" * 50)

if __name__ == "__main__":
    asyncio.run(test_strategic_logic())
