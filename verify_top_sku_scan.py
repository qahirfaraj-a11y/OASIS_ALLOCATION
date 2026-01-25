import asyncio
import logging
import os
import json
from app.logic.order_engine import OrderEngine

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyTopSKUScan")

async def test_top_sku_scan():
    data_dir = os.path.join(os.getcwd(), 'app', 'data')
    engine = OrderEngine(data_dir)
    
    # Run the intelligence update (This will call scan_sales_profitability)
    print("\n--- Running Full Intelligence Update ---")
    engine.scan_sales_profitability()
    
    # Check if the updated file exists
    updated_file = os.path.join(data_dir, "sales_profitability_intelligence_2025_updated.json")
    if os.path.exists(updated_file):
        print(f"\nSUCCESS: Updated file created at {updated_file}")
        with open(updated_file, 'r', encoding='utf-8') as f:
            data = json.load(f)
            
        print(f"Total items ingested: {len(data)}")
        
        # Verify first item (should be rank 1)
        # Sort items by rank to find the top one
        top_items = sorted(data.items(), key=lambda x: x[1]['sales_rank'])
        if top_items:
            name, info = top_items[0]
            print(f"\nTop Item (Rank 1):")
            print(f"  Name: {name}")
            print(f"  Qty: {info['total_qty_sold']}")
            print(f"  Revenue: {info['revenue']}")
            print(f"  Margin %: {info['margin_pct']}")
    else:
        print("\nFAILURE: Updated file not found.")

if __name__ == "__main__":
    asyncio.run(test_top_sku_scan())
