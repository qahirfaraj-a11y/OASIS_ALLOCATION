import asyncio
import os
from oasis.logic.order_engine import OrderEngine

async def run():
    engine = OrderEngine("oasis/data")
    engine.load_local_databases()
    
    input_file = r"C:\Users\iLink\Desktop\Projects\processed_dpl__1776958868.xlsx"
    
    recs = await engine.run_intelligent_analysis(
        file_path=input_file,
        output_path=None,  # We don't need to save
        allocation_mode="replenishment"
    )
    
    for r in recs[:10]:
        print(f"Product: {r['product_name']}")
        print(f"  Historical Avg: {r.get('historical_avg_order_qty')}")
        print(f"  Rec Qty: {r.get('recommended_quantity')}")
        print(f"  Reason: {r.get('reasoning')}")

if __name__ == "__main__":
    asyncio.run(run())
