import asyncio
import os
import logging
import time
from datetime import datetime
from oasis.logic.order_engine import OrderEngine

# Setup Logging
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")
logger = logging.getLogger("OASIS.PRODUCTION")

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

async def run_production():
    logger.info("Starting O.A.S.I.S. Production Run (Full Loop)")
    
    # Initialize Engine
    engine = OrderEngine(DATA_DIR)
    
    # 1. Detect Latest Inventory (Supporting both 'inventory' and '_sl' formats)
    inventory_file = engine.get_latest_inventory_file()
    if not inventory_file:
        files = [f for f in os.listdir(DATA_DIR) if '_sl' in f.lower() and (f.endswith('.csv') or f.endswith('.xlsx'))]
        if files:
            full_paths = [os.path.join(DATA_DIR, f) for f in files]
            inventory_file = max(full_paths, key=os.path.getmtime)
            
    if not inventory_file:
        logger.error("No inventory or stock ledger file found in data directory. Aborting.")
        return
        
    logger.info(f"Targeting Latest Inventory: {os.path.basename(inventory_file)}")
    
    # 2. Setup Output Path
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    output_path = os.path.join(DATA_DIR, f"production_orders_{timestamp}.xlsx")
    
    # 3. Execute Analysis
    # Using replenishment mode and letting the engine calculate budget as requested.
    # Note: We pass a very high default budget if needed, but the engine usually uses internal targets.
    start_time = time.time()
    try:
        results = await engine.run_intelligent_analysis(
            file_path=inventory_file,
            output_path=output_path,
            allocation_mode="replenishment",
            total_budget=10_000_000.0 # High ceiling to avoid artificial capping
        )
        
        duration = time.time() - start_time
        logger.info(f"Production Run Complete in {duration:.2f}s")
        
        # Summary
        if results:
            total_qty = sum(r.get('recommended_quantity', 0) for r in results)
            # Find cost_price if available
            total_cost = sum(r.get('recommended_quantity', 0) * r.get('cost_price', 0) for r in results)
            logger.info(f"Generated recommendations for {len(results)} SKUs.")
            logger.info(f"Total Recommended Qty: {total_qty}")
            logger.info(f"Estimated Deployment: KES {total_cost:,.2f}")
            logger.info(f"Output File: {output_path}")
        else:
            logger.warning("No recommendations generated.")
            
    except Exception as e:
        logger.error(f"Production run failed: {e}", exc_info=True)

if __name__ == "__main__":
    asyncio.run(run_production())
