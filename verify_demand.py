import sys
import os
import json
import logging

# Path setup to import app.logic
sys.path.append('c:/Users/iLink/.gemini/antigravity/scratch')

from app.logic.order_engine import OrderEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyDemand")

def main():
    data_dir = 'c:/Users/iLink/.gemini/antigravity/scratch/app/data'
    engine = OrderEngine(data_dir)
    
    logger.info("Step 1: Loading Databases...")
    engine.load_local_databases()
    
    logger.info("Step 2: Processing All Intelligence Sources...")
    # This will now include Returns, POS, and Transfers
    engine.update_supplier_quality_scores()
    engine.update_demand_intelligence()
    
    logger.info("Step 3: Verifying Movement Results...")
    # Check a few items from jan_cash or transfers
    # Safaricom 100/= is in trn_1_12.xlsx
    forecast_db = engine.databases.get('sales_forecasting', {})
    
    test_items = ['SC100', '1002000900832'] # Safaricom 100 and Airwick (from jan_cash)
    for item in test_items:
        data = forecast_db.get(item)
        if data:
            logger.info(f"Entry for {item}: {json.dumps(data, indent=2)}")
        else:
            # Try searching by code if first try failed
            logger.warning(f"{item} not found in forecasting DB")

if __name__ == "__main__":
    main()
