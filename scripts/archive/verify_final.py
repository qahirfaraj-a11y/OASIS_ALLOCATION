import sys
import os
import json
import logging

# Path setup to import app.logic
sys.path.append('c:/Users/iLink/.gemini/antigravity/scratch')

from app.logic.order_engine import OrderEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyFinal")

def main():
    data_dir = 'c:/Users/iLink/.gemini/antigravity/scratch/app/data'
    engine = OrderEngine(data_dir)
    engine.load_local_databases()
    
    forecast_db = engine.databases.get('sales_forecasting', {})
    
    # Check item from list
    test_item = '151  AIRWICK 375ML CITRUS AIR FRESHNER AIR225'
    data = forecast_db.get(test_item)
    if data:
        logger.info(f"SUCCESS: Data for '{test_item}': {json.dumps(data, indent=2)}")
    else:
        logger.error(f"FAILURE: '{test_item}' not found in forecasting DB")

    # Check supplier quality update
    sq_db = engine.databases.get('supplier_quality', {})
    aquamist = sq_db.get('AQUAMIST LIMITED')
    if aquamist:
        logger.info(f"SUCCESS: AQUAMIST quality score: {aquamist['quality_score']} ({aquamist['risk_level']})")
    else:
        logger.error("FAILURE: AQUAMIST LIMITED not found")

if __name__ == "__main__":
    main()
