import sys
import os
import json
import logging

# Path setup to import app.logic
sys.path.append('c:/Users/iLink/.gemini/antigravity/scratch')

from app.logic.order_engine import OrderEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyPatterns")

def main():
    data_dir = 'c:/Users/iLink/.gemini/antigravity/scratch/app/data'
    engine = OrderEngine(data_dir)
    
    logger.info("Step 1: Loading Databases...")
    engine.load_local_databases()
    
    logger.info("Step 2: Processing PO Patterns...")
    engine.update_supplier_patterns()
    
    logger.info("Step 3: Verifying Results...")
    patterns_db = engine.databases.get('supplier_patterns', {})
    
    # Check BROOKSIDE DAIRY LIMITED (known to be in po_1-2.xlsx)
    brookside = patterns_db.get('BROOKSIDE DAIRY LIMITED')
    if brookside:
        logger.info(f"BROOKSIDE Patterns: {json.dumps(brookside, indent=2)}")
        if brookside['total_orders_2025'] > 0:
            logger.info("SUCCESS: PO patterns were processed for BROOKSIDE")
        else:
            logger.error("FAILURE: BROOKSIDE has 0 orders in the database")
    else:
        logger.error("FAILURE: BROOKSIDE DAIRY LIMITED not found in patterns database")

if __name__ == "__main__":
    main()
