import sys
import os
import json
import logging

# Path setup to import app.logic
sys.path.append('c:/Users/iLink/.gemini/antigravity/scratch')

from app.logic.order_engine import OrderEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyReturns")

def main():
    data_dir = 'c:/Users/iLink/.gemini/antigravity/scratch/app/data'
    engine = OrderEngine(data_dir)
    
    logger.info("Step 1: Loading Databases...")
    engine.load_local_databases()
    
    logger.info("Step 2: Processing Purchase Returns...")
    engine.update_supplier_quality_scores()
    
    logger.info("Step 3: Verifying Results...")
    sq_db = engine.databases.get('supplier_quality', {})
    
    # Check AQUAMIST LIMITED (known to be in prts_1.xlsx)
    aquamist = sq_db.get('AQUAMIST LIMITED')
    if aquamist:
        logger.info(f"AQUAMIST stats: {json.dumps(aquamist, indent=2)}")
        if aquamist['total_returns'] > 0:
            logger.info("SUCCESS: Returns were processed for AQUAMIST")
        else:
            logger.error("FAILURE: AQUAMIST has 0 returns in the database")
    else:
        logger.error("FAILURE: AQUAMIST LIMITED not found in quality database")

if __name__ == "__main__":
    main()
