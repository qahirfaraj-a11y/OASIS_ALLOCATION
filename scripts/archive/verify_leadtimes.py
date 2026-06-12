import sys
import os
import json
import logging

# Path setup to import app.logic
sys.path.append('c:/Users/iLink/.gemini/antigravity/scratch')

from app.logic.order_engine import OrderEngine

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("VerifyLeadTime")

def main():
    data_dir = 'c:/Users/iLink/.gemini/antigravity/scratch/app/data'
    engine = OrderEngine(data_dir)
    
    logger.info("Step 1: Loading Databases...")
    engine.load_local_databases()
    
    logger.info("Step 2: Processing Lead Time Intelligence...")
    engine.update_lead_time_intelligence()
    
    logger.info("Step 3: Verifying Results...")
    patterns_db = engine.databases.get('supplier_patterns', {})
    
    # Check AQUAMIST LIMITED (known to be in GRN sample)
    aquamist = patterns_db.get('AQUAMIST LIMITED')
    if aquamist:
        logger.info(f"AQUAMIST Pattern: {json.dumps(aquamist, indent=2)}")
        if 'estimated_delivery_days' in aquamist:
            logger.info(f"SUCCESS: Lead time for AQUAMIST is {aquamist['estimated_delivery_days']} days")
        else:
            logger.error("FAILURE: estimated_delivery_days missing for AQUAMIST")
    else:
        logger.error("FAILURE: AQUAMIST LIMITED not found in patterns database")

if __name__ == "__main__":
    main()
