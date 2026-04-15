import os
import logging
from oasis.exchange.kuber_bridge_hook import push_to_kuber
from oasis.exchange.liquidity_report import LiquidityAnalyzer

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KUBER.TestPhase2")

def verify_integration():
    data_dir = "./oasis/data_test_phase2/"
    if os.path.exists(data_dir):
        import shutil
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)
    
    # 1. MOCK O.A.S.I.S. RECOMMENDATIONS
    # This is what OrderEngine would return
    recs = [
        {
            "product_name": "COOKING OIL 2LT",
            "recommended_quantity": 48,
            "cost_price": 580.0,
            "department": "STAPLE",
            "avg_daily_sales": 12.5,
            "lata_var": 0.95
        },
        {
            "product_name": "YOGURT FRESA 250ML",
            "recommended_quantity": 100,
            "cost_price": 65.0,
            "department": "FRESH",
            "avg_daily_sales": 30.0,
            "lata_var": 1.2
        }
    ]
    
    logger.info("=== PHASE 2 TEST: TRIGGERING KUBER HOOK ===")
    push_to_kuber(data_dir, recs)
    
    logger.info("\n=== PHASE 2 TEST: RUNNING SHADOW BANK LEDGER ===")
    analyzer = LiquidityAnalyzer(data_dir)
    analyzer.generate_report()
    
    # 2. VERIFY STATUS
    from oasis.exchange.exchange_registry import ExchangeRegistry
    reg = ExchangeRegistry(data_dir)
    listed_items = [p for p in reg.registry["active_positions"].values() if p["status"] == "LISTED"]
    
    logger.info(f"Verification: {len(listed_items)} items successfully LISTED (Manual Review required).")
    assert len(listed_items) == 2, "Failed to list recommendations!"
    logger.info("SUCCESS: Integration and Listing logic verified.")

if __name__ == "__main__":
    verify_integration()
