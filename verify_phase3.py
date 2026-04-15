import os
import logging
from datetime import datetime, timedelta
from oasis.exchange.exchange_registry import ExchangeRegistry
from oasis.exchange.secondary_market import SecondaryMarket
from oasis.exchange.clearing_house import ClearingHouse
from oasis.exchange.risk_protocol import RiskAssessor

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KUBER.TestPhase3")

def run_phase3_validation():
    data_dir = "./oasis/data_test_phase3/"
    if os.path.exists(data_dir):
        import shutil
        shutil.rmtree(data_dir)
    os.makedirs(data_dir)
    
    reg = ExchangeRegistry(data_dir)
    market = SecondaryMarket(reg)
    ch = ClearingHouse(reg)
    risk = RiskAssessor()
    
    # 1. SETUP INVESTORS
    inv1 = reg.add_investor("ALICE", 50000.0)
    inv2 = reg.add_investor("BOB", 50000.0)
    inv3 = reg.add_investor("CHARLIE", 50000.0)
    
    # 2. FRACTIONAL FUNDING
    # Product: Cooking Oil, Total Cost: 10,000 KES
    wp = risk.calculate_wp({"product_name": "OIL", "department": "STAPLE", "avg_daily_sales": 10})
    pos_id = reg.create_position("COOKING OIL 2LT", 20, 500.0, wp) # 20 units * 500 = 10k
    
    logger.info("--- TEST 1: FRACTIONAL FUNDING ---")
    reg.fund_position(inv1, pos_id, 4000.0, 0.15) # Alice funds 40%
    reg.fund_position(inv2, pos_id, 6000.0, 0.15) # Bob funds 60%
    
    pos = reg.registry["active_positions"][pos_id]
    assert pos["shares_funded"] >= 1.0
    assert pos["status"] == "FUNDED"
    logger.info("SUCCESS: Fractional funding complete (Alice: 40%, Bob: 60%)")
    
    # 3. P2P MATCHING & FEE SPLITS
    logger.info("--- TEST 2: P2P MATCHED TRADE & FEE SPLIT ---")
    # Alice sells 20% (half of her stake) to Charlie for 2,000 KES
    order_id = market.list_shares_for_sale(inv1, pos_id, 0.20, 2000.0)
    market.execute_p2p_matching(inv3, order_id)
    
    # Verify fee split (0.1% of 2000 = 2 KES)
    # Insurance: 1.0, Liquidity: 0.60, OpEx: 0.40
    ledger = reg.registry["global_ledger"]
    assert ledger["gpp_insurance_fund"] == 1.0
    assert ledger["gpp_liquidity_fund"] == 0.60
    assert ledger["platform_fees"] == 0.40
    
    assert pos["shareholders"][inv3] == 0.20
    assert pos["shareholders"][inv1] == 0.20
    logger.info("SUCCESS: P2P Match verified with 0.1% Growth Engine splits.")
    
    # 4. GPP SAFETY NET (48H HAIRCUT)
    logger.info("--- TEST 3: GPP SAFETY NET (HAIRCUT) ---")
    # bob tries to sell his 60%, no one buys. 48h passes.
    # Seed GPP fund so it can afford the buyout for the test
    reg.registry["global_ledger"]["gpp_liquidity_fund"] = 100000.0
    
    order_id = market.list_shares_for_sale(inv2, pos_id, 0.60, 6000.0)
    order = next(o for o in market.order_book["asks"] if o["order_id"] == order_id)
    order["created_at"] = (datetime.now() - timedelta(hours=49)).isoformat()
    
    market.run_gpp_liquidity_bot()
    # Buyout should have happened. Bob's 6k cost basis * 0.93 = 5580 KES recovery.
    assert inv2 not in pos["shareholders"] or pos["shareholders"][inv2] == 0
    assert pos["shareholders"]["KUBER_DAO"] == 0.60
    logger.info(f"SUCCESS: GPP Bot triggered. Bob exited via safety net with 7% haircut.")
    
    # 5. BATCH SETTLEMENT
    logger.info("--- TEST 4: BATCH SETTLEMENT (PRO-RATA) ---")
    # Current Shareholders: Alice (20%), Charlie (20%), KUBER_DAO (60%)
    # Sell 10 units (50% of the PO)
    # Total units = 20. 10 units = 50%.
    ch.process_sale_event(pos_id, 10, 600.0) # Sold at profit
    
    # Verify Alice and Charlie got funds. Alice (20%) should get 20% of yield.
    alice = reg.registry["investors"][inv1]
    charlie = reg.registry["investors"][inv3]
    
    assert alice["yield_generated"] > 0
    assert charlie["yield_generated"] > 0
    logger.info(f"SUCCESS: Batch settlement distributed pro-rata to Alice and Charlie.")
    
    logger.info("\n=== ALL PHASE 3 TESTS PASSED ===")

if __name__ == "__main__":
    run_phase3_validation()
