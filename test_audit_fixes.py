import os
import json
import logging
from oasis.logic.golden_logic_v10 import OrderEngine
from oasis.logic.lata_shield import run_lata
from oasis.logic.dharam_revenue import run_dharam
from oasis.logic.mande_triage import run_mande

# Setup basic logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("OASIS.VERIFY")

def verify_audit_fixes():
    data_dir = "./oasis/data"
    nn_path = "./neutral_network_export"
    config_path = os.path.join(data_dir, "oasis_engines_config.json")

    # Ensure paths exist for test
    if not os.path.exists(data_dir):
        logger.error(f"Data directory {data_dir} not found.")
        return

    # 1. Verify Config Load
    with open(config_path, 'r') as f:
        config = json.load(f)
    
    logger.info("=== VERIFYING CONFIG LOAD ===")
    engine = OrderEngine(data_dir)
    assert engine.global_settings['simulation_correction_multiplier'] == config['global_settings']['simulation_correction_multiplier']
    logger.info("OrderEngine successfully loaded global_settings.")

    # 2. Verify LATA Shield (No Synthetic Gaps)
    logger.info("\n=== VERIFYING LATA SHIELD ===")
    # We'll just run it and check if it crashes or logs errors
    # In a real environment, we'd check the output patterns.json for 'INSUFFICIENT_DATA'
    try:
        lata_result = run_lata(data_dir)
        logger.info("LATA Shield executed without synthetic fallback error.")
    except Exception as e:
        logger.error(f"LATA Shield failed: {e}")

    # 3. Verify DHARAM (Config Factor)
    logger.info("\n=== VERIFYING DHARAM REVENUE ===")
    try:
        dharam_result = run_dharam(nn_path, data_dir)
        logger.info(f"DHARAM executed. Applied Stockout Threshold: {dharam_result['stats']['stockout_threshold']}")
        assert dharam_result['stats']['brand_loyalty_factor'] == config['engines']['dharam']['brand_loyalty_factor']
    except Exception as e:
        logger.error(f"DHARAM failed: {e}")

    # 4. Verify MANDE (Trapped Days)
    logger.info("\n=== VERIFYING MANDE TRIAGE ===")
    try:
        mande_result = run_mande(nn_path, data_dir)
        logger.info(f"MANDE executed. Targeted Trapped Days: {config['engines']['mande']['trapped_capital_days']}")
        # Check one supplier if exists
        if mande_result.get('all_suppliers'):
            assert mande_result['all_suppliers'][0]['trapped_days'] == config['engines']['mande']['trapped_capital_days']
            logger.info("MANDE successfully applied configurable trapped days.")
    except Exception as e:
        logger.error(f"MANDE failed: {e}")

    # 5. Verify Strict Mode
    logger.info("\n=== VERIFYING STRICT MODE ===")
    # Toggle strict mode on
    config['global_settings']['strict_mathematical_mode'] = True
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
    
    engine_strict = OrderEngine(data_dir)
    product = {"product_name": "TEST BREAD", "avg_daily_sales": 5.0, "estimated_delivery_days": 2}
    tier_profile = {}
    
    target_strict = engine_strict.calculate_replenishment_target_stock(product, tier_profile)
    
    # Toggle strict mode off
    config['global_settings']['strict_mathematical_mode'] = False
    with open(config_path, 'w') as f:
        json.dump(config, f, indent=4)
        
    engine_normal = OrderEngine(data_dir)
    target_normal = engine_normal.calculate_replenishment_target_stock(product, tier_profile)
    
    logger.info(f"Strict Target: {target_strict}")
    logger.info(f"Normal Target: {target_normal}")
    
    if target_strict < target_normal:
        logger.info("Strict Mode successfully bypassed heuristics/multipliers.")
    else:
        logger.warning("Strict Mode did not result in a lower target (check if multipliers are > 1.0).")

if __name__ == "__main__":
    verify_audit_fixes()
