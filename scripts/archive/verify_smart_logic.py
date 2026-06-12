import os
import sys
import logging

logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")
logger = logging.getLogger("VerifySmartLogic")

# Setup project paths
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine
from oasis.logic.simulation_bridge import SimulationOrderUtil

DATA_DIR = os.path.join(os.getcwd(), "data")

def run_tests():
    logger.info("==============================================")
    logger.info("🧪 RUNNING OASIS SMART ORDERING VERIFICATION TEST 🧪")
    logger.info("==============================================")

    # 1. Initialize Order Engine and verify load
    logger.info("1. Initializing and caching core OrderEngine...")
    engine = OrderEngine(DATA_DIR)
    engine.load_local_databases()
    logger.info("[OK] Core OrderEngine loaded cleanly.")

    # 2. Verify SimulationOrderUtil init with cached engine
    logger.info("2. Initializing SimulationOrderUtil with cached engine...")
    sim_util = SimulationOrderUtil(DATA_DIR, engine=engine)
    logger.info("[OK] SimulationOrderUtil instantiated instantly using cached databases.")

    # Populate dummy configs if missing
    engine.engines_config = {
        "engines": {
            "amit": {"enabled": True},
            "mande": {"enabled": True},
            "halo": {"enabled": True}
        }
    }
    
    # Mock engine databases
    engine.databases['amit_enforcement'] = {"BAD SKU"}
    engine.databases['mande_purge_list'] = {"BAD SUPPLIER"}
    engine.databases['halo_protection_list'] = {"PROTECTED SKU"}

    logger.info("[OK] Mocked Chapter 11 databases configured.")

    # Mock product list to test all logic branches
    mock_skus = [
        # Fresh item - daily supply
        {
            "product_name": "Fresh Milk 1L",
            "supplier_name": "Dairy Supplier",
            "is_fresh": True,
            "avg_daily_sales": 10.0,
            "current_stock": 5,
            "current_stocks": 5,
            "pack_size": 12,
            "cost_price": 80.0,
            "target_coverage_days": 1.2,
            "median_gap_days": 1,
            "lead_time_days": 1,
            "estimated_delivery_days": 1,
            "reorder_point": 12.0
        },
        # Blacklisted SKU
        {
            "product_name": "BAD SKU",
            "supplier_name": "Dairy Supplier",
            "is_fresh": False,
            "avg_daily_sales": 2.0,
            "current_stock": 0,
            "pack_size": 1,
            "cost_price": 100.0,
            "target_coverage_days": 14,
            "median_gap_days": 7,
            "reorder_point": 14.0
        },
        # Purged Supplier SKU
        {
            "product_name": "Trapped Item",
            "supplier_name": "BAD SUPPLIER",
            "is_fresh": False,
            "avg_daily_sales": 2.0,
            "current_stock": 0,
            "pack_size": 1,
            "cost_price": 100.0,
            "target_coverage_days": 14,
            "median_gap_days": 7,
            "reorder_point": 14.0
        },
        # Halo Protected SKU
        {
            "product_name": "PROTECTED SKU",
            "supplier_name": "Dairy Supplier",
            "is_fresh": False,
            "avg_daily_sales": 5.0,
            "current_stock": 1,
            "pack_size": 6,
            "cost_price": 500.0,
            "target_coverage_days": 14,
            "median_gap_days": 7,
            "reorder_point": 10.0
        },
        # Micro-order SKU (Low quantity/price)
        {
            "product_name": "Cheap Tiny Item",
            "supplier_name": "Micro Supplier",
            "is_fresh": False,
            "avg_daily_sales": 0.2,
            "current_stock": 0,
            "pack_size": 1,
            "cost_price": 10.0,
            "target_coverage_days": 14,
            "median_gap_days": 7,
            "reorder_point": 2.0
        }
    ]

    # Prepare SKU data (skip enrichment since we are using mocked products)
    prepared = sim_util.prepare_sku_data(mock_skus, skip_enrichment=True)
    logger.info("3. Running calculate_order_quantity...")
    raw_recs = sim_util.calculate_order_quantity(prepared, use_real_date=False, current_day=7)

    # Validate output recommendations
    logger.info("4. Validating calculate_order_quantity results...")
    
    recs_map = {r['product_name']: r for r in raw_recs}
    
    # Validate Fresh double-stacking fix
    milk = recs_map["Fresh Milk 1L"]
    logger.info(f"  Milk reasoning: {milk['reasoning']}")
    assert "Fresh DDoS Target: 1.20d" in milk['reasoning'], "FAIL: Fresh item did not use lean DDoS target days."
    logger.info("  [PASS] Fresh item double-stacking buffer successfully bypassed.")

    # Validate AMIT blacklist
    bad_sku = recs_map["BAD SKU"]
    logger.info(f"  Bad SKU: qty={bad_sku['recommended_quantity']}, reason={bad_sku['reasoning']}")
    assert bad_sku['recommended_quantity'] == 0 and "AMIT" in bad_sku['reasoning'], "FAIL: AMIT blacklist was not enforced."
    logger.info("  [PASS] AMIT Blacklist successfully enforced.")

    # Validate MANDE purge
    trapped = recs_map["Trapped Item"]
    logger.info(f"  Trapped supplier SKU: qty={trapped['recommended_quantity']}, reason={trapped['reasoning']}")
    assert trapped['recommended_quantity'] == 0 and "MANDE" in trapped['reasoning'], "FAIL: MANDE delisting was not enforced."
    logger.info("  [PASS] MANDE Purge successfully enforced.")

    # Validate HALO Protection
    protected = recs_map["PROTECTED SKU"]
    logger.info(f"  Protected SKU: reason={protected['reasoning']}")
    assert "HALO Protected" in protected['reasoning'], "FAIL: HALO protection was not applied."
    logger.info("  [PASS] HALO protection successfully detected and tagged.")

    # Finalize orders (Rounding, safety caps)
    logger.info("5. Running finalize_orders...")
    finalized = sim_util.finalize_orders(raw_recs)

    # 6. Verify Two-Stage Minimum Order Gate
    logger.info("6. Running apply_minimum_order_gate...")
    gated = sim_util.apply_minimum_order_gate(finalized)
    
    po_recs = gated['po_recs']
    transfer_recs = gated['transfer_recs']
    supplier_summary = gated['supplier_summary']

    po_map = {r['product_name']: r for r in po_recs}
    trans_map = {r['product_name']: r for r in transfer_recs}

    logger.info(f"  PO Recs: {[r['product_name'] for r in po_recs]}")
    logger.info(f"  Transfer Recs: {[r['product_name'] for r in transfer_recs]}")

    # Cheap Tiny Item must be in transfer_recs due to Stage 1 SKU-level MOQ/MOP
    assert "Cheap Tiny Item" in trans_map, "FAIL: Micro-order SKU was not filtered at Stage 1."
    assert "Item MOQ/MOP Gate" in trans_map["Cheap Tiny Item"]["reasoning"], "FAIL: Stage 1 routing reasoning missing."
    logger.info("  [PASS] Stage 1 SKU MOQ/MOP gate successfully routed tiny order to transfers.")

    # Let's check supplier summary
    logger.info(f"  Supplier summary: {supplier_summary}")
    
    logger.info("==============================================")
    logger.info("🎉 ALL VERIFICATION TESTS COMPLETED SUCCESSFULLY 🎉")
    logger.info("==============================================")

if __name__ == "__main__":
    run_tests()
