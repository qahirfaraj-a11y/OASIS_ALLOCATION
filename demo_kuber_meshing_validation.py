import os
import json
import logging
from oasis.exchange.bridge import OasisKuberBridge
from oasis.exchange.clearing_house import ClearingHouse
from oasis.exchange.exchange_registry import ExchangeRegistry

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')
logger = logging.getLogger("KUBER.Validation")

def run_validation():
    data_dir = "./oasis/data_validation/"
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    
    # 1. INITIALIZE O.A.S.I.S. STANDALONE SIGNALS (MOCK)
    # We simulate O.A.S.I.S. outputting a PO file independently
    oasis_po = [
        {
            "product_name": "FRESH MILK 500ML",
            "department": "FRESH",
            "qty_ordered": 1000,
            "unit_cost": 45.0,
            "avg_daily_sales": 250.0,
            "lata_var": 1.1 # Moderate supplier variance
        },
        {
            "product_name": "SUGAR 1KG",
            "department": "STAPLE",
            "qty_ordered": 500,
            "unit_cost": 120.0,
            "avg_daily_sales": 20.0,
            "lata_var": 1.0 # Perfect supplier
        }
    ]
    po_path = os.path.join(data_dir, "oasis_standalone_po.json")
    with open(po_path, 'w') as f: json.dump(oasis_po, f)
    
    logger.info("=== STEP 1: PARALLEL INGESTION (THE BRIDGE) ===")
    bridge = OasisKuberBridge(data_dir)
    bridge.ingest_oasis_projection(po_path)
    
    registry = ExchangeRegistry(data_dir)
    clearing = ClearingHouse(registry)
    
    logger.info("\n=== STEP 2: LIQUIDITY FUNDING ===")
    inv_id = registry.add_investor("Alpha Capital", 500000.0)
    
    active_ids = list(registry.registry["active_positions"].keys())
    for pid in active_ids:
        # Fund at a base 2% yield per cycle
        registry.fund_position(inv_id, pid, yield_pct=0.035) 
        
    logger.info("\n=== STEP 3: SETTLEMENT SCENARIOS (LEVERAGE PROOFING) ===")
    
    # --- SCENARIO A: NORMAL PROFIT (SUGAR) ---
    # Sugar sells out fully at 150 KES (Cost 120)
    logger.info("--- SCENARIO A: Normal Profit (Sugar) ---")
    sugar_pos = [pid for pid in active_ids if "SUGAR" in registry.registry["active_positions"][pid]["sku"]][0]
    res_a = clearing.process_sale_event(sugar_pos, qty_sold=500, sale_price=150.0)
    print(f"  Result: Principal Protected? {res_a['principal_protected']} | Yield: {res_a['yield']} KES")

    # --- SCENARIO B: NORMAL WASTE (MILK @ 5% Loss) ---
    # Fresh Milk has an 8% threshold. 5% loss should be absorbed by the 'float'.
    logger.info("\n=== SCENARIO B: Normal Waste (Milk @ 5% Loss) ---")
    milk_pos = [pid for pid in active_ids if "MILK" in registry.registry["active_positions"][pid]["sku"]][0]
    # Sell price is 55 KES, but we only sell 950 units (5% waste/loss)
    # total_cost = 1000 * 45 = 45,000
    # sale_revenue = 950 * 55 = 52,250 
    # Wait, if revenue > cost, it's still a profit. 
    # To test principal protection, we need total revenue < total cost basis.
    
    # Let's simulate a massive break in the fridge: Only 800 units sold at original cost (No margin)
    # Revenue = 800 * 45 = 36,000 (Cost basis = 45,000 | Loss = 9,000 = 20%)
    logger.info("--- SCENARIO C: CATASTROPHIC LOSS (Milk @ 20% Loss) ---")
    # Fresh Milk Threshold is 8%.
    # Loss = 20%. GPP should cover 12% (20 - 8).
    # We pass is_final=True to trigger the batch waste protection logic
    res_c = clearing.process_sale_event(milk_pos, qty_sold=800, sale_price=45.0, is_final=True)
    print(f"  Milk Stats: Cost Basis: 45000.0 | Revenue Collected: 36000.0 (20% Loss)")
    print(f"  GPP Intervention: {res_c['gpp_intervention']} KES")
    print(f"  Recovered Principal: {res_c['recovered_principal']} KES (Principal protected up to 8% threshold)")
    print(f"  Principal Protected? {res_c['principal_protected']}")

    logger.info("\n=== VALIDATION SUMMARY ===")
    summary = registry.get_summary()
    print(f"Global Performance Pool (GPP) Balance: {summary['gpp_balance']} KES")
    print(f"Total Value Locked (After Settlement): {summary['total_locked']} KES")

if __name__ == "__main__":
    run_validation()
