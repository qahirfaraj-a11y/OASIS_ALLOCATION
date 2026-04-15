import os
import json
from oasis.exchange.risk_protocol import RiskAssessor
from oasis.exchange.exchange_registry import ExchangeRegistry
from oasis.exchange.clearing_house import ClearingHouse

def test_clearing_house_logic():
    data_dir = "./oasis/data/"
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    
    # Init Components
    risk = RiskAssessor(normal_waste_threshold=0.02)
    reg = ExchangeRegistry(data_dir)
    ch = ClearingHouse(reg)
    
    # 1. Setup Investor
    inv_id = reg.add_investor("Alice", 10000.0)
    print(f"Investor Created: {inv_id} with $10,000")
    
    # 2. Setup Position (Tier 1: Fresh Milk)
    sku_name = "BIO 1L FRESH WHOLE MILK"
    wp_data = risk.calculate_wp({
        "product_name": sku_name,
        "department": "FRESH MILK",
        "avg_daily_sales": 72.0,
        "lata_variance_multiplier": 1.05
    })
    
    pos_id = reg.create_position(sku_name, qty=100, cost_price=150.0, wp_data=wp_data)
    print(f"Position LISTED: {pos_id} | Total Cost: 15,000")
    
    # 3. Fund Position (Need more capital for 15,000)
    reg.registry["investors"][inv_id]["available_capital"] = 20000.0
    funded = reg.fund_position(inv_id, pos_id, yield_pct=0.12)
    assert funded == True
    print(f"Position FUNDED by {inv_id}")
    
    # 4. Simulate a Partial Sale (10 units)
    # Unit Cost: 150. Sale Price: 180.
    # Total Trade: 1,800.
    # Total Cost: 1,500.
    # GP: 300.
    # Expected Splits:
    # GPP Levy (0.5% of 1,800) = 9
    # Oasis Fee (5% of 300) = 15
    # Inv Yield (60% of 300) = 180
    # Store Cut (Residual: 300 - 180 - 15 - 9) = 96
    
    print("\n--- PROCESSING SALE EVENT (10 Units @ 180) ---")
    settlement = ch.process_sale_event(pos_id, qty_sold=10, sale_price=180.0)
    
    print(json.dumps(settlement, indent=2))
    
    # Assertions
    assert settlement["recovered_cost"] == 1500.0
    assert settlement["yield"] == 180.0
    assert settlement["gpp_levy"] == 9.0
    assert settlement["oasis_fee"] == 15.0
    assert settlement["store_opex"] == 96.0
    
    print("\nVERIFICATION SUCCESSful: Revenue Split Mathematics Correct.")
    
    # 5. Check Investor Wallet (Revolving Pool)
    inv = reg.registry["investors"][inv_id]
    # Initial 20,000. Spent 15,000. Available: 5,000.
    # After Sale: 5,000 + 1,500 (recovery) = 6,500.
    assert inv["available_capital"] == 6500.0
    assert inv["yield_generated"] == 180.0
    print(f"Investor Available Capital (Revolving): {inv['available_capital']}")
    print(f"Investor Yield: {inv['yield_generated']}")

if __name__ == "__main__":
    test_clearing_house_logic()
