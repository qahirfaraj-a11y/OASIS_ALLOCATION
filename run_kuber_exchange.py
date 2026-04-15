import os
import argparse
import logging
import json
from oasis.exchange.risk_protocol import RiskAssessor
from oasis.exchange.exchange_registry import ExchangeRegistry
from oasis.exchange.clearing_house import ClearingHouse
from oasis.exchange.pos_simulator import run_pos_simulation
from oasis.exchange.secondary_market import SecondaryMarket

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("KUBER.Exchange")

def main():
    parser = argparse.ArgumentParser(description="O.A.S.I.S. KUBER v2.0 - The Fractionalized PO Exchange")
    parser.add_argument("--data-dir", default="./oasis/data/", help="Path to oasis/data directory")
    parser.add_argument("--action", choices=["setup", "fund", "pos", "status", "sell-p2p", "buy-p2p", "exit-shadow-bank"], required=True, help="Action to perform")
    parser.add_argument("--investor", help="Investor Name or ID")
    parser.add_argument("--capital", type=float, help="Initial Capital for new investor")
    parser.add_argument("--pos-id", help="Position ID for secondary trade or exit")
    parser.add_argument("--discount", type=float, default=0.01, help="Discount for secondary listing (default 0.01 = 1%)")
    
    args = parser.parse_args()
    
    if not os.path.exists(args.data_dir):
        os.makedirs(args.data_dir)
        
    registry = ExchangeRegistry(args.data_dir)
    risk = RiskAssessor(normal_waste_threshold=0.02)
    
    if args.action == "setup":
        # Create some mock positions to fund
        mock_stock = [
            {"sku": "FRESH MILK 500ML", "dept": "FRESH MILK", "qty": 500, "cost": 45.0, "ads": 150.0},
            {"sku": "WHITE BREAD 400G", "dept": "BREAD", "qty": 300, "cost": 55.0, "ads": 85.0},
            {"sku": "KASUKU 1KG FAT", "dept": "COOKING FAT", "qty": 50, "cost": 150.0, "ads": 1.5}
        ]
        
        for item in mock_stock:
            wp_data = risk.calculate_wp({"product_name": item["sku"], "department": item["dept"], "avg_daily_sales": item["ads"]})
            pos_id = registry.create_position(item["sku"], item["qty"], item["cost"], wp_data)
            print(f"LISTED Position: {pos_id} | SKU: {item['sku']} | Risk: {wp_data['risk_tranche']}")
        
        print("Setup complete. Use --action fund to allocate capital.")

    elif args.action == "fund":
        if not args.investor or not args.capital:
            print("Error: --investor and --capital required for funding.")
            return
            
        inv_id = registry.add_investor(args.investor, args.capital)
        print(f"REGISTERED Investor: {inv_id} | Name: {args.investor}")
        
        # Auto-fund all listed positions
        active_ids = [pid for pid, p in registry.registry["active_positions"].items() if p["status"] == "LISTED"]
        for pid in active_ids:
            pos = registry.registry["active_positions"][pid]
            # Calculate targeted yield based on Risk Assessor
            # We assume a base yield of 6% annualized
            # Since FMCG cycles are fast (7-60 days), we'll use a 2% base yield for this cycle.
            base_yield = 0.02 
            target_yield = risk.assign_yield_target(base_yield, {"risk_tranche": pos["risk_tranche"], "wp_score": pos["wp_score"], "yield_premium": 0.05 if "High" in pos["risk_tranche"] else 0.01})
            
            if registry.fund_position(inv_id, pid, target_yield):
                print(f"  -> FUNDED: {pid} ({pos['sku']}) @ {target_yield*100:.2f}% Yield")
                
    elif args.action == "pos":
        # Run POS Simulation for 30 seconds
        run_pos_simulation(args.data_dir, duration_seconds=30)
        
    elif args.action == "sell-p2p":
        if not args.pos_id:
            print("Error: --pos-id required for secondary listing.")
            return
        market = SecondaryMarket(registry)
        market.list_position(args.pos_id, discount_pct=args.discount)
        
    elif args.action == "buy-p2p":
        if not args.investor or not args.pos_id:
            print("Error: --investor and --pos-id required for P2P purchase.")
            return
        market = SecondaryMarket(registry)
        market.buy_position_p2p(args.investor, args.pos_id)
        
    elif args.action == "exit-shadow-bank":
        if not args.investor or not args.pos_id:
            print("Error: --investor and --pos-id required for Shadow Bank exit.")
            return
        market = SecondaryMarket(registry)
        market.shadow_bank_buyback(args.investor, args.pos_id)
        
    elif args.action == "status":
        summary = registry.get_summary()
        print("\n=== KUBER EXCHANGE STATUS ===")
        print(f"Active Positions: {summary['active_count']}")
        print(f"Funded Investors: {summary['investor_count']}")
        print(f"Total Value Locked (TVL): ${summary['total_locked']:.2f}")
        print(f"Global Performance Pool (GPP): ${summary['gpp_balance']:.6f}")
        
        # Details per investor
        print("\n--- INVESTOR PORTFOLIOS ---")
        for i_id, i in registry.registry["investors"].items():
            print(f"{i['name']} ({i_id}):")
            print(f"  Available: ${i['available_capital']:.2f} | Yield Earned: ${i['yield_generated']:.2f}")
            print(f"  Active POS count: {len(i['active_positions'])}")

if __name__ == "__main__":
    main()
