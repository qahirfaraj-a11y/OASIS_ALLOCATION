import time
import random
import logging
from .exchange_registry import ExchangeRegistry
from .clearing_house import ClearingHouse

logger = logging.getLogger("KUBER.POS")

def run_pos_simulation(data_dir: str, duration_seconds: int = 60):
    """
    Simulates a live POS environment where sales are occurring.
    """
    reg = ExchangeRegistry(data_dir)
    ch = ClearingHouse(reg)
    
    print("=== KUBER v2.0 POS SIMULATOR STARTING ===")
    print(f"Tracking {len(reg.registry['active_positions'])} active inventory positions.")
    
    start_time = time.time()
    try:
        while time.time() - start_time < duration_seconds:
            # 1. Identify an active position to "scan"
            active_ids = [pid for pid, p in reg.registry["active_positions"].items() if p["status"] == "FUNDED" and p["qty_on_hand"] > 0]
            
            if not active_ids:
                print("No funded inventory available for sale. Waiting...")
                time.sleep(5)
                continue
                
            pos_id = random.choice(active_ids)
            pos = reg.registry["active_positions"][pos_id]
            
            # 2. Simulate a scan (random qty 1-3)
            qty_sold = random.randint(1, min(3, pos["qty_on_hand"]))
            
            # 3. Simulate markup (Cost + 10-25%)
            sale_price = pos["unit_cost"] * (1.1 + random.random() * 0.15)
            
            print(f"\n[POS SCAN] SKU: {pos['sku']} | Qty: {qty_sold} | Price: {sale_price:.2f}")
            
            # 4. Trigger Clearing House Settlement
            settlement = ch.process_sale_event(pos_id, qty_sold, sale_price)
            
            if settlement["status"] == "SUCCESS":
                # Update qty_on_hand locally for the next iteration (registry already saved by CH)
                pos["qty_on_hand"] -= qty_sold
                print(f"  -> SETTLED: Investor {settlement['investor_id']} earned +${settlement['yield']}")
                print(f"  -> GPP LEVY: +${settlement['gpp_levy']} | O.A.S.I.S. Fee: +${settlement['oasis_fee']}")
                
                # Check for termination
                if pos["qty_on_hand"] <= 0:
                    ch.terminate_position(pos_id, reason="SOLD_OUT")
                    print(f"  -> POSITION CLOSED: {pos_id} Fully Cleared.")
            else:
                print(f"  -> SETTLEMENT FAILED: {settlement['reason']}")
                
            time.sleep(random.uniform(1, 3)) # Wait for next customer
            
    except KeyboardInterrupt:
        print("\nPOS Simulation Terminated by User.")
    
    print("\n=== POS SIMULATION COMPLETE ===")
    summary = reg.get_summary()
    print(f"Global Volume: ${summary['total_volume']:.2f}")
    print(f"GPP Balance: ${summary['gpp_balance']:.2f}")

if __name__ == "__main__":
    import os
    data_dir = "./oasis/data/"
    if not os.path.exists(data_dir): os.makedirs(data_dir)
    run_pos_simulation(data_dir)
