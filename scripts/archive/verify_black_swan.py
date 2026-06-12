import sys
import os
import pandas as pd
from datetime import datetime

# Add parent dir to path
sys.path.append(os.getcwd())

from retail_simulator import RetailSimulator, STORE_UNIVERSES
from oasis.logic.simulation_bridge import SimulationOrderUtil

def test_black_swan_impact():
    print("--- O.A.S.I.S. BLACK SWAN AUDIT ---")
    
    # 1. Setup Baseline
    config = STORE_UNIVERSES["Small_200k"].copy()
    config["budget"] = 200000
    
    bridge = SimulationOrderUtil(os.path.join(os.getcwd(), 'oasis', 'data'))
    
    print("\n[1/3] Running Baseline Simulation (14 Days)...")
    sim_base = RetailSimulator("Baseline", config, seed=42, bridge=bridge)
    result_base = sim_base.run(14)
    
    fill_base = result_base.avg_fill_rate
    print(f"  Baseline Fill Rate: {fill_base:.1f}%")
    
    # 2. Setup Shock (Supplier Failure)
    print("\n[2/3] Running 'Supplier Failure' Shock Simulation (14 Days)...")
    sim_shock = RetailSimulator("Shocked", config, seed=42, bridge=bridge)
    
    # Schedule a massive supplier failure from day 2 to 14
    sim_shock.add_shock(2, 14, "Supplier Failure", magnitude=1.5)
    result_shock = sim_shock.run(14)
    
    fill_shock = result_shock.avg_fill_rate
    print(f"  Shocked Fill Rate: {fill_shock:.1f}%")
    
    # 3. Analyze Impact
    impact = fill_base - fill_shock
    print(f"\n[3/3] Impact Analysis:")
    print(f"  Delta Fill Rate: -{impact:.1f}%")
    
    if impact > 2.0:
        print("\nSUCCESS: Black Swan logic correctly depressed KPIs.")
    else:
        print("\nFAILURE: Black Swan logic had no significant impact.")
        sys.exit(1)

if __name__ == "__main__":
    try:
        test_black_swan_impact()
    except Exception as e:
        print(f"\nAUDIT CRASHED: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)
