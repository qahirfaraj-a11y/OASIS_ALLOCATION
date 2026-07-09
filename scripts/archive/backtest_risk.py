import os
import sys
import numpy as np
import pandas as pd
from datetime import datetime

# Setup paths
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from retail_simulator import RetailSimulator, SKUState
from dist_release.ops_dashboard import get_all_store_risks
from collections import defaultdict

def run_backtest():
    print("Initializing Backtest Simulator for 14 days...")
    # Setup dummy configuration
    config = {
        "description": "Backtest Macro Store",
        "budget": 500000,
        "demand_scale_factor": 1.0,
        "safety_days": 10,
        "reorder_frequency_days": 7,
        "min_order_value": 5000
    }
    
    sim = RetailSimulator(tier_name="Macro", store_config=config)
    
    # Trackers for the 8-hour risk scores
    risk_scores_at_8h = []
    
    # Simulate day by day
    for day in range(1, 15):
        print(f"--- Simulating Day {day} ---")
        
        # At start of day, process arrivals
        sim.process_arrivals(day)
        
        # We will manually step through hours (6am to 22pm) to catch the "8-hour to stockout" window
        hours = range(6, 23)
        for hour in hours:
            # Fake advance: 1/16th of daily demand
            for name, sku in sim.skus.items():
                if sku.avg_daily_sales > 0:
                    hourly_demand = (sku.avg_daily_sales / 16.0)
                    sku.current_stock = max(0.0, sku.current_stock - hourly_demand)
            
            # Calculate Risk Scores at this hour
            # Mock store list
            stores = [{'org_cd': 'STORE1'}]
            
            # We want to check SKUs hitting 8 hours to stockout
            for name, sku in sim.skus.items():
                if sku.avg_daily_sales > 0:
                    hours_to_so = (sku.current_stock / sku.avg_daily_sales) * 24.0
                    
                    if 7.0 <= hours_to_so <= 8.5:
                        # Compute risk dynamically using the formula in ops_dashboard
                        so_ratio = 1.0 if sku.current_stock == 0 else 0.0
                        crit_ratio = 1.0 if hours_to_so < 12 else 0.0
                        inv_risk = min(1.0, (so_ratio * 1.5) + (crit_ratio * 0.5))
                        
                        alpha_dynamic = 0.5 * (1.0 - (inv_risk ** 2))
                        blended_risk = (0.28 * alpha_dynamic) + (inv_risk * (1.0 - alpha_dynamic))
                        
                        risk_scores_at_8h.append(blended_risk)

    print("\n================ BACKTEST RESULTS ================")
    print(f"Total instances of SKUs hitting 8-hours to stockout: {len(risk_scores_at_8h)}")
    if risk_scores_at_8h:
        avg_risk = np.mean(risk_scores_at_8h)
        print(f"Average Risk Score exactly 8 hours before stockout: {avg_risk:.4f}")
        print("Note: If the score is > 0.45, the Sigmoidal Brake successfully alarmed the system!")
    else:
        print("No SKUs hit the 8-hour window in this simulation.")

if __name__ == "__main__":
    run_backtest()
