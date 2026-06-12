import os
import sys
import pandas as pd
import numpy as np
import logging
from collections import defaultdict
import datetime

# Setup Path
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))

from run_simulation_scenario import run_simulation
from oasis.simulation.simulation_engine import SalesSimulator # Used for access to day factors if needed

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("VerificationRunner")

def run_verification_simulation():
    """
    Runs a 30-day simulation to verify Impact of Allocation Changes (Impulse, Staples, etc.)
    Focuses on Early Stockouts (Day 1-7) to validate Day 1 Allocation Logic.
    """
    print("="*80)
    print("VERIFICATION SIMULATION: ALLOCATION ENGINE v7.1")
    print("Focus: Impulse, Staples, Bakery, Dairy Boosts")
    print("="*80)

    # 1. Run Baseline Simulation (Medium Store - 5M Budget)
    # 5M is enough to get a good range of items, but not too huge to crash Mega logic
    budget = 5_000_000 
    duration = 30
    scenario = "Verification_v7"
    
    print(f"Running {duration}-Day Simulation with ${budget:,.0f} Budget...")
    
    # We capture the return value which contains daily metrics and hopefully explicit stockout data
    # run_simulation returns dict with 'daily_metrics'
    # BUT we need granular SKU-level data.
    # run_simulation exports 'metrics_csv' (daily aggregate) and 'orders_csv'.
    # It DOES NOT export daily stock per SKU by default.
    
    # PROBLEM: We need granular data to check specific categories.
    # We will invoke run_simulation and then we need to rely on what it produces.
    # Since we can't easily change run_simulation internals without editing it,
    # we might strictly rely on the 'orders' (to see if reorders happened)
    # OR we rely on 'simulation_results_*.xlsx' if it produces one (OrderEngine produces it, Simulator maybe not).
    
    # Wait, run_simulation calls OrderEngine.apply_greenfield_allocation
    # This DOES produce an allocation logical output.
    # We should capture THAT to see the 'Target Coverage' and 'Boost Reasons'.
    
    # Let's run it.
    results = run_simulation(
        scenario_name=scenario,
        duration_days=duration,
        budget_override=budget,
        target_month="JAN" # Standard month
    )
    
    # 2. Analyze the Output
    # The simulation runner saves:
    # - orders_Verification_v7_JAN.csv
    # - simulation_metrics_Verification_v7_JAN.csv
    
    # We also need the INITIAL ALLOCATION to know what was stocked and why.
    # run_simulation doesn't save the full allocation breakdown to CSV, only logs it?
    # Actually OrderEngine usually saves 'simulation_results_....xlsx'. 
    # Let's find the most recent one.
    
    latest_sim_file = find_latest_file(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"), "simulation_results_2026")
    print(f"\nAnalyzing Allocation file: {latest_sim_file}")
    
    if not latest_sim_file:
        print("ERROR: No allocation result file found.")
        return

    analyze_allocation_effectiveness(latest_sim_file)

def find_latest_file(directory, prefix):
    files = [f for f in os.listdir(directory) if f.startswith(prefix) and f.endswith(".xlsx")]
    if not files: return None
    files.sort(key=lambda x: os.path.getmtime(os.path.join(directory, x)), reverse=True)
    return os.path.join(directory, files[0])

def analyze_allocation_effectiveness(file_path):
    """
    Reads the Allocation Excel and checks if Boosts were applied.
    """
    df = pd.read_excel(file_path)
    
    # Columns normally: 'Product', 'Category', 'Recommended Qty', 'Reasoning'
    # We need to map them if names differ. 'product_name', 'product_category'
    
    # Normalize cols
    df.columns = [c.lower().replace(' ', '_') for c in df.columns]
    
    # Check for Boost Keywords in Reasoning
    print("\n" + "-"*60)
    print("CATEGORY BOOST VERIFICATION (Day 1 State)")
    print("-" * 60)
    
    categories = {
        'Impulse/Confect': ['LOLLIPOP', 'LOLLYPOP', 'CHUPA', 'CANDY', 'GIANT', 'ORBIT', 'WRIGLEY'],
        'Staples': ['KENSALT', 'NDOVU', 'MAIZE MEAL', 'ATTA', 'SALT', 'FLOUR'],
        'Bakery': ['BREAD', 'FESTIVE', 'NATURES'],
        'Dairy': ['DAIMA', 'BIO', 'FRESH MILK', 'MAZIWA']
    }
    
    stats = defaultdict(lambda: {'count': 0, 'boosted': 0, 'avg_qty': 0.0, 'reasons': []})
    
    for _, row in df.iterrows():
        p_name = str(row.get('product_name', '')).upper()
        reason = str(row.get('reasoning', '')).upper()
        qty = row.get('recommended_quantity', 0)
        
        for cat, keywords in categories.items():
            if any(k in p_name for k in keywords):
                stats[cat]['count'] += 1
                stats[cat]['avg_qty'] += qty
                
                # Check for specific boost text from OrderEngine
                # 'category_boost' or just implicit higher coverage?
                # The code adds "category_boost_reason" to internal dict, does it print to Reasoning?
                # Looking at code: p['category_boost_reason'] = ...
                # But does it make it to 'reasoning' string?
                # In Step 11 View File, I don't see it appending to 'reasoning' string...
                # It sets p['category_boost'] and p['category_boost_reason'].
                # The AI Reasoning prompt generation *might* see it if passed, or it might be used in `calculate_replenishment_target_stock`.
                # Wait, `calculate_replenishment_target_stock` (line 481) calculates target days.
                # Lines 801-846 in `enrich_product_data` MODIFY `target_coverage_days`.
                # This `target_coverage_days` is used in Phase 2 allocation (line 1586 `smart_target_days`).
                # So it DOES affect Quantity.
                # But it might not be explicitly logged in "Reasoning" text unless "SMART TARGET" or similar is logged.
                
                # However, we can check if coverage > baseline (7 days).
                # Approximation: Coverage = Qty / (Avg Daily Sales)
                ads = row.get('avg_daily_sales', 0.1)
                if ads > 0:
                    cov = qty / ads
                    if cov > 10: # Likely boosted
                         stats[cat]['boosted'] += 1
                
                if "BOOST" in reason or "FRESH" in reason: # Simple text check
                     # stats[cat]['boosted'] += 1 # Maybe too loose
                     pass

    print(f"{'Category':<20} | {'Count':<6} | {'Boosted(Est)':<12} | {'Avg Qty':<10}")
    print("-" * 60)
    for cat, stat in stats.items():
        boost_pct = (stat['boosted'] / stat['count'] * 100) if stat['count'] > 0 else 0
        print(f"{cat:<20} | {stat['count']:<6} | {stat['boosted']:<4} ({boost_pct:.0f}%) | {stat['avg_qty']:<10.1f}")

    print("\n" + "="*80)
    print("VERIFICATION COMPLETE")
    print("Please review the detailed logs above.")
    print("="*80)

if __name__ == "__main__":
    run_verification_simulation()
