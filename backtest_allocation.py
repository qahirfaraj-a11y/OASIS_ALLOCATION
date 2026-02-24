"""
Backtest Orchestrator (GAP-M)
=============================
Automates the circular feedback loop between Allocation (OrderEngine) and Simulation (RetailSimulator).
Runs multiple iterations to demonstrate self-correction of stockouts.

Usage:
    python backtest_allocation.py --tier Large_10M --iterations 3 --days 14

Process:
    1. Run Cleanup (Optional)
    2. Loop N times:
       a. Run Allocation (OrderEngine) -> generates allocation_results.json
       b. Run Simulation (RetailSimulator) -> uses allocation, generates feedback
       c. Analyze Results (Fill Rate, Stockouts)
    3. Generate Report showing improvement over iterations
"""
import argparse
import json
import os
import subprocess
import sys
import time
import pandas as pd
from pathlib import Path

# Setup Paths
SCRATCH_DIR = Path(r"C:\Users\iLink\.gemini\antigravity\scratch")
OASIS_DIR = SCRATCH_DIR / "oasis"
DATA_DIR = OASIS_DIR / "data"
FEEDBACK_FILE = DATA_DIR / "simulation_feedback.json"

def run_command(cmd, description):
    print(f"\n--- {description} ---")
    print(f"Cmd: {cmd}")
    start = time.time()
    result = subprocess.run(cmd, shell=True, cwd=str(SCRATCH_DIR), capture_output=True, text=True)
    duration = time.time() - start
    
    if result.returncode != 0:
        print(f"❌ FAILED ({duration:.1f}s)")
        print(result.stderr)
        return False
    
    print(f"✅ COMPLETED ({duration:.1f}s)")
    # Extract key metrics from output if possible
    return True

def parse_simulation_results(tier):
    # Find latest results file
    files = list(SCRATCH_DIR.glob(f"simulation_results_*.xlsx"))
    if not files:
        return None
    latest = max(files, key=os.path.getctime)
    
    try:
        # Read the KPIs from the text output in the log would be better, 
        # but let's try to infer from the feedback file which is structured
        if FEEDBACK_FILE.exists():
            with open(FEEDBACK_FILE, 'r') as f:
                fb = json.load(f)
                # This file accumulates, so it gives us a global view
                # We need the specfic run stats. 
                # Let's rely on the simulation text output in the main loop instead?
                # Or just grab the summary from the latest Excel if needed.
                pass
    except Exception as e:
        print(f"Error reading results: {e}")
    return latest.name

def main():
    parser = argparse.ArgumentParser(description="OASIS Feedback Loop Orchestrator")
    parser.add_argument("--tier", default="Large_10M", help="Tier to simulate")
    parser.add_argument("--days", type=int, default=14, help="Days to simulate")
    parser.add_argument("--iterations", type=int, default=3, help="Number of feedback loops")
    parser.add_argument("--seed", type=int, default=42, help="Base seed (increments per iter)")
    parser.add_argument("--reset", action="store_true", help="Clear existing feedback")
    
    args = parser.parse_args()
    
    print("="*60)
    print(f"STARTING BACKTEST ORCHESTRATOR (GAP-M)")
    print(f"Tier: {args.tier} | Days: {args.days} | Iterations: {args.iterations}")
    print("="*60)
    
    if args.reset and FEEDBACK_FILE.exists():
        print("Clearing existing feedback data...")
        os.remove(FEEDBACK_FILE)
    
    history = []
    
    for i in range(1, args.iterations + 1):
        print(f"\n\nITERATION {i}/{args.iterations}")
        print("-" * 30)
        
        current_seed = args.seed + i - 1
        
        cmd = f"python retail_simulator.py --tier {args.tier} --days {args.days} --seed {current_seed}"
        
        print(f"Running Simulation & Allocation (Seed {current_seed})...")
        
        # Capture output to parse KPIs directly
        process = subprocess.run(cmd, shell=True, cwd=str(SCRATCH_DIR), capture_output=True, text=True)
        
        if process.returncode != 0:
            print("Simulation Failed:")
            print(process.stderr)
            break
            
        # Parse output for Fill Rate and Stockouts
        output = process.stdout
        # print(output) # Debug
        
        print("\nIteration Results:")
        # Simple extraction
        fill_rate_line = [l for l in output.split('\n') if "Fill Rate:" in l and "[FILL]" in l]
        stockout_line = [l for l in output.split('\n') if "Stockout Rate:" in l]
        
        fill_rate_val = fill_rate_line[0].split(':')[1].strip() if fill_rate_line else "N/A"
        stockout_val = stockout_line[0].split(':')[1].strip() if stockout_line else "N/A"

        if fill_rate_line: print(fill_rate_line[0].strip())
        if stockout_line: print(stockout_line[0].strip())
            
        # Store for summary
        stats = {
            "iteration": i,
            "seed": current_seed,
            "fill_rate": fill_rate_val,
            "stockout_rate": stockout_val
        }
        history.append(stats)
        
        # Check feedback file update
        if FEEDBACK_FILE.exists():
            try:
                with open(FEEDBACK_FILE, 'r') as f:
                    fb = json.load(f)
                    print(f"Feedback Updated: {len(fb.get('sku_feedback', {}))} SKUs tracked")
            except:
                pass
                
    print("\n\n" + "="*60)
    print("IMPROVEMENT REPORT")
    print("="*60)
    print(f"{'Iter':<5} {'Seed':<5} {'Fill Rate':<15} {'Stockout Rate':<15}")
    print("-" * 50)
    for h in history:
        print(f"{h['iteration']:<5} {h['seed']:<5} {h['fill_rate']:<15} {h['stockout_rate']:<15}")

if __name__ == "__main__":
    main()
