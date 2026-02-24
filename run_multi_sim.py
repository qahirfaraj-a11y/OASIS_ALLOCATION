"""
Multi-Tier Multi-Seed Simulation Runner
Runs simulations across all tiers with multiple random seeds to generate
diverse feedback data for the circular feedback loop.
"""
import subprocess
import sys
import os

TIERS = ["Small_200k", "Medium_1M", "Large_10M", "Mega_100M"]  # Correct tier names
SEEDS = [42, 123, 456, 789, 1001]  # 5 different seeds
DAYS = 14  # 2 weeks

def run_simulation(tier: str, seed: int):
    """Run a single simulation with specified tier and seed."""
    cmd = [
        sys.executable, "retail_simulator.py",
        "--tier", tier,
        "--seed", str(seed),
        "--days", str(DAYS)
    ]
    
    print(f"\n{'='*60}")
    print(f"Running: {tier} | Seed: {seed}")
    print(f"{'='*60}")
    
    result = subprocess.run(cmd, capture_output=False, text=True)
    return result.returncode == 0

def main():
    print("="*70)
    print("MULTI-TIER MULTI-SEED SIMULATION RUNNER")
    print(f"Tiers: {len(TIERS)} | Seeds per tier: {len(SEEDS)}")
    print(f"Total simulations: {len(TIERS) * len(SEEDS)}")
    print("="*70)
    
    results = []
    
    for tier in TIERS:
        for seed in SEEDS:
            success = run_simulation(tier, seed)
            results.append({
                'tier': tier,
                'seed': seed,
                'success': success
            })
    
    print("\n" + "="*70)
    print("SIMULATION BATCH COMPLETE")
    print("="*70)
    
    # Summary
    successful = sum(1 for r in results if r['success'])
    failed = len(results) - successful
    
    print(f"Successful: {successful}/{len(results)}")
    print(f"Failed: {failed}/{len(results)}")
    
    if failed > 0:
        print("\nFailed simulations:")
        for r in results:
            if not r['success']:
                print(f"  - {r['tier']} (seed: {r['seed']})")
    
    # Check feedback file
    feedback_path = os.path.join("oasis", "data", "simulation_feedback.json")
    if os.path.exists(feedback_path):
        import json
        with open(feedback_path) as f:
            data = json.load(f)
        print(f"\nFeedback File Status:")
        print(f"  Total simulations: {data.get('simulation_count', 0)}")
        print(f"  SKUs tracked: {len(data.get('sku_feedback', {}))}")
        print(f"  Tiers covered: {list(data.get('tier_feedback', {}).keys())}")
        problem_skus = len([s for s in data.get('sku_feedback', {}).values() 
                          if s.get('stockout_frequency', 0) > 0.5])
        print(f"  Problem SKUs (>50% stockout): {problem_skus}")

if __name__ == "__main__":
    main()
