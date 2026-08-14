
import os
import sys
import pandas as pd
import json
import logging
import warnings

# devkit/ -> repo root. This worked transitively before (the sibling import
# below bootstraps it), which is exactly the kind of accident that breaks the
# day someone imports this module instead of running it.
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from run_simulation_scenario import run_simulation

# Suppress warnings
warnings.filterwarnings("ignore")

# Setup Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("TierRunner")

def run_tier_analysis():
    print("="*80)
    print("RUNNING 30-DAY FRESH STOCKOUT ANALYSIS ACROSS TIERS")
    print("="*80)
    
    tiers = [
        {'name': 'Micro', 'budget': 100000.0},
        {'name': 'Small', 'budget': 300000.0},
        {'name': 'Super', 'budget': 5000000.0}
    ]
    
    results = {}
    
    for tier in tiers:
        print(f"\n>> Running Tier: {tier['name']} (${tier['budget']:,.0f})")
        scenario_name = f"Tier_{tier['name']}"
        
        # Run Simulation (30 Days)
        # This will generate oasis/data/simulation_feedback.json
        run_simulation(
            scenario_name=scenario_name,
            duration_days=30,
            target_month="JAN",
            budget_override=tier['budget']
        )
        
        # Read Feedback
        feedback_path = os.path.abspath("oasis/data/simulation_feedback.json")
        if not os.path.exists(feedback_path):
            print(f"ERROR: No feedback file found for {tier['name']}")
            continue
            
        with open(feedback_path, 'r') as f:
            feedback = json.load(f)
            
        sku_data = feedback.get('sku_feedback', {})
        
        # Analyze Categories
        cats = {
            'Fresh Milk': ['FRESH MILK', 'TUZO', 'KCC', 'BROOKSIDE'],
            'Bread': ['BREAD', 'FESTIVE', 'SUPA'],
            'UHT Milk': ['UHT', 'LONG LIFE']
        }
        
        tier_stats = {}
        
        for cat_name, keywords in cats.items():
            # Find relevant SKUs
            relevant_skus = []
            for sku in sku_data.keys():
                sku_upper = sku.upper()
                # Exclude UHT from Fresh Milk check
                if cat_name == 'Fresh Milk' and ('UHT' in sku_upper or 'LONG LIFE' in sku_upper):
                    continue
                    
                if any(k in sku_upper for k in keywords):
                    relevant_skus.append(sku)
            
            # Calculate Metrics
            if not relevant_skus:
                tier_stats[cat_name] = {'Stockout%': 0.0, 'LostSales': 0}
            else:
                avg_stockout_freq = sum(sku_data[s]['stockout_frequency'] for s in relevant_skus) / len(relevant_skus)
                total_lost = sum(sku_data[s]['lost_sales'] for s in relevant_skus)
                tier_stats[cat_name] = {
                    'Stockout%': avg_stockout_freq * 100, # Convert to %
                    'LostSales': total_lost
                }
                
        results[tier['name']] = tier_stats
        
    # Print Comparison Table
    print("\n" + "="*95)
    print(f"{'CATEGORY STOCKOUT RATES (30 Days)':<40} | {'MICRO':<15} | {'SMALL':<15} | {'SUPER':<15}")
    print("="*95)
    
    categories = ['Fresh Milk', 'Bread', 'UHT Milk']
    
    for cat in categories:
        row = f"{cat:<40} | "
        for t in tiers:
            stats = results.get(t['name'], {}).get(cat, {})
            so_pct = stats.get('Stockout%', 0)
            lost = stats.get('LostSales', 0)
            
            # Format: "5.2% (12u)"
            val = f"{so_pct:.1f}% ({int(lost)}u)"
            row += f"{val:<15} | "
        print(row)
        
    print("-" * 95)

if __name__ == "__main__":
    run_tier_analysis()
