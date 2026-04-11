
import pandas as pd
import logging
import sys
import os
import numpy as np

# Setup paths
sys.path.append(os.path.join(os.getcwd(), 'oasis'))
from logic.order_engine import OrderEngine
from simulation.data_loader import HistoricalDataLoader

# Configure Logging (Keep it quiet)
logging.basicConfig(level=logging.ERROR) 
logger = logging.getLogger("ScaledSimFast")

def run_tier_simulation_fast():
    print("--- SCALED DEMAND SIMULATION (FAST - FRESH ONLY) ---")
    
    # 1. Initialize
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    engine = OrderEngine(data_dir=data_path)
    
    # Load DBs
    print("Loading Databases...")
    engine.load_local_databases()
    
    # 2. Load Real Data
    print("Loading Full Dataset (mar_cash.xlsx)...")
    file_path = os.path.join(data_path, 'mar_cash.xlsx')
    if not os.path.exists(file_path):
        print("Error: mar_cash.xlsx not found.")
        return

    # Load and parse
    raw_products = engine.parse_inventory_file(file_path)
    print(f"Parsed {len(raw_products)} raw products.")
    
    # 3. FILTER FOR FRESH ONLY (Speed Hack)
    # We want Milk, Bread, Yoghurt, maybe key Staples like Sugar/Maize for comparison?
    # Let's stick to Perishables for now as that's the "Fresh Allocation" task.
    
    fresh_keywords = ['MILK', 'BREAD', 'YOGHURT', 'DAIRY', 'CAKE', 'ROLL', 'BUN', 'SCONE', 'UHT', 'LONG LIFE']
    filtered_products = []
    
    for p in raw_products:
        name = p['product_name'].upper()
        if any(k in name for k in fresh_keywords):
            filtered_products.append(p)
            
    print(f"Filtered down to {len(filtered_products)} Fresh/UHT candidates.")
    
    # 4. Enrich Filtered
    print("Enriching Filtered Set...")
    engine.enrich_product_data(filtered_products)
    print("Enrichment Complete.")
    
    # 5. Define Tiers
    tiers = [
        {'name': 'Micro', 'budget': 100000.0},
        {'name': 'Small', 'budget': 300000.0},
        {'name': 'Super', 'budget': 5000000.0}
    ]
    
    results = {}
    
    for tier in tiers:
        print(f"\n>> Simulating {tier['name']} Store (Budget: ${tier['budget']:,.0f})...")
        
        import copy
        tier_products = copy.deepcopy(filtered_products)
        
        # Run Allocation
        # Note: Engine allocates based on budget. Even with few items, it will try to fill them.
        alloc_result = engine.apply_greenfield_allocation(tier_products, total_budget=tier['budget'])
        recs = alloc_result['recommendations']
        
        # Filter allocated
        allocated = [r for r in recs if r['recommended_quantity'] > 0]
        
        # Analyze Categories
        summary = {
            'Total SKUs': len(allocated),
            'Total Cost': sum(r['recommended_quantity'] * r.get('est_cost', r.get('cost_price', 0)) for r in allocated),
            'Bread': analyze_category(allocated, 'BREAD'),
            'Fresh Milk': analyze_category(allocated, 'MILK', fresh_only=True),
            'UHT Milk': analyze_category(allocated, 'MILK', uht_only=True)
        }
        results[tier['name']] = summary
        
        print(f"   Allocated {len(allocated)} SKUs. Cost: ${summary['Total Cost']:,.0f}")

    # 6. Print Comparative Table
    print("\n" + "="*80)
    print(f"{'CATEGORY':<20} | {'METRIC':<15} | {'MICRO ($100k)':<15} | {'SMALL ($300k)':<15} | {'SUPER ($5M)':<15}")
    print("="*80)
    
    metrics = [
        ('Fresh Milk', 'Avg Days Cov'), ('Fresh Milk', 'Avg Units'),
        ('Bread', 'Avg Days Cov'), ('Bread', 'Avg Units'),
        ('UHT Milk', 'Avg Days Cov'), ('UHT Milk', 'Avg Units')
    ]
    
    for cat, met in metrics:
        row = f"{cat:<20} | {met:<15} | "
        for t in tiers:
            name = t['name']
            data = results[name].get(cat, {})
            val = data.get(met, 0)
            row += f"{val:<15.1f} | "
        print(row)

def analyze_category(recs, dept_keyword, fresh_only=False, uht_only=False):
    # Filter items
    items = []
    for r in recs:
        # Check Dept
        dept = r.get('product_category', '').upper()
        p_name = r.get('product_name', '').upper()
        
        # Keyword check (Dept might be 'FRESH')
        if dept_keyword in dept or dept_keyword in p_name:
            # Sub-filters
            is_uht = 'UHT' in p_name or 'LONG LIFE' in p_name or 'ESL' in p_name or 'TETRA' in p_name
            is_fresh_flag = r.get('is_fresh', False)
            
            if uht_only:
                if not is_uht: continue
            elif fresh_only:
                if is_uht: continue 
                if not is_fresh_flag and 'MAZIWA' not in p_name: continue
            
            items.append(r)
            
    if not items:
        return {'Avg Days Cov': 0, 'Avg Units': 0}
        
    total_units = sum(r['recommended_quantity'] for r in items)
    avg_units = total_units / len(items)
    
    total_cov = 0
    valid_cov_count = 0
    for r in items:
        ads = r.get('avg_daily_sales', 0)
        if ads > 0:
            cov = r['recommended_quantity'] / ads
            total_cov += cov
            valid_cov_count += 1
            
    avg_cov = total_cov / valid_cov_count if valid_cov_count > 0 else 0
    
    return {'Avg Days Cov': avg_cov, 'Avg Units': avg_units}

if __name__ == "__main__":
    run_tier_simulation_fast()
