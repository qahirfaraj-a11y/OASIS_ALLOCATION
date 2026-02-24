
import pandas as pd
import logging
import sys
import os
import numpy as np

# Setup paths
sys.path.append(os.path.join(os.getcwd(), 'oasis'))
from logic.order_engine import OrderEngine

# Configure Logging (Keep it quiet)
logging.basicConfig(level=logging.ERROR) 
logger = logging.getLogger("TierVerification")

def run_tier_verification():
    print("--- FRESH ALLOCATION VERIFICATION ACROSS TIERS ---")
    
    # 1. Initialize
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    engine = OrderEngine(data_dir=data_path)
    
    # Load DBs
    print("Loading Databases and Rules...")
    engine.load_local_databases()
    
    # 2. Load Real Data
    print("Loading Inventory (mar_cash.xlsx)...")
    file_path = os.path.join(data_path, 'mar_cash.xlsx')
    if not os.path.exists(file_path):
        print("Error: mar_cash.xlsx not found.")
        return

    # Load and parse
    raw_products = engine.parse_inventory_file(file_path)
    
    # 3. FILTER FOR FRESH ONLY 
    # Focus on the user's core concern: Milk, Bread, UHT
    fresh_keywords = ['MILK', 'BREAD', 'YOGHURT', 'DAIRY', 'CAKE', 'ROLL', 'BUN', 'SCONE', 'UHT', 'LONG LIFE']
    filtered_products = []
    
    for p in raw_products:
        name = p['product_name'].upper()
        if any(k in name for k in fresh_keywords):
            filtered_products.append(p)
            
    print(f"Analyzing {len(filtered_products)} Fresh & Long Life items.")
    
    # 4. Enrich Filtered
    # 3b. PATCH: Inject Default Cost to prevent Anchor Pruning ($0 spend = pruned)
    for p in filtered_products:
        if p.get('cost_price', 0) == 0:
             # Heuristic: 80% of selling price or default $50
             if p.get('selling_price', 0) > 0:
                 p['cost_price'] = p['selling_price'] * 0.8
             else:
                 p['cost_price'] = 50.0 # Fallback
    
    print("Enriching Item Intelligence (Sales, Profitability, Supplier Patterns)...")
    engine.enrich_product_data(filtered_products)
    
    # PATCH AGAIN: Ensure Cost is non-zero (Enrichment might have overwritten it)
    for p in filtered_products:
        if p.get('cost_price', 0) == 0:
            p['cost_price'] = 50.0
            
    print("Enrichment Complete.")
    
    # 5. Define Tiers
    tiers = [
        {'name': 'Micro', 'budget': 100000.0, 'desc': 'Entry Level'},
        {'name': 'Small', 'budget': 300000.0, 'desc': 'Standard Duka'},
        {'name': 'Super', 'budget': 5000000.0, 'desc': 'Sufficient Capital'}
    ]
    
    results = {}
    
    for tier in tiers:
        print(f"\n>> Simulating {tier['name']} Store (${tier['budget']:,.0f})...")
        
        import copy
        tier_products = copy.deepcopy(filtered_products)
        
        # Run Allocation
        alloc_result = engine.apply_greenfield_allocation(tier_products, total_budget=tier['budget'])
        recs = alloc_result['recommendations']
        
        # Filter allocated
        allocated = [r for r in recs if r['recommended_quantity'] > 0]
        
        # DEBUG: Inspect UHT Items specifically
        print(f"\n   [DEBUG] Tier: {tier['name']} - Inspecting UHT Candidates:")
        uht_candidates = [p for p in tier_products if 'UHT' in p['product_name'].upper() or 'LONG LIFE' in p['product_name'].upper()]
        
        # Print first 3 candidates
        for p in uht_candidates[:3]:
            # Check if allocated
            alloc_rec = next((r for r in recs if r['product_name'] == p['product_name']), None)
            qty = alloc_rec['recommended_quantity'] if alloc_rec else 0
            reason = alloc_rec.get('reasoning', 'N/A') if alloc_rec else 'Not in Recs'
            
            print(f"   - {p['product_name'][:40]}...")
            print(f"     ADS: {p.get('avg_daily_sales', 0):.4f} | Cost: {p.get('cost_price')} | Is Fresh: {p.get('is_fresh')} | Cat: {p.get('product_category')}")
            print(f"     Supp: {p.get('supplier_name')} | Freq: {p.get('supplier_frequency')}")
            print(f"     Alloc Qty: {qty} | Reason: {reason}")
            
        # Capture Specific Examples
        # Find ANY Fresh Milk that got allocated to use as example
        fresh_milk_allocated = [r for r in allocated if 'MILK' in r['product_name'].upper() and 'FRESH' in r['product_name'].upper()]
        if fresh_milk_allocated:
            milk_example = fresh_milk_allocated[0] # Pick the first one
            # print(f"   [DEBUG] Found Milk Example: {milk_example['product_name']}")
        else:
            milk_example = None
            
        uht_example = next((r for r in allocated if 'UHT' in r['product_name'].upper()), None)
        bread_example = next((r for r in allocated if 'BREAD' in r['product_name'].upper()), None)
        
        summary = {
            'Total SKUs': len(allocated),
            'Allocated_Raw': allocated,  # Store raw for lookup
            'Total Cost': sum(r['recommended_quantity'] * r.get('est_cost', r.get('cost_price', 0)) for r in allocated),
            'Fresh Milk': analyze_category(allocated, 'MILK', fresh_only=True),
            'UHT Milk': analyze_category(allocated, 'MILK', uht_only=True),
            'Examples': {
                'Tuzo Fresh': milk_example['recommended_quantity'] if milk_example else 0,
                'UHT Milk': uht_example['recommended_quantity'] if uht_example else 0,
                'Bread': bread_example['recommended_quantity'] if bread_example else 0,
                'Milk Name': milk_example['product_name'] if milk_example else "None"
            }
        }
        results[tier['name']] = summary

    # 6. Print Comparative Table
    print("\n" + "="*95)
    print(f"{'METRIC':<25} | {'MICRO ($100k)':<20} | {'SMALL ($300k)':<20} | {'SUPER ($5M)':<20}")
    print("="*95)
    
    # General Stats
    print(f"{'Total Fresh SKUs Stocked':<25} | {results['Micro']['Total SKUs']:<20} | {results['Small']['Total SKUs']:<20} | {results['Super']['Total SKUs']:<20}")
    
    print("-" * 95)
    # Category Averages
    metrics = [
        ('Fresh Milk (Avg Units)', 'Fresh Milk', 'Avg Units'),
        ('Fresh Milk (Avg Days)',  'Fresh Milk', 'Avg Days Cov'),
        ('UHT Milk (Avg Units)',   'UHT Milk',   'Avg Units'),
        ('UHT Milk (Avg Days)',    'UHT Milk',   'Avg Days Cov'),
    ]
    
    for label, cat, key in metrics:
        row = f"{label:<25} | "
        for t in tiers:
            val = results[t['name']][cat].get(key, 0)
            row += f"{val:<20.1f} | "
        print(row)
        
    print("-" * 95)
    
    # 7. SKU COMPARISON TABLE
    print("\n" + "="*95)
    print(f"{'SKU COMPARISON':<40} | {'MICRO ($100k)':<15} | {'SMALL ($300k)':<15} | {'SUPER ($5M)':<15}")
    print("="*95)
    # Find common best-sellers to compare
    target_skus = [
        ('FRESH MILK', ['FRESH MILK'], 'Fresh Milk (500ml)'),
        ('BREAD', ['BREAD'], 'Bread (400g)'),
        ('LONG LIFE MILK', ['LONG LIFE MILK'], 'Long Life Milk (500ml)')
    ]
    
    for category_label, keywords, display_name in target_skus:
        # Find a representative SKU that exists in the Super tier (most likely to have it)
        # We'll use the super tier list to find a match
        super_recs = results['Super'].get('Allocated_Raw', [])
        
        representative_sku = None
        for r in super_recs:
            p_name = r['product_name'].upper()
            if p_name == category_label:
                 representative_sku = r['product_name']
                 break
                 
        if representative_sku:
            # Get Qty for this SKU across all tiers
            row = f"{display_name:<40} | "
            for t in tiers:
                tier_recs = results[t['name']].get('Allocated_Raw', [])
                match = next((r for r in tier_recs if r['product_name'] == representative_sku), None)
                qty = match['recommended_quantity'] if match else 0
                row += f"{qty:<15} | "
            print(row)
        else:
             print(f"{display_name:<40} | {'N/A':<15} | {'N/A':<15} | {'N/A':<15}")
             
    print("-" * 95)


def analyze_category(recs, dept_keyword, fresh_only=False, uht_only=False):
    items = []
    for r in recs:
        dept = r.get('product_category', '').upper()
        p_name = r.get('product_name', '').upper()
        if dept_keyword in dept or dept_keyword in p_name:
            is_uht = 'UHT' in p_name or 'LONG LIFE' in p_name or 'ESL' in p_name or 'TETRA' in p_name
            is_fresh_flag = r.get('is_fresh', False)
            if uht_only and not is_uht: continue
            if fresh_only and (is_uht or (not is_fresh_flag and 'MAZIWA' not in p_name)): continue
            items.append(r)
            
    if not items: return {'Avg Days Cov': 0, 'Avg Units': 0}
        
    avg_units = sum(r['recommended_quantity'] for r in items) / len(items)
    
    total_cov = 0
    valid = 0
    for r in items:
        ads = r.get('avg_daily_sales', 0)
        if ads > 0:
            total_cov += r['recommended_quantity'] / ads
            valid += 1
    avg_cov = total_cov / valid if valid > 0 else 0
    
    return {'Avg Days Cov': avg_cov, 'Avg Units': avg_units}

if __name__ == "__main__":
    run_tier_verification()
