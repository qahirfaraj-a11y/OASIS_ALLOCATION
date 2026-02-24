
import pandas as pd
import logging
import sys
import os

# Setup paths
sys.path.append(os.path.join(os.getcwd(), 'oasis'))
from logic.order_engine import OrderEngine
from simulation.data_loader import HistoricalDataLoader

# Configure Logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("FreshDebug")

def debug_fresh_logic():
    print("--- DEBUGGING FRESH ALLOCATION (GRN FREQUENCY) ---")
    
    # 1. Initialize Engine
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    engine = OrderEngine(data_dir=data_path)
    loader = HistoricalDataLoader(data_dir=data_path)
    
    # 2. Load Data
    print("Loading Monthly Demand...")
    seasonal_map = loader.load_monthly_demand("MAR") # Use March as proxy
    
    # 3. Create Test Candidates (Fresh Items with known Frequencies)
    # We will manually construct these to match what the engine expects
    # Frequencies from JSON:
    # BIO 1L FRESH WHOLE MILK: 1.0 (Daily)
    # TUZO 500ML FRESH MILK (POUCH): 1.0 (Daily)
    # BIO 500ML MAZIWA LALA SWEETENED: 0.375 (Every 3 days)
    # BROOKSIDE 1LT UHT WHOLE MILK: 0.9375 (High) -> But is UHT?
    # BROOKSIDE 1LT UHT LOWFAT MILK: 0.9375 
    # INDOMIE (Dry) -> Should be ignored by this logic
    
    test_items = [
        {'product_name': 'BIO 1L FRESH WHOLE MILK', 'avg_daily_sales': 14.5, 'is_fresh': True, 'dept': 'MILK'},
        {'product_name': 'TUZO 500ML FRESH MILK (POUCH)', 'avg_daily_sales': 60.0, 'is_fresh': True, 'dept': 'MILK'},
        {'product_name': 'BIO 500ML MAZIWA LALA SWEETENED', 'avg_daily_sales': 5.0, 'is_fresh': True, 'dept': 'MILK'},
        {'product_name': 'BROOKSIDE 1LT UHT WHOLE MILK', 'avg_daily_sales': 33.0, 'is_fresh': True, 'dept': 'MILK'}, # UHT Checking
        {'product_name': 'FESTIVE 400G MILKY WHITE SLICED', 'avg_daily_sales': 31.0, 'is_fresh': True, 'dept': 'BREAD'}
    ]
    
    # Mock row data for enrichment
    candidates = []
    for item in test_items:
        rec = {
            'product_name': item['product_name'],
            'avg_daily_sales': item['avg_daily_sales'],
            'product_category': item['dept'],
            'cost_price': 50.0,
            'selling_price': 60.0,
            'is_fresh': True,
            'min_display_qty': 3,
            'pack_size': 1,
            'supplier_name': 'TEST SUPPLIER',
            'estimated_delivery_days': 1.0, # Lead Time 1
            'is_staple_override': False, 
            'margin_pct': 0.0,
            'recommended_quantity': 0,
            'supplier_frequency': 'daily'
        }
        candidates.append(rec)

    # 4. Run Allocation
    # We need to bypass the full `apply_greenfield_allocation` complexity and just call the internal logic if possible, 
    # but `apply_greenfield_allocation` does the calculations.
    # We will run the full thing with a small budget to see Day 1 allocation.
    
    result = engine.apply_greenfield_allocation(candidates, total_budget=100000.0, seasonal_demand_map=seasonal_map)
    allocated = {r['product_name']: r for r in result['recommendations']}

    # 5. Analyze Results
    print(f"\n{'PRODUCT':<40} | {'ADS':<6} | {'Freq (JSON)':<6} | {'Calc Cycle':<6} | {'Target':<6} | {'Alloc Qty':<6} | {'Days Cov'}")
    print("-" * 110)
    
    for item in test_items:
        p_name = item['product_name']
        rec = allocated.get(p_name)
        if not rec:
            print(f"{p_name[:40]:<40} | NOT ALLOCATED")
            continue
            
        ads = rec['avg_daily_sales']
        qty = rec['recommended_quantity']
        
        # Get freq manually to display
        freq = engine.grn_frequency_map.get(p_name, -1)
        calc_cycle = 1.0/freq if freq > 0 else 1.0
        
        days_cov = qty / ads if ads > 0 else 0
        
        print(f"{p_name[:40]:<40} | {ads:<6.1f} | {freq:<6.4f} | {calc_cycle:<6.2f} | {'?':<6} | {qty:<6.0f} | {days_cov:.2f}")

if __name__ == "__main__":
    debug_fresh_logic()
