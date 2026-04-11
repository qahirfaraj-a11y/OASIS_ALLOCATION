
import pandas as pd
import logging
import sys
import os

# Setup paths
sys.path.append(os.path.join(os.getcwd(), 'oasis'))
from logic.order_engine import OrderEngine
from simulation.data_loader import HistoricalDataLoader

# Configure Logging (Suppress INFO to keep output clean, only show our prints)
logging.basicConfig(level=logging.WARNING) 
logger = logging.getLogger("FreshDoc")

def run_fresh_scenarios():
    print("--- FRESH ALLOCATION LOGIC BREAKDOWN (SCENARIOS) ---")
    
    # 1. Initialize Engine
    data_path = os.path.join(os.getcwd(), 'oasis', 'data')
    engine = OrderEngine(data_dir=data_path)
    loader = HistoricalDataLoader(data_dir=data_path)
    
    # 2. Load Metadata (Monthly Demand Proxy)
    # We need this for the engine to run, even if we mock items
    try:
        seasonal_map = loader.load_monthly_demand("MAR") 
    except:
        seasonal_map = {}

    # 3. Define Test Items (The "Market Basket" for Fresh)
    # 3. Define Master Test Items (ADS based on MEGA STORE history)
    # We will simulate that we loaded a MEGA store file with high ADS.
    # The engine should scale this down for Micro/Small stores.
    test_items = [
        {'product_name': 'TUZO 1L FRESH MILK', 'avg_daily_sales': 120.0, 'is_fresh': True, 'dept': 'MILK', 'freq': 1.0, 'UHT': False}, # Mega Store sells 120
        {'product_name': 'FESTIVE BREAD 400G', 'avg_daily_sales': 80.0, 'is_fresh': True, 'dept': 'BREAD', 'freq': 1.0, 'UHT': False},
        {'product_name': 'BIO YOGHURT 500ML', 'avg_daily_sales': 15.0,  'is_fresh': True, 'dept': 'YOGHURT', 'freq': 0.4, 'UHT': False},
        {'product_name': 'BROOKSIDE UHT 1L',   'avg_daily_sales': 45.0, 'is_fresh': True, 'dept': 'MILK', 'freq': 0.9, 'UHT': True}, # UHT is technically Fresh dept 
    ]
    
    # 4. Define Scenarios (Budgets)
    scenarios = [
        {'name': 'Micro Store', 'budget': 100000.0, 'desc': 'Strict JIT, Low Depth'},
        {'name': 'Small Store', 'budget': 300000.0, 'desc': 'Moderate Depth'},
        {'name': 'Super Store', 'budget': 5000000.0, 'desc': 'Full Depth (High Cap)'}
    ]
    
    for scen in scenarios:
        print(f"\n>> SCENARIO: {scen['name'].upper()} (Budget: ${scen['budget']:,.0f})")
        print(f"   Context: {scen['desc']}")
        
        # Prepare Candidates
        candidates = []
        for item in test_items:
            # Mock record
            rec = {
                'product_name': item['product_name'],
                'avg_daily_sales': item['avg_daily_sales'],
                'product_category': item['dept'],
                'cost_price': 50.0,
                'selling_price': 60.0,
                'is_fresh': item['is_fresh'],
                'min_display_qty': 3,
                'pack_size': 1,
                'supplier_name': 'TEST SUPPLIER',
                'estimated_delivery_days': 1.0, 
                'recommended_quantity': 0,
                'supplier_frequency': 'daily',
                'reasoning': '' # Initialize to prevent KeyError
            }
            # Manually inject UHT into name if needed for logic trigger
            if item['UHT']:
                 rec['product_name'] += " UHT"
            
            candidates.append(rec)
            
        # Run Allocation
        # Note: We clear the GRN map to mock specific frequencies if we wanted, 
        # but here we rely on the loaded file. 
        # To test OUR specific frequencies, we might need to patch get_grn_cycle_days 
        # or just rely on the logic:
        # If product name isn't in JSON, it defaults to 1.0.
        # So 'TUZO 1L FRESH MILK' might default to 1.0 (Daily).
        # 'BIO YOGHURT' might default to 1.0.
        # To strictly test frequency logic, we must mock the map.
        
        engine.grn_frequency_map = {
            'TUZO 1L FRESH MILK': 1.0,
            'FESTIVE BREAD 400G': 1.0,
            'BIO YOGHURT 500ML': 0.4, # Every 2.5 days
            'BROOKSIDE UHT 1L UHT': 0.9
        }
        
        result = engine.apply_greenfield_allocation(candidates, total_budget=scen['budget'], seasonal_demand_map=seasonal_map)
        
        print(f"   {'PRODUCT':<30} | {'ADS':<5} | {'Target Days':<11} | {'Alloc Qty':<9} | {'Days Cov':<8}")
        print("   " + "-"*75)
        
        for rec in result['recommendations']:
            p_name = rec['product_name']
            qty = rec['recommended_quantity']
            ads = rec['avg_daily_sales']
            cov = qty / ads if ads > 0 else 0
            
            # Re-calculate target for display
            freq = engine.grn_frequency_map.get(p_name, 1.0)
            target = (1.0/freq) + 0.25 if freq > 0 else 1.25
            if 'UHT' in p_name: target = max(7.0, target)
            
            print(f"   {p_name:<30} | {ads:<5.0f} | {target:<11.2f} | {qty:<9.0f} | {cov:.2f}")

if __name__ == "__main__":
    run_fresh_scenarios()
