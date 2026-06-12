
import logging
import os
import sys
import pandas as pd
from typing import List, Dict

# Setup Path
sys.path.append(os.path.abspath("C:/Users/iLink/.gemini/antigravity/scratch"))

from oasis.logic.order_engine import OrderEngine
from oasis.simulation.simulation_engine import SalesSimulator, InventoryTracker, RiskModel, ReplenishmentLogic
from oasis.simulation.data_loader import HistoricalDataLoader

# Configure Logger
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("Debug9.28M")

def debug_9m_sim():
    budget_override = 9280000.0 # 9.28M
    target_month = "FEB" 
    duration_days = 12
    benchmark_value = 115_000_000.0
    
    logger.info(f"--- STARTING DEBUG 9.28M SIM (Budget: ${budget_override:,.0f}) ---")
    
    data_dir = "C:/Users/iLink/.gemini/antigravity/scratch/oasis/data"
    scratch_dir = "C:/Users/iLink/.gemini/antigravity/scratch"
    
    # Load Data
    loader = HistoricalDataLoader(data_dir)
    seasonality_map = loader.load_seasonality_indices()
    trend_map = loader.load_item_trends()
    month_factor = seasonality_map.get(target_month, 1.0)
    
    # Load Scorecard
    scorecard_path = os.path.join(scratch_dir, "Full_Product_Allocation_Scorecard_v3.csv")
    try:
        df = pd.read_csv(scorecard_path, encoding='utf-8-sig')
    except:
        df = pd.read_csv(scorecard_path, encoding='latin1')
        
    df['Supplier'] = df['Supplier'].fillna('UNKNOWN')
    
    # SCALING
    traffic_scale = budget_override / benchmark_value
    logger.info(f"Traffic Scale: {traffic_scale:.5f}")
    
    recommendations_in = []
    
    for _, row in df.iterrows():
        p_name = str(row.get('Product')).strip().upper()
        trend = trend_map.get(p_name, 1.0)
        
        raw_sales = float(row.get('Avg_Daily_Sales', 0))
        scaled_sales = raw_sales * traffic_scale # Apply Scale
        
        rec = {
            'product_name': row.get('Product'),
            'item_code': str(row.get('Product')),
            'product_category': row.get('Department'),
            'supplier_name': row.get('Supplier'),
            'avg_daily_sales': scaled_sales, 
            'selling_price': float(row.get('Unit_Price', 0)),
            'current_stock': float(row.get('Current_Stock', 0)),
            'pack_size': 1,
            'is_consignment': False,
            'ABC_Class': row.get('ABC_Class', 'C'),
            'margin_pct': float(row.get('Margin_Pct', 0)),
            'is_staple': str(row.get('Is_Staple')).upper() == 'TRUE',
            'supplier_reliability': float(row.get('Supplier_Reliability', 0.5)),
            'estimated_delivery_days': float(row.get('Lead_Time_Days', 7)), 
            'trend_multiplier': trend 
        }
        recommendations_in.append(rec)
        
    # Run Allocation
    engine = OrderEngine(scratch_dir)
    engine.load_local_databases()
    result = engine.apply_greenfield_allocation(recommendations_in, total_budget=budget_override)
    allocated_items = [r for r in result['recommendations'] if r.get('recommended_quantity', 0) > 0]
    
    logger.info(f"Allocated {len(allocated_items)} Items.")

    # Initialize Sim
    tracker = InventoryTracker()
    tracker.initialize_stock(allocated_items)
    
    sales_sim = SalesSimulator(seed=42)
    replenisher = ReplenishmentLogic(check_frequency_days=1)
    
    logger.info(f"--- RUNNING 12 DAY LOOP ---")
    
    for day in range(1, duration_days + 1):
        # Morning Receive
        received = tracker.receive_stock(day)
        
        # Run Sales
        stats = tracker.process_daily_sales(sales_sim, day, month_factor=month_factor, store_scale_factor=1.0)
        
        fill_rate = 0.0
        if (stats['units_sold'] + stats['stockouts']) > 0:
             fill_rate = stats['units_sold'] / (stats['units_sold'] + stats['stockouts'])
             
        logger.info(f"Day {day}: FillRate={fill_rate:.1%} Stockouts={stats['stockouts']} Received={received}")

        # Reorder Check
        draft_orders = replenisher.check_for_reorder(tracker.inventory, day_index=day, month_factor=month_factor)
        
        if draft_orders:
            for do in draft_orders:
                # SIMULATION RUNNER CONSTANT: arrival = day + 3
                arrival = day + 3 
                tracker.pending_orders.append({
                    'sku': do['sku'],
                    'qty': do['qty'],
                    'arrival_day': arrival
                })

    logger.info("Simulation Complete.")

if __name__ == "__main__":
    debug_9m_sim()
