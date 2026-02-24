"""
Run Allocation Simulation (Headless)
=====================================

Runs a full allocation simulation using OrderEngine v2.0 with Gap-11 Flex Pool Fix & Gap-K Supplier Consolidation.
Targeting 'Small Duka' scenario (300k budget) to verify Pass 2B behavior.
"""
import pandas as pd
import sys
import os
import logging

# Ensure app path is in sys.path
sys.path.append(os.getcwd())

from oasis.logic.order_engine import OrderEngine

# Configure Logging
logging.basicConfig(level=logging.INFO, format='%(message)s')
logger = logging.getLogger("OrderEngine")

# Configuration
DATA_DIR = os.getcwd()
SCORECARD_FILE = os.path.join(DATA_DIR, "Full_Product_Allocation_Scorecard_v3.csv")

def load_and_run_allocation():
    print(f"Running Allocation Simulation with Budget: $300,000.00")
    print("="*60)

    # 1. Load Data
    print("Loading scorecard data...")
    try:
        df = pd.read_csv(SCORECARD_FILE, encoding='utf-8-sig') # Handle potential BOM
    except UnicodeDecodeError:
        df = pd.read_csv(SCORECARD_FILE, encoding='latin1')

    # Convert to Recs (Mimic App Logic)
    recommendations = []
    # Fill NaN with safer defaults
    df['Supplier'] = df['Supplier'].fillna('UNKNOWN')
    
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'item_code': str(row.get('Product')), # Dummy ID
            'barcode': str(row.get('Product')), # Dummy Barcode
            'product_category': row.get('Department'),
            'supplier_name': row.get('Supplier'), # CRITICAL FOR GAP K
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0)),
            'selling_price': float(row.get('Unit_Price', 0)),
            'current_stock': float(row.get('Current_Stock', 0)),
            'pack_size': 1, # Default
            'is_consignment': False, # Default
            'ABC_Class': row.get('ABC_Class', 'C'),
            'margin_pct': float(row.get('Margin_Pct', 0)),
            'is_staple': str(row.get('Is_Staple')).upper() == 'TRUE',
            'supplier_reliability': float(row.get('Supplier_Reliability', 0.5)),
            'estimated_delivery_days': float(row.get('Lead_Time_Days', 7))
        }
        recommendations.append(rec)

    # INJECT TEST CASE FOR SUPPLIER CONSOLIDATION
    # High volume item from a supplier that is NOT in the Top 3 for RICE
    # (Top 3 RICE are Capwell, Mjengo, Krish usually)
    # test_rec = {
    #     'product_name': 'TEST RICE - BAD SUPPLIER',
    #     'item_code': 'TEST_001',
    #     'barcode': 'TEST_001',
    #     'product_category': 'RICE',
    #     'supplier_name': 'BAD SUPPLIER LTD',
    #     'avg_daily_sales': 5000.0, # Huge volume to ensure it sorts fairly high
    #     'selling_price': 200.0,
    #     'current_stock': 0,
    #     'pack_size': 1, 
    #     'is_consignment': False,
    #     'ABC_Class': 'A',
    #     'margin_pct': 10.0,
    #     'is_staple': True,
    #     'supplier_reliability': 0.5,
    #     'estimated_delivery_days': 2
    # }
    # recommendations.append(test_rec)
    # print(f"Injected Test Case: {test_rec['product_name']} ({test_rec['supplier_name']})")

    print(f"Loaded {len(recommendations)} products.")

    # 2. Init Engine
    engine = OrderEngine(DATA_DIR)
    
    # 3. Execution
    print("\nExecuting Order Engine...")
    print("-" * 60)
    
    # Run Allocation
    budget = 300000.0
    result = engine.apply_greenfield_allocation(recommendations, budget)
    
    # 4. Analysis
    summary = result['summary']
    allocated = result['recommendations']
    
    print("\nSimulation Results")
    print("="*60)
    print(f"Total Budget:       ${summary['total_budget']:,.2f}")
    print(f"Pass 1 (Width):     ${summary['pass1_cash']:,.2f}")
    print(f"Pass 2 (Depth):     ${summary['pass2_cash']:,.2f}")
    print(f"Pass 2B (Flex):     ${summary.get('pass2b_cash', 0.0):,.2f}")
    print(f"Total Cash Used:    ${summary['total_cash_used']:,.2f}")
    print(f"Unused Budget:      ${summary['unused_budget']:,.2f}")
    
    print("\nFlex Pool Metrics (Gap 11):")
    print(f"- Eligible Items:   {summary.get('pass2b_items_eligible', 'N/A')}")
    print(f"- Enhanced Items:   {summary.get('pass2b_items_enhanced', 'N/A')}")
    
    print("\nSupplier Consolidation Metrics (Gap K):")
    consolidation_tags = [item for item in allocated if "[PASS 1: SUPPLIER CONSOLIDATION]" in item.get('reason_tag', '')]
    skipped_count = summary.get('skip_reasons', {}).get("supplier_consolidation", 0)
    print(f"- Items Skipped due to Consolidation: {skipped_count}")
    
    # Verify specific items
    print("\nSample Skipped Items:")
    skipped_samples = [item for item in recommendations if "[PASS 1: SUPPLIER CONSOLIDATION]" in result['summary'].get('reason_map', {}).get(item['product_name'], '')]
    # Note: reason_map might not be populated in result structure, checking log or allocated list for skipped items is harder without 'all' return
    
    # Check if we have logs for consolidation
    # (Logs act as verification)

if __name__ == "__main__":
    load_and_run_allocation()
