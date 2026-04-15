import asyncio
from typing import Dict, Any

from oasis.logic.order_engine import OrderEngine

# Mock Database
mock_databases = {
    'product_supplier_map': {'FRESH MILK': 'DAIRY', 'STAPLE BEANS': 'GENERAL SUPPLIER'},
    'supplier_patterns': {
        'DAIRY': {'estimated_delivery_days': 1, 'order_frequency': 'daily'},
        'GENERAL SUPPLIER': {'estimated_delivery_days': 7, 'order_frequency': 'weekly'}
    },
    'product_department_map': {'FRESH MILK': 'DAIRY', 'STAPLE BEANS': 'GROCERY'},
    'sales_forecasting': {},
    'supplier_quality': {},
    'product_intelligence': {},
    'simulation_feedback': {}
}

async def run_regression_test():
    print("Initializing OrderEngine...")
    import os
    # Use absolute paths for the mock environment
    base_dir = os.path.dirname(os.path.abspath(__file__))
    mock_dir = os.path.join(base_dir, "mock_dir")
    
    # Create required database files for enrichment
    import json
    for db_name, data in mock_databases.items():
        with open(os.path.join(mock_dir, f"{db_name}.json"), 'w') as f:
            json.dump(data, f)

    engine = OrderEngine(data_dir=mock_dir)
    
    inventory_file = os.path.join(mock_dir, "Rhapta_Inventory.csv")
    output_file = os.path.join(mock_dir, "Order_Report.xlsx")

    print(f"Running full analysis for {inventory_file}...")
    
    # Run the full intelligent analysis pipeline
    recommendations = await engine.run_intelligent_analysis(
        file_path=inventory_file,
        output_path=output_file,
        allocation_mode="initial_load",
        total_budget=100000.0
    )
    
    print("\n--- RESULTS ---")
    # Log all items for debugging
    for r in recommendations:
        print(f"Product: {r['product_name']}, Qty: {r.get('recommended_quantity', 0)}, Reasoning: {r.get('reasoning')}")

    fresh_rec = next((r for r in recommendations if 'FRESH MILK' in r['product_name'].upper()), None)
    
    # Check R20 (Fresh MDQ Inflation Override)
    if fresh_rec and fresh_rec.get('recommended_quantity', 0) <= 1.0:
         print("SUCCESS: R20 FIX VERIFIED.")
    else:
         q = fresh_rec.get('recommended_quantity') if fresh_rec else 'N/A'
         print(f"FAILURE: R20 FIX FAILED - Qty: {q}")

    # Check Obsidian Output
    vault_path = os.path.join(base_dir, "oasis", "Oasis")
    print(f"\nVerifying Obsidian Vault at: {vault_path}")
    
    store_note = os.path.join(vault_path, "Entities", "Stores", "Store_Rhapta.md")
    if os.path.exists(store_note):
        print(f"SUCCESS: [[Store_Rhapta]] entity note created.")
        with open(store_note, 'r') as f:
            print("Content Snippet:")
            print(f.read()[:100])
    else:
        print(f"FAILURE: [[Store_Rhapta]] note MISSING at {store_note}")

    neural_dir = os.path.join(vault_path, "Neural_Archive")
    neural_files = os.listdir(neural_dir) if os.path.exists(neural_dir) else []
    if any(f.startswith("Run_") for f in neural_files):
        print(f"SUCCESS: Relational Neural Note found in {neural_dir}")
    else:
        print(f"FAILURE: Neural Note MISSING in {neural_dir}")
    
if __name__ == "__main__":
    asyncio.run(run_regression_test())
