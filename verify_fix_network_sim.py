
import pandas as pd
from retail_simulator import RetailSimulator, SKUState
from oasis.logic.simulation_bridge import SimulationOrderUtil
import os

# 1. Setup Mock Data
data_dir = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"
bridge = SimulationOrderUtil(data_dir)
df = pd.DataFrame({
    'Product': ['TEST_PROD'],
    'Supplier': ['TEST_SUPP'],
    'Department': ['GENERAL'],
    'Unit_Price': [100.0],
    'Margin_Pct': [25.0],
    'Avg_Daily_Sales': [10.0],
    'Pack_Size': [1],
    'ABC_Class': ['A']
})

# 2. Test Initialization with new arguments (should NOT throw TypeError)
try:
    sim = RetailSimulator(
        tier_name="Small_200k",
        store_config={
            "budget": 200000,
            "safety_days": 12,
            "demand_scale_factor": 0.003,
            "reorder_frequency_days": 3,
            "min_order_value": 2500,
            "description": "Test Store"
        },
        bridge=bridge,
        preloaded_data=df
    )
    print("SUCCESS: RetailSimulator initialized successfully with preloaded_data!")
    
    # Check if bridge was preserved (not overwritten by line 370)
    if sim.bridge == bridge:
        print("SUCCESS: Shared bridge was preserved!")
    else:
        print("FAILURE: Shared bridge was OVERWRITTEN! (Line 370 bug)")
        
except TypeError as e:
    print(f"FAILURE: TypeError still present: {e}")
except Exception as e:
    print(f"FAILURE: Unexpected error: {e}")
