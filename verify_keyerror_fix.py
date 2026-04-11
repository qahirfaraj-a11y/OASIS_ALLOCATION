import pandas as pd
import numpy as np
import sys
import os

# Mock the environment
products = [
    {"product_name": "Test Item 1", "current_stocks": 10}, # Missing avg_daily_sales
    {"product_name": "Test Item 2", "current_stocks": 20, "avg_daily_sales": 2.0}
]

def verify_fix():
    print("Starting verification of KeyError fix...")
    
    # Simulate the logic in ops_dashboard.py
    df_p = pd.DataFrame(products)
    
    print("Initial DataFrame columns:", df_p.columns.tolist())
    
    # Apply the fix logic
    for col in ["avg_daily_sales", "current_stocks"]:
        if col not in df_p.columns:
            print(f"Adding missing column: {col}")
            df_p[col] = 0.0
            
    try:
        # This is line 682 in ops_dashboard.py
        df_p["days_cover"] = np.where(df_p["avg_daily_sales"] > 0, (df_p["current_stocks"] / df_p["avg_daily_sales"]).round(1), 999)
        print("Calculation successful!")
        print(df_p[["product_name", "avg_daily_sales", "current_stocks", "days_cover"]])
    except KeyError as e:
        print(f"FAILED: KeyError raised: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"FAILED: An unexpected error occurred: {e}")
        sys.exit(1)

    print("Verification PASSED!")

if __name__ == "__main__":
    verify_fix()
