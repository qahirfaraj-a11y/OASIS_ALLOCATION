import os
import json
import logging
from oasis.logic.order_engine import OrderEngine

# Setup logging to see the scaling messages
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("TestDemandScaling")

def test_demand_scaling():
    data_dir = "C:\\Users\\iLink\\.gemini\\antigravity\\scratch\\data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Mock necessary files for BudgetManager
    with open(os.path.join(data_dir, "no_grn_suppliers.json"), "w") as f:
        json.dump([], f)
    with open(os.path.join(data_dir, "staple_products.json"), "w") as f:
        json.dump(["MILK", "BEANS"], f)
    
    # Create a dummy department_scaling_ratios.csv
    with open(os.path.join(data_dir, "department_scaling_ratios.csv"), "w") as f:
        f.write("Department,Capital_Weight\n")
        f.write("FRESH,0.1\n")
        f.write("GENERAL,0.9\n")

    engine = OrderEngine(data_dir)
    
    # 1. Mock products with LOW ADS to trigger Global Scaling
    # Total monthly sales = (1.0 * 100 * 30) + (0.1 * 60 * 30) = 3000 + 180 = 3180
    # Baseline is 100M. 
    # Scale factor should be ~100M / 3180 = 31,446x
    recs = [
        {
            "product_name": "MILK",
            "product_category": "FRESH",
            "avg_daily_sales": 0.1,
            "selling_price": 60.0,
            "is_fresh": True,
            "pack_size": 1,
            "current_stocks": 0
        },
        {
            "product_name": "BEANS",
            "product_category": "GENERAL",
            "avg_daily_sales": 1.0,
            "selling_price": 100.0,
            "is_fresh": False,
            "pack_size": 10,
            "current_stocks": 0
        }
    ]
    
    # Test with 300,000 budget
    budget = 300000
    
    print(f"\n--- Testing Greenfield Allocation with ${budget:,} Budget ---")
    result = engine.apply_greenfield_allocation(recs, total_budget=budget)
    
    summary = result['summary']
    allocated = {r['product_name']: r for r in result['recommendations']}
    
    print(f"\nSummary:")
    print(f"Total Budget Used: ${summary['total_cash_used']:,.2f}")
    
    for name, r in allocated.items():
        print(f"\nProduct: {name}")
        print(f"  Initial ADS: {0.1 if name == 'MILK' else 1.0}")
        print(f"  Final ADS (Post-Global Scale): {r.get('avg_daily_sales', 0):.2f}")
        print(f"  P1 Scaled ADS (Proportional): {r.get('p1_scaled_ads', 0):.4f}")
        print(f"  Rec Qty: {r.get('recommended_quantity', 0)}")
        print(f"  Reasoning: {r.get('reasoning', '')}")
        
    # Check if BEANS (Staple) got depth
    beans = allocated.get('BEANS')
    if beans:
        # Expected beans ADS after global scale should be huge.
        # But wait, 1.0 * 31446 = 31k units/day.
        # 300k budget / (100 cost) = 3000 units total.
        # So we should spend the WHOLE 300k budget if scaling is working.
        if summary['total_cash_used'] > 250000:
            print("\nSUCCESS: Budget utilized well.")
        else:
            print("\nFAILURE: Budget underutilized.")
            
        if beans.get('recommended_quantity', 0) > 1:
            print("SUCCESS: Depth allocated.")
        else:
            print("FAILURE: No depth allocated.")

if __name__ == "__main__":
    test_demand_scaling()
