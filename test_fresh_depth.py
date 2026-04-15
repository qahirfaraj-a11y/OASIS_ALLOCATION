import os
import json
from oasis.logic.order_engine import OrderEngine

def test_fresh_spoilage_prevention():
    data_dir = "C:\\Users\\iLink\\.gemini\\antigravity\\scratch\\data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Mock databases
    with open(os.path.join(data_dir, "no_grn_suppliers.json"), "w") as f:
        json.dump([], f)
    
    engine = OrderEngine(data_dir)
    
    # Mock products
    # 1. Fresh Milk (Department: FRESH MILK) - Should be capped at 1.5 - 2.0 days
    # 2. Bread (Department: BREAD) - Should be capped at 1.5 - 2.0 days
    # 3. Sugar (Department: SUGAR) - Dry staple, should have more depth
    
    recs = [
        {
            "product_name": "FRESH MILK 500ML",
            "department": "FRESH MILK",
            "avg_daily_sales": 1000.0, # High volume
            "selling_price": 60,
            "margin_pct": 10,
            "sales_rank": 1,
            "current_stocks": 0
        },
        {
            "product_name": "WHITE BREAD 400G",
            "department": "BREAD",
            "avg_daily_sales": 500.0,
            "selling_price": 65,
            "margin_pct": 12,
            "sales_rank": 5,
            "current_stocks": 0
        },
        {
            "product_name": "SUGAR 1KG",
            "department": "SUGAR",
            "avg_daily_sales": 300.0,
            "selling_price": 150,
            "margin_pct": 15,
            "sales_rank": 10,
            "current_stocks": 0
        }
    ]

    # Test with Rhapta scale budget ($1.5M)
    budget = 1500000
    
    print(f"\n--- Testing Fresh Spoilage Prevention ($ {budget:,} Budget) ---")
    result = engine.apply_greenfield_allocation(recs, total_budget=budget)
    
    recommendations = result['recommendations']
    
    for r in recommendations:
        name = r['product_name']
        qty = r['recommended_quantity']
        ads = r['avg_daily_sales']
        days = qty / ads if ads > 0 else 0
        
        print(f"Product: {name:20} | Qty: {qty:6} | ADS: {ads:6.1f} | Depth: {days:4.2f} days")
        
        if "MILK" in name or "BREAD" in name:
            # Should be around 1.5 - 2.0 days
            if days > 3.0:
                print(f"  [ERROR] Fresh item {name} has too much depth: {days:.2f} days!")
            else:
                print(f"  [PASS] Fresh item {name} depth is safe: {days:.2f} days.")
        else:
            # Dry items should have more depth (e.g. 14+ days for this budget)
            if days < 5.0:
                print(f"  [WARNING] Dry item {name} has low depth: {days:.2f} days (Expected higher for $1.5M budget)")
            else:
                print(f"  [PASS] Dry item {name} depth is substantial: {days:.2f} days.")

if __name__ == "__main__":
    test_fresh_spoilage_prevention()
