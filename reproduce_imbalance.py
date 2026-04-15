import os
import json
from oasis.logic.order_engine import OrderEngine

def test_unbalanced_allocation():
    data_dir = "C:\\Users\\iLink\\.gemini\\antigravity\\scratch\\data"
    os.makedirs(data_dir, exist_ok=True)
    
    # Mock databases
    with open(os.path.join(data_dir, "no_grn_suppliers.json"), "w") as f:
        json.dump([], f)
    
    engine = OrderEngine(data_dir)
    
    # 1. Mock a large list of products: 
    # - 5 High-velocity staples (will eat budget)
    # - 100 Low-velocity discretionary (user wants these expanded)
    recs = []
    
    # High-velocity staples
    for i in range(5):
        recs.append({
            "product_name": f"STAPLE_{i}",
            "product_category": "SUGAR",
            "avg_daily_sales": 20.0,
            "selling_price": 200,
            "margin_pct": 15,
            "sales_rank": 10 + i,
            "current_stocks": 0
        })
        
    # Low-velocity discretionary
    for i in range(100):
        recs.append({
            "product_name": f"DISC_{i}",
            "product_category": "TOYS",
            "avg_daily_sales": 0.2,
            "selling_price": 500,
            "margin_pct": 30,
            "sales_rank": 500 + i,
            "current_stocks": 0
        })

    # Test with 300,000 budget
    budget = 300000
    
    print(f"\n--- Testing Allocation with ${budget:,} Budget ---")
    result = engine.apply_greenfield_allocation(recs, total_budget=budget)
    
    summary = result['summary']
    allocated = [r for r in result['recommendations'] if r.get('recommended_quantity', 0) > 0]
    
    staples = [r for r in allocated if "STAPLE" in r['product_name']]
    disc = [r for r in allocated if "DISC" in r['product_name']]
    
    print(f"Total Budget Spent: ${summary['total_cash_used']:,}")
    print(f"Number of Staples Allocated: {len(staples)} / 5")
    print(f"Number of Discretionary Allocated: {len(disc)} / 100")
    
    if staples:
        avg_staple_spend = sum(r['recommended_quantity'] * r.get('cost_price', r['selling_price']*0.8) for r in staples) / len(staples)
        print(f"Avg Spend per Staple: ${avg_staple_spend:,.2f}")
        
    if disc:
        avg_disc_spend = sum(r['recommended_quantity'] * r.get('cost_price', r['selling_price']*0.8) for r in disc) / len(disc)
        print(f"Avg Spend per Disc: ${avg_disc_spend:,.2f}")
        
    # Check if we hit the "Static 4/6" issue
    min_qty = min([r['recommended_quantity'] for r in allocated])
    max_qty = max([r['recommended_quantity'] for r in allocated])
    print(f"Qty Range: {min_qty} to {max_qty}")

if __name__ == "__main__":
    test_unbalanced_allocation()
