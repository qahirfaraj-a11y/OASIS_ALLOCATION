import sys
import os
import pandas as pd
import logging

# Ensure app is in path
sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

# Configure logging to see OrderEngine output
logging.basicConfig(level=logging.INFO, format='%(message)s')

SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v2.csv"

def run_simulation(budget):
    print(f"\n==================================================")
    print(f"SIMULATION: GREENFIELD ALLOCATION (Budget: ${budget:,.0f})")
    print(f"==================================================")
    
    # 1. Load Data
    print("Loading Scorecard Data...")
    if not os.path.exists(SCORECARD_FILE):
        print("Scorecard file not found.")
        return

    df = pd.read_csv(SCORECARD_FILE)
    
    # 2. Convert to OrderEngine Enriched Format
    # Map CSV columns to the dict structure expected by apply_greenfield_allocation
    recommendations = []
    for _, row in df.iterrows():
        rec = {
            'product_name': row.get('Product'),
            'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
            'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
            'product_category': row.get('Department', 'GENERAL'),
            'pack_size': 1, # Default
            'moq_floor': 0,
            'recommended_quantity': 0, # Reset
            'reasoning': ''
        }
        recommendations.append(rec)
        
    print(f"Prepared {len(recommendations)} candidates.")
    
    # 3. Initialize Engine
    engine = OrderEngine(r"c:\Users\iLink\.gemini\antigravity\scratch")
    
    # 4. Run Logic
    final_recs = engine.apply_greenfield_allocation(recommendations, budget)
    
    # 5. Analyze Results
    allocated_value = 0.0
    items_stocked = 0
    anchors_overridden = 0
    wallet_caps = 0
    
    dept_spend = {}
    
    for r in final_recs:
        qty = r['recommended_quantity']
        if qty > 0:
            price = r['selling_price']
            val = qty * price * 0.75 # Cost estimate
            allocated_value += val
            items_stocked += 1
            
            dept = r.get('product_category')
            dept_spend[dept] = dept_spend.get(dept, 0) + val
            
            res = r['reasoning']
            if "ANCHOR OVERRIDE" in res:
                anchors_overridden += 1
            if "WALLET CAP" in res:
                wallet_caps += 1
                
    print(f"\n--- RESULTS ---")
    print(f"Total Allocated: ${allocated_value:,.2f} / ${budget:,.2f} ({allocated_value/budget:.1%})")
    print(f"Items Stocked: {items_stocked} / {len(recommendations)}")
    print(f"Anchors Overridden (Price Ceiling Bypass): {anchors_overridden}")
    print(f"Depth Wallet Caps Hit: {wallet_caps}")
    
    print(f"\nTop 5 Departments by Spend:")
    sorted_depts = sorted(dept_spend.items(), key=lambda x: x[1], reverse=True)[:5]
    for d, v in sorted_depts:
        print(f"  {d}: ${v:,.0f}")

    print(f"\nTop 10 SKUs by Spend:")
    sorted_recs = sorted(final_recs, key=lambda x: x['recommended_quantity'] * x['selling_price'], reverse=True)[:10]
    for r in sorted_recs:
        val = r['recommended_quantity'] * r['selling_price'] * 0.75
        print(f"  {r['product_name']} ({r['product_category']}): ${val:,.2f} ({r['recommended_quantity']} units)")
        
    # Validation Checks
    if budget < 500000:
        if items_stocked < 500:
            print("\n[WARNING] Stock count too low for Small Store!")
        if anchors_overridden == 0:
             print("\n[WARNING] No Anchor Overrides triggered! Check Staple list.")
    else:
        if allocated_value < budget * 0.5:
             print("\n[WARNING] Mega Store massive under-spend! Check wallet rules.")

if __name__ == "__main__":
    if len(sys.argv) > 1:
        run_simulation(float(sys.argv[1]))
    else:
        run_simulation(200000)   # Small
        run_simulation(200000000) # Mega
