import pandas as pd
import random
import os

# Create a mock prospect raw CSV
def generate_messy_prospect_data():
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scorecard_path = os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v3.csv")
    
    if not os.path.exists(scorecard_path):
        print(f"Error: Could not find {scorecard_path} to use as a base.")
        return
        
    df = pd.read_csv(scorecard_path)
    
    # Select ~500 items for a good, readable pitch dataset
    df = df.sample(500, random_state=42)
    
    prospect_data = []
    
    # Make some typical "retail" personas
    
    for _, row in df.iterrows():
        p_name = str(row.get('Product', 'Unknown Item'))
        dept = str(row.get('Department', 'General'))
        cost = float(row.get('Unit_Price', random.uniform(50, 5000)))  # Usually scorecard has cost roughly as Unit_Price
        price = round(cost * random.uniform(1.15, 1.4), 2)
        
        # 1. Mess up the naming a bit (like POS systems do)
        raw_name = p_name.lower().title() # Bad casing
        if random.random() < 0.2:
            raw_name = raw_name.replace(" ", "") # Missing spaces
        if random.random() < 0.1:
            raw_name = raw_name + " - OLD"
            
        # 2. Custom weird departments
        prospect_dept = dept
        if random.random() < 0.3:
            prospect_dept = dept + " (Local)"
        elif random.random() < 0.1:
            prospect_dept = "Miscelaneous Store items"  # typo intentional
            
        # 3. Simulate Sales over 30 days
        velocity_profile = random.random()
        
        # Scenario A: DEAD STOCK (High SOH, Zero/Low Sales)
        if velocity_profile < 0.2: 
            soh = random.randint(30, 200)
            qty_sold_30_days = random.randint(0, 2)
            supplier = "Local Distri BUTOR"
            
        # Scenario B: STOCKOUT FAST MOVER (Zero SOH, High Sales history)
        elif velocity_profile < 0.4:
            soh = 0
            qty_sold_30_days = random.randint(50, 300)
            supplier = "FAST MOVERS KENYA"
            
        # Scenario C: Normal item
        else:
            qty_sold_30_days = random.randint(10, 150)
            soh = random.randint(10, 100)
            supplier = row.get('Supplier Name', 'General Wholesaler')
            
        # Messy supplier
        if pd.isna(supplier):
            supplier = "UNKNOWN"
        else:
            supplier = str(supplier).strip().lower().title()

        prospect_data.append({
            'Item_Description': raw_name,
            'Category': prospect_dept,
            'Supplier_Vendor': supplier,
            'Stock_On_Hand_Qty': soh,
            'Cost_Price_KES': cost,
            'Retail_Price_KES': price,
            'Qty_Sold_Last_30_Days': qty_sold_30_days,
            'Barcode': f"890{random.randint(100000000, 999999999)}"
        })
        
    out_df = pd.DataFrame(prospect_data)
    out_path = os.path.join(base_dir, "raw_prospect_data.csv")
    out_df.to_csv(out_path, index=False)
    print(f"Generated {len(out_df)} rows of messy prospect data at {out_path}")
    
    # Quick sanity check print
    print("\nSample Data:")
    print(out_df.head(3))

if __name__ == "__main__":
    generate_messy_prospect_data()
