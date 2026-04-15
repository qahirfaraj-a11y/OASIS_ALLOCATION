import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta
import os

def generate_operations_logs(days=90, num_items=500):
    base_dir = os.path.dirname(os.path.abspath(__file__))
    scorecard_path = os.path.join(base_dir, "Full_Product_Allocation_Scorecard_v3.csv")
    
    if os.path.exists(scorecard_path):
        df_base = pd.read_csv(scorecard_path).sample(num_items, random_state=42)
    else:
        # Fallback if no scorecard
        print("Scorecard not found, generating dummy items.")
        df_base = pd.DataFrame({
            'Product': [f"Item_{i}" for i in range(num_items)],
            'Supplier Name': [f"Supplier_{i%10}" for i in range(num_items)],
            'Unit_Price': [random.uniform(10, 1000) for _ in range(num_items)]
        })
        
    end_date = datetime.now()
    start_date = end_date - timedelta(days=days)
    
    # 1. Create Catalog & Profiles
    catalog = []
    supplier_col = 'Supplier Name' if 'Supplier Name' in df_base.columns else 'Supplier_Vendor'
    if supplier_col in df_base.columns:
        suppliers = list(df_base[supplier_col].dropna().unique())
    else:
        suppliers = []
    if not suppliers: suppliers = ["General Mkt", "FastMovers Ltd", "SlowSupplies"]
    
    # Define supplier criminal profiles
    supplier_profiles = {}
    for i, supp in enumerate(suppliers):
        if i % 4 == 0:
            supplier_profiles[supp] = {'reliability': 0.6, 'lead_variance': 5} # The criminals (LATA targets)
        elif i % 5 == 0:
            supplier_profiles[supp] = {'reliability': 0.75, 'lead_variance': 2}
        else:
            supplier_profiles[supp] = {'reliability': 0.98, 'lead_variance': 0} # Reliable

    for _, row in df_base.iterrows():
        base_name = str(row.get('Product', 'Unknown Item')).upper()
        supp = row.get('Supplier Name') if not pd.isna(row.get('Supplier Name')) else random.choice(suppliers)
        cost = float(row.get('Unit_Price', random.uniform(50, 5000)))
        
        # Velocity profile
        r = random.random()
        if r < 0.2:
            velocity = random.uniform(0, 0.2) # Dead stock
            stock = random.randint(50, 200)
        elif r < 0.5:
            velocity = random.uniform(5, 20) # Fast mover
            stock = random.randint(0, 10) # Prone to stockouts
        else:
            velocity = random.uniform(0.5, 3) # Normal
            stock = random.randint(20, 50)
            
        catalog.append({
            'barcode': f"890{random.randint(100000000, 999999999)}",
            'item_name': base_name,
            'supplier': supp,
            'cost': cost,
            'price': round(cost * 1.3, 2),
            'base_velocity': velocity,
            'current_stock': stock
        })
        
    # Generate Logs
    pos_logs = []
    grn_logs = []
    grts_logs = []
    transfer_logs = []
    
    # Simulate day by day
    for d in range(days):
        current_date = start_date + timedelta(days=d)
        date_str = current_date.strftime("%Y-%m-%d")
        
        for item in catalog:
            # --- POS SALES ---
            # Add Poisson variance
            daily_sales = np.random.poisson(item['base_velocity'])
            if daily_sales > 0:
                # Cap sales if they run out of synthetic stock (for DHARAM logic later)
                actual_sales = min(daily_sales, item['current_stock']) if item['current_stock'] > 0 else 0
                
                if actual_sales > 0:
                    item['current_stock'] -= actual_sales
                    # Break into random transaction sizes
                    tx_left = actual_sales
                    tx_id_base = f"TX-{current_date.strftime('%Y%m%d')}"
                    tx_num = 1
                    while tx_left > 0:
                        qty = random.randint(1, min(3, tx_left))
                        tx_left -= qty
                        pos_logs.append({
                            'Date': date_str,
                            'Transaction_ID': f"{tx_id_base}-{random.randint(1000, 9999)}",
                            'Barcode': item['barcode'],
                            'Item_Name': item['item_name'],
                            'Qty_Sold': qty,
                            'Unit_Price_KES': item['price'],
                            'Unit_Cost_KES': item['cost']
                        })
                
            # --- GRN / PURCHASING ---
            # Trigger order if stock is low
            if item['current_stock'] < item['base_velocity'] * 3 and random.random() < 0.3:
                order_qty = int(item['base_velocity'] * 14) + 10 # Order 2 weeks supply
                supp_prof = supplier_profiles.get(item['supplier'], {'reliability': 1.0, 'lead_variance': 0})
                
                # Supplier fulfills based on reliability
                received_qty = int(order_qty * max(0.2, min(1.0, random.gauss(supp_prof['reliability'], 0.1))))
                
                # Lead time variance
                days_late = int(abs(random.gauss(0, supp_prof['lead_variance'])))
                received_date = current_date + timedelta(days=days_late)
                
                grn_logs.append({
                    'Order_Date': date_str,
                    'Received_Date': received_date.strftime("%Y-%m-%d"),
                    'PO_Number': f"PO-{random.randint(10000, 99999)}",
                    'Supplier_Name': item['supplier'],
                    'Item_Name': item['item_name'],
                    'Ordered_Qty': order_qty,
                    'Received_Qty': received_qty
                })
                # Add to synthetic stock (simulating it arrived today for simplicity of simulation state, though logged as future)
                item['current_stock'] += received_qty
                
            # --- TRANSFERS ---
            # If stock is highly imbalanced (we simulate this randomly for high stock items)
            if item['current_stock'] > 100 and random.random() < 0.05:
                transfer_qty = random.randint(10, 50)
                item['current_stock'] -= transfer_qty
                transfer_logs.append({
                    'Date': date_str,
                    'From_Branch': 'Main Store',
                    'To_Branch': random.choice(['Branch North', 'Branch South', 'City Center']),
                    'Item_Name': item['item_name'],
                    'Qty_Transferred': transfer_qty
                })
                
            # --- GRTS / SHRINKAGE ---
            if item['current_stock'] > 0 and random.random() < 0.01:
                shrink_qty = random.randint(1, 3)
                item['current_stock'] -= shrink_qty
                grts_logs.append({
                    'Date': date_str,
                    'Item_Name': item['item_name'],
                    'Supplier': item['supplier'],
                    'Qty_Adjusted': shrink_qty,
                    'Reason': random.choice(['Expired', 'Damaged', 'Theft', 'System Sync'])
                })

    # Save to CSV
    pd.DataFrame(pos_logs).to_csv(os.path.join(base_dir, 'prospect_pos_sales.csv'), index=False)
    pd.DataFrame(grn_logs).to_csv(os.path.join(base_dir, 'prospect_inbound_grn.csv'), index=False)
    pd.DataFrame(transfer_logs).to_csv(os.path.join(base_dir, 'prospect_transfers.csv'), index=False)
    pd.DataFrame(grts_logs).to_csv(os.path.join(base_dir, 'prospect_shrinkage_grts.csv'), index=False)
    
    # Save a final SOH snapshot
    soh_data = [{'Item_Name': i['item_name'], 'Barcode': i['barcode'], 'Stock_On_Hand': i['current_stock'], 'Unit_Cost': i['cost']} for i in catalog]
    pd.DataFrame(soh_data).to_csv(os.path.join(base_dir, 'prospect_inventory_snapshot.csv'), index=False)
    
    print(f"Generated Phase 2 Operations Audit Logs:")
    print(f"- POS Transactions: {len(pos_logs)}")
    print(f"- GRN Inbounds: {len(grn_logs)}")
    print(f"- Transfers: {len(transfer_logs)}")
    print(f"- Adjustments/Shrink: {len(grts_logs)}")

if __name__ == "__main__":
    generate_operations_logs()
