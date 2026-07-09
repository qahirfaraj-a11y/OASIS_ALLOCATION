import sqlite3
import pandas as pd
import json

db_path = "oasis/data/mock_pos_erp.db"
nodes_path = "neutral_network_export/nodes.csv"
barcode_map_path = "oasis/data/product_barcode_map.json"

print("Starting System Data Synchronization...")

# 1. Load Data
df_nodes = pd.read_csv(nodes_path)
df_nodes = df_nodes[df_nodes['type'] == 'SKU'].copy()

with open(barcode_map_path, 'r') as f:
    bmap = json.load(f)

# Clean nodes data
df_nodes['department'] = df_nodes['department'].str.replace(r'^\[\[(.*)\]\]$', r'\1', regex=True).str.strip()
df_nodes['supplier'] = df_nodes['supplier'].str.replace(r'^\[\[(.*)\]\]$', r'\1', regex=True).str.strip()

# Create uppercase ID for matching
df_nodes['name_upper'] = df_nodes['id'].str.upper().str.strip()

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# 2. Synchronize Suppliers
print("Synchronizing Suppliers...")
unique_suppliers = df_nodes['supplier'].unique()
cursor.execute("SELECT SUPPLIER_NAME, SUPPLIER_CD FROM SUPPLIER_MST")
existing_suppliers = {row[0].strip().upper(): row[1] for row in cursor.fetchall() if row[0]}

new_suppliers = []
for supp in unique_suppliers:
    supp_upper = str(supp).strip().upper()
    if supp_upper and supp_upper != "UNKNOWN" and supp_upper not in existing_suppliers:
        # Generate new supplier code
        new_cd = f"SUPP_{len(existing_suppliers) + len(new_suppliers) + 1000}"
        new_suppliers.append((new_cd, str(supp).strip()))
        existing_suppliers[supp_upper] = new_cd

if new_suppliers:
    cursor.executemany("INSERT INTO SUPPLIER_MST (SUPPLIER_CD, SUPPLIER_NAME, ACTIVE_FLAG) VALUES (?, ?, 'Y')", new_suppliers)
    print(f"Inserted {len(new_suppliers)} new suppliers.")

# 3. Synchronize Items
print("Synchronizing ITEM_MST...")
# Read existing items
cursor.execute("SELECT ITM_LONG_NAME, ITM_CD FROM ITEM_MST")
db_items = {row[0].strip().upper(): row[1] for row in cursor.fetchall() if row[0]}

updates = []
for _, row in df_nodes.iterrows():
    name_up = row['name_upper']
    if name_up in db_items:
        itm_cd = db_items[name_up]
        dept = row['department']
        supp_name_up = str(row['supplier']).strip().upper()
        
        # Get Supplier Code
        supp_cd = existing_suppliers.get(supp_name_up, "UNKNOWN_SUPP")
        
        # Get Barcode
        barcode = bmap.get(row['id'], None)
        if not barcode: # Fallback to case-insensitive match
            for k, v in bmap.items():
                if k.strip().upper() == name_up:
                    barcode = v
                    break
        if not barcode:
            barcode = "MISSING_BC"
            
        updates.append((dept, supp_cd, barcode, itm_cd))

if updates:
    cursor.executemany("UPDATE ITEM_MST SET DEPARTMENT = ?, SUPPLIER_CD = ?, SCAN_ITM_CD = ? WHERE ITM_CD = ?", updates)
    print(f"Updated {len(updates)} items in DB.")

conn.commit()
conn.close()

print("Synchronization Complete!")
