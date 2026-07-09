import sqlite3
import pandas as pd
import json

db_path = "oasis/data/mock_pos_erp.db"
nodes_path = "neutral_network_export/nodes.csv"
barcode_map_path = "oasis/data/product_barcode_map.json"

conn = sqlite3.connect(db_path)
df_items = pd.read_sql("SELECT ITM_CD, ITM_LONG_NAME, SCAN_ITM_CD, DEPARTMENT, SUPPLIER_CD FROM ITEM_MST", conn)
df_suppliers = pd.read_sql("SELECT SUPPLIER_CD, SUPPLIER_NAME FROM SUPPLIER_MST", conn)

df_items = df_items.merge(df_suppliers, on='SUPPLIER_CD', how='left')

df_nodes = pd.read_csv(nodes_path)
df_nodes = df_nodes[df_nodes['type'] == 'SKU']
df_nodes['department'] = df_nodes['department'].str.replace(r'^\[\[(.*)\]\]$', r'\1', regex=True).str.strip()
df_nodes['supplier'] = df_nodes['supplier'].str.replace(r'^\[\[(.*)\]\]$', r'\1', regex=True).str.strip()

with open(barcode_map_path, 'r') as f:
    bmap = json.load(f)

print(f"Total Nodes SKUs: {len(df_nodes)}")
print(f"Total DB Items: {len(df_items)}")

df_nodes['name_upper'] = df_nodes['id'].str.upper().str.strip()
df_items['name_upper'] = df_items['ITM_LONG_NAME'].str.upper().str.strip()

merged = pd.merge(df_nodes, df_items, on='name_upper', how='left')
missing_in_db = merged[merged['ITM_CD'].isna()]

print(f"Nodes missing in DB: {len(missing_in_db)}")

if len(merged.dropna(subset=['ITM_CD'])) > 0:
    mismatched_dept = merged[(merged['ITM_CD'].notna()) & (merged['department'].str.upper() != merged['DEPARTMENT'].str.upper())]
    print(f"Mismatched Departments: {len(mismatched_dept)}")
    
    mismatched_supplier = merged[(merged['ITM_CD'].notna()) & (merged['supplier'].str.upper() != merged['SUPPLIER_NAME'].str.upper())]
    print(f"Mismatched Suppliers: {len(mismatched_supplier)}")
