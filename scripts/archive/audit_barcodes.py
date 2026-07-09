import sqlite3
import pandas as pd
import json

db_path = "oasis/data/mock_pos_erp.db"
barcode_map_path = "oasis/data/product_barcode_map.json"

try:
    conn = sqlite3.connect(db_path)
    tables = pd.read_sql("SELECT name FROM sqlite_master WHERE type='table';", conn)['name'].tolist()
    print("Tables:", tables)
    for t in tables:
        cols = pd.read_sql(f"PRAGMA table_info({t});", conn)['name'].tolist()
        print(f"Table '{t}' columns: {cols}")
except Exception as e:
    print("DB error:", e)

try:
    with open(barcode_map_path, 'r') as f:
        bmap = json.load(f)
    print("Sample keys:", {k:bmap[k] for k in list(bmap.keys())[:2]})
except Exception as e:
    print("Barcode error:", e)
