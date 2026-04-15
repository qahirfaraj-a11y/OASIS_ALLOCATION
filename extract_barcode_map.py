import sqlite3
import json
import os

db_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp.db'
output_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\product_barcode_map.json'

def extract_mapping():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Try to find the correct table and columns based on SchemaMapper
    # ITM_LONG_NAME -> product_name
    # SCAN_ITM_CD -> barcode
    try:
        cursor.execute("SELECT ITM_LONG_NAME, SCAN_ITM_CD FROM ITEM_MST")
        rows = cursor.fetchall()
        mapping = {row[0]: row[1] for row in rows if row[0] and row[1]}
        
        with open(output_path, 'w') as f:
            json.dump(mapping, f, indent=4)
        print(f"Extracted {len(mapping)} mappings to {output_path}")
    except Exception as e:
        print(f"Error: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    extract_mapping()
