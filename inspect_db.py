import sqlite3
import os

def inspect():
    db_paths = ['C:/Oasis/oasis.db', './oasis.db', 'C:/Users/iLink/.gemini/antigravity/scratch/oasis.db']
    for db_path in db_paths:
        if os.path.exists(db_path):
            print(f"=== DB FOUND: {db_path} ===")
            conn = sqlite3.connect(db_path)
            cursor = conn.cursor()
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [t[0] for t in cursor.fetchall()]
            print('Tables:', tables)
            for table in tables:
                cursor.execute(f"PRAGMA table_info({table})")
                cols = [c[1] for c in cursor.fetchall()]
                print(f'  Table {table} columns:', cols)
                # If there are interesting tables, sample them
                if any(x in table.lower() for x in ['transport', 'logistics', 'delivery', 'vendor', 'supplier', 'po', 'grn', 'price', 'cost']):
                    cursor.execute(f"SELECT * FROM {table} LIMIT 2")
                    print(f"    Sample {table}:", cursor.fetchall())
        else:
            print(f"DB NOT FOUND: {db_path}")

if __name__ == "__main__":
    inspect()
