import sqlite3
import pandas as pd

db_path = r'C:\Oasis\oasis.db'

def list_tables():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Tables: {tables}")
    
    for table in tables:
        cursor.execute(f"PRAGMA table_info({table})")
        cols = [c[1] for c in cursor.fetchall()]
        print(f"Table: {table}, Columns: {cols}")
    conn.close()

if __name__ == "__main__":
    list_tables()
