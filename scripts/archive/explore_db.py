import sqlite3
import pandas as pd

db_path = r'C:\Oasis\oasis.db'

def explore_db():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # List tables
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    print(f"Tables: {tables}")
    
    for table in tables:
        print(f"\n--- Table: {table} ---")
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table} LIMIT 5", conn)
            print(df.head())
        except Exception as e:
            print(f"Error reading {table}: {e}")
            
    conn.close()

if __name__ == "__main__":
    explore_db()
