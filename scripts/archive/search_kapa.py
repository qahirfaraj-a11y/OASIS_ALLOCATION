import sqlite3
import pandas as pd

db_path = r'C:\Oasis\oasis.db'

def search_kapa():
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = [t[0] for t in cursor.fetchall()]
    
    for table in tables:
        try:
            # Check column names first
            cursor.execute(f"PRAGMA table_info({table})")
            columns = [c[1] for c in cursor.fetchall()]
            
            # Search each column for 'Kapa'
            for col in columns:
                query = f"SELECT * FROM {table} WHERE {col} LIKE '%Kapa%' LIMIT 10"
                df = pd.read_sql_query(query, conn)
                if not df.empty:
                    print(f"\nMatch in Table: {table}, Column: {col}")
                    print(df)
        except Exception as e:
            pass
            
    conn.close()

if __name__ == "__main__":
    search_kapa()
