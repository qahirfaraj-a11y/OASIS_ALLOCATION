import sqlite3

def check_db():
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    print("Tables in oasis.db:", tables)
    for table_tuple in tables:
        table = table_tuple[0]
        cursor.execute(f"PRAGMA table_info({table});")
        info = cursor.fetchall()
        print(f"\nTable '{table}' columns:")
        for col in info:
            print(f"  {col[1]} ({col[2]})")
            
        # Search for Kapa in each table
        try:
            cursor.execute(f"SELECT * FROM {table} LIMIT 1;")
            row = cursor.fetchone()
            if row:
                print(f"  Sample row from '{table}': {row}")
        except Exception as e:
            print(f"  Error reading '{table}': {e}")
            
    conn.close()

check_db()
