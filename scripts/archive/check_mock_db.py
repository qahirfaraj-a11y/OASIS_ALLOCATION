import sqlite3
import os

db_path = r"C:\Oasis\oasis\data\mock_pos_erp.db"
if not os.path.exists(db_path):
    print(f"File not found: {db_path}")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
    tables = cursor.fetchall()
    conn.close()
    print("Tables in mock_pos_erp.db:")
    for t in tables:
        print(t[0])
