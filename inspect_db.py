import sqlite3
import os

db_path = 'oasis/data/mock_pos_erp.db'
conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# Get table sizes theoretically by looking at row counts
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
for table in tables:
    table_name = table[0]
    cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
    count = cursor.fetchone()[0]
    print(f"Table '{table_name}': {count} rows")

conn.close()
