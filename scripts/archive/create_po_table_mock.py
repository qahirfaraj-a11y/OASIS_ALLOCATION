import sqlite3
import os

db_path = r"C:\Oasis\oasis\data\mock_pos_erp.db"
if not os.path.exists(db_path):
    print(f"Error: {db_path} not found")
else:
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS INTEGRATION_PURCHASE_ORDERS (
        PO_ID           INTEGER PRIMARY KEY AUTOINCREMENT,
        ORG_CD          TEXT,
        ITM_CD          TEXT,
        PRODUCT_NAME    TEXT,
        SUPPLIER_CD     TEXT,
        QUANTITY        REAL DEFAULT 0,
        UNIT_COST       REAL DEFAULT 0,
        TOTAL_COST      REAL DEFAULT 0,
        REASONING       TEXT,
        STATUS          TEXT DEFAULT 'PENDING',
        CREATED_DT      TEXT,
        APPROVED_DT     TEXT,
        APPROVED_BY     TEXT,
        BATCH_ID        TEXT
    );
    """)
    conn.commit()
    conn.close()
    print("Table INTEGRATION_PURCHASE_ORDERS created successfully in mock_pos_erp.db")
