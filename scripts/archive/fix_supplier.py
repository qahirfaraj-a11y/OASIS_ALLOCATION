import sqlite3
db_path = "oasis/data/mock_pos_erp.db"
conn = sqlite3.connect(db_path)
conn.execute("INSERT OR REPLACE INTO SUPPLIER_MST (SUPPLIER_CD, SUPPLIER_NAME, ACTIVE_FLAG) VALUES ('UNKNOWN_SUPP', 'Unknown', 'Y')")
conn.commit()
