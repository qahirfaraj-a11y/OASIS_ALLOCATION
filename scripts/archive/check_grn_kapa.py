import sqlite3

def check():
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=== Distinct supplier names in GRN_DTL containing KAPA ===")
    cursor.execute("SELECT DISTINCT SUPPLIER_CD, SUPPLIER_NAME FROM GRN_DTL WHERE SUPPLIER_NAME LIKE '%KAPA%';")
    print(cursor.fetchall())
    
    print("=== First 5 GRN rows containing KAPA ===")
    cursor.execute("SELECT * FROM GRN_DTL WHERE SUPPLIER_NAME LIKE '%KAPA%' LIMIT 5;")
    for row in cursor.fetchall():
        print(row)
        
    print("=== Unique items under Kapa in GRN_DTL ===")
    cursor.execute("SELECT DISTINCT ITM_CD, ITEM_NAME, BARCODE FROM GRN_DTL WHERE SUPPLIER_NAME LIKE '%KAPA%' LIMIT 10;")
    for row in cursor.fetchall():
        print(row)
        
    conn.close()

check()
