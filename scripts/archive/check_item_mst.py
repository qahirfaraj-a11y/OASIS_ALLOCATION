import sqlite3

def check():
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("--- Searching ITEM_MST for KAPA or similar ---")
    cursor.execute("SELECT * FROM ITEM_MST WHERE ITM_LONG_NAME LIKE '%KAPA%' OR ITM_LONG_NAME LIKE '%ATILLA%' OR ITM_LONG_NAME LIKE '%MANDASHI%' LIMIT 10;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    print("\n--- Searching ITEM_MST for first 5 rows ---")
    cursor.execute("SELECT * FROM ITEM_MST LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    print("\n--- Searching BASIC_CP_MST for first 5 rows ---")
    cursor.execute("SELECT * FROM BASIC_CP_MST LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    print("\n--- Searching BASIC_SP_MST for first 5 rows ---")
    cursor.execute("SELECT * FROM BASIC_SP_MST LIMIT 5;")
    rows = cursor.fetchall()
    for row in rows:
        print(row)
        
    conn.close()

check()
