import sqlite3

def check():
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=== Sales for ITM_KAPAKASUKU2KG ===")
    cursor.execute("SELECT * FROM POS_SALES_DTL WHERE ITM_CD = 'ITM_KAPAKASUKU2KG';")
    print(cursor.fetchall())
    
    print("=== Sales for other items with name containing KAPA or similar ===")
    cursor.execute("SELECT DISTINCT ITM_CD, ITEM_NAME FROM POS_SALES_DTL WHERE ITEM_NAME LIKE '%KAPA%' OR ITEM_NAME LIKE '%KASUKU%' OR ITEM_NAME LIKE '%ATILLA%';")
    print(cursor.fetchall())
    
    conn.close()

check()
