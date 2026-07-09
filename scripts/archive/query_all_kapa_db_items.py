import sqlite3

def check():
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    print("=== All items for supplier SUP_KAPA in ITEM_MST ===")
    cursor.execute("""
        SELECT i.ITM_CD, i.ITM_LONG_NAME, i.SCAN_ITM_CD, i.DEPARTMENT, cp.BCP_CP, sp.BSP_SP
        FROM ITEM_MST i
        LEFT JOIN BASIC_CP_MST cp ON i.ITM_CD = cp.BCP_ITEM_CD
        LEFT JOIN BASIC_SP_MST sp ON i.ITM_CD = sp.BSP_ITEM_CD
        WHERE i.SUPPLIER_CD = 'SUP_KAPA';
    """)
    rows = cursor.fetchall()
    print(f"Total SUP_KAPA items in DB: {len(rows)}")
    for r in rows[:20]:
        print(r)
        
    conn.close()

check()
