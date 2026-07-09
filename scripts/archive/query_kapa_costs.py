import pandas as pd
import sqlite3
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    print("kapa.xlsx row count:", len(df_kapa))
    
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    # Let's inspect some item codes and barcodes from kapa.xlsx
    samples = df_kapa[['DESCRIPTION', 'BARCODE', 'ITEM CODE', 'SP']].dropna(subset=['BARCODE']).head(10)
    for idx, row in samples.iterrows():
        desc = row['DESCRIPTION']
        bc = str(row['BARCODE']).split('.')[0] # remove float decimal if any
        it_cd = row['ITEM CODE']
        sp_excel = row['SP']
        
        # Check in ITEM_MST
        cursor.execute("SELECT ITM_CD, SCAN_ITM_CD, CATEGORY, DEPARTMENT FROM ITEM_MST WHERE SCAN_ITM_CD = ? OR ITM_CD = ?;", (bc, it_cd))
        item_rows = cursor.fetchall()
        
        # Check in BASIC_CP_MST
        cursor.execute("SELECT BCP_CP FROM BASIC_CP_MST WHERE BCP_ITEM_CD = ?;", (it_cd,))
        cp_rows = cursor.fetchall()
        
        # Check in BASIC_SP_MST
        cursor.execute("SELECT BSP_SP FROM BASIC_SP_MST WHERE BSP_ITEM_CD = ?;", (it_cd,))
        sp_rows = cursor.fetchall()
        
        print(f"\nSKU: {desc}")
        print(f"  Barcode: {bc}, ItemCode: {it_cd}, SP (Excel): {sp_excel}")
        print(f"  ITEM_MST: {item_rows}")
        print(f"  BASIC_CP_MST (Cost Price): {cp_rows}")
        print(f"  BASIC_SP_MST (Selling Price): {sp_rows}")
        
    conn.close()

check()
