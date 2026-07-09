import pandas as pd
import sqlite3
import os

def check():
    kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_excel_path, header=2)
    
    detail_path = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    df_detail = pd.read_excel(detail_path)
    kapa_detail = df_detail[df_detail['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)].copy()
    kapa_detail['Item_Name_upper'] = kapa_detail['Item Name'].astype(str).str.strip().str.upper()
    kapa_detail['calculated_cp'] = kapa_detail['Net Amt'] / kapa_detail['GRN Qty']
    cp_map = kapa_detail.groupby('Item_Name_upper')['calculated_cp'].mean().to_dict()
    
    db_path = r"C:\Oasis\oasis.db"
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    missing = []
    for idx, row in df_kapa.iterrows():
        desc = row['DESCRIPTION']
        if pd.isna(desc):
            continue
        desc_upper = str(desc).strip().upper()
        if desc_upper not in cp_map:
            it_cd = row['ITEM CODE']
            cursor.execute("SELECT BCP_CP FROM BASIC_CP_MST WHERE BCP_ITEM_CD = ?;", (it_cd,))
            cp_rows = cursor.fetchall()
            
            missing.append({
                'DESCRIPTION': desc,
                'BARCODE': row['BARCODE'],
                'ITEM CODE': it_cd,
                'SP': row['SP'],
                'BASIC_CP_MST': cp_rows
            })
            
    print(f"Missing {len(missing)} CPs:")
    df_missing = pd.DataFrame(missing)
    print(df_missing.to_string())
    conn.close()

check()
