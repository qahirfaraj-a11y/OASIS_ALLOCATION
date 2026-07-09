import sqlite3
import pandas as pd
import os

db_path = r'C:\Oasis\oasis.db'
sales_csv = r'C:\Oasis\inbound_drops\bootstrap\ORG001_sales.csv'

def deep_kapa_analysis():
    conn = sqlite3.connect(db_path)
    
    # 1. Get Kapa items
    kapa_items = pd.read_sql_query("SELECT ITM_CD, ITM_LONG_NAME FROM ITEM_MST WHERE SUPPLIER_CD = 'SUP_KAPA'", conn)
    print(f"Kapa Items in DB: {len(kapa_items)}")
    print(kapa_items)
    
    itm_cds = set(kapa_items['ITM_CD'].tolist())
    
    # 2. Analyze Sales CSV
    print(f"\n--- Analyzing {sales_csv} ---")
    chunk_size = 100000
    total_kapa_sales = 0
    total_kapa_qty = 0
    kapa_sales_data = []
    
    try:
        # Check first row to see column names
        first_chunk = pd.read_csv(sales_csv, nrows=1)
        print(f"Sales CSV Columns: {first_chunk.columns.tolist()}")
        itm_col = 'ITM_CD' if 'ITM_CD' in first_chunk.columns else 'ITEM_CD'
        val_col = 'TOTAL_VALUE' if 'TOTAL_VALUE' in first_chunk.columns else 'NET_AMT'
        qty_col = 'QTY' if 'QTY' in first_chunk.columns else 'QUANTITY'
        
        for chunk in pd.read_csv(sales_csv, chunksize=chunk_size):
            # Also search for names if ITM_CD doesn't match
            mask = chunk[itm_col].isin(itm_cds) | chunk.astype(str).apply(lambda x: x.str.contains('KAPA|KASUKU|TOSS|NURU', case=False)).any(axis=1)
            kapa_chunk = chunk[mask]
            if not kapa_chunk.empty:
                total_kapa_sales += kapa_chunk[val_col].sum()
                total_kapa_qty += kapa_chunk[qty_col].sum()
                kapa_sales_data.append(kapa_chunk)
                
        if kapa_sales_data:
            df_kapa = pd.concat(kapa_sales_data)
            print(f"Total Kapa Sales: KES {total_kapa_sales:,.2f}")
            print(f"Total Kapa Qty: {total_kapa_qty:,.2f}")
            print("\nTop Selling Kapa Products:")
            print(df_kapa.groupby(itm_col)[val_col].sum().sort_values(ascending=False).head(10))
        else:
            print("No Kapa sales found in CSV.")
            
    except Exception as e:
        print(f"Error analyzing sales CSV: {e}")
        
    conn.close()

if __name__ == "__main__":
    deep_kapa_analysis()
