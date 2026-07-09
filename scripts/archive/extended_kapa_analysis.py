import sqlite3
import pandas as pd
import os

db_path = r'C:\Oasis\oasis.db'
sales_csv = r'C:\Oasis\inbound_drops\bootstrap\ORG001_sales.csv'

def extended_kapa_analysis():
    conn = sqlite3.connect(db_path)
    
    # 1. Broad search for Kapa related items in ITEM_MST
    search_terms = ['KAPA', 'KASUKU', 'COWBOY', 'FRESH FRI', 'SALIT', 'NURU', 'JAMAA', 'TOSS', 'TENA', 'BODYLINE', 'PRESTIGE']
    query_parts = [f"ITM_LONG_NAME LIKE '%{term}%'" for term in search_terms]
    query = f"SELECT ITM_CD, ITM_LONG_NAME, SUPPLIER_CD FROM ITEM_MST WHERE {' OR '.join(query_parts)} OR SUPPLIER_CD = 'SUP_KAPA'"
    
    kapa_items = pd.read_sql_query(query, conn)
    print(f"Kapa-related Items found in DB: {len(kapa_items)}")
    print(kapa_items)
    
    itm_cds = set(kapa_items['ITM_CD'].tolist())
    
    # 2. Analyze Sales CSV with these codes
    print(f"\n--- Analyzing {sales_csv} ---")
    chunk_size = 100000
    total_kapa_sales = 0
    kapa_sales_data = []
    
    try:
        first_chunk = pd.read_csv(sales_csv, nrows=1)
        itm_col = 'ITM_CD' if 'ITM_CD' in first_chunk.columns else 'ITEM_CD'
        val_col = 'TAX_INCL' if 'TAX_INCL' in first_chunk.columns else 'TOTAL_VALUE'
        qty_col = 'QUANTITY' if 'QUANTITY' in first_chunk.columns else 'QTY'
        
        for chunk in pd.read_csv(sales_csv, chunksize=chunk_size):
            kapa_chunk = chunk[chunk[itm_col].isin(itm_cds)]
            if not kapa_chunk.empty:
                total_kapa_sales += kapa_chunk[val_col].sum()
                kapa_sales_data.append(kapa_chunk)
                
        if kapa_sales_data:
            df_kapa = pd.concat(kapa_sales_data)
            print(f"Total Kapa Sales (from CSV): KES {total_kapa_sales:,.2f}")
            
            # Aggregate by product
            summary = df_kapa.groupby(itm_col).agg({
                qty_col: 'sum',
                val_col: 'sum'
            }).reset_index()
            
            # Join with names
            summary = summary.merge(kapa_items, left_on=itm_col, right_on='ITM_CD', how='left')
            summary = summary.sort_values(val_col, ascending=False)
            print("\nTop Kapa Products by Revenue:")
            print(summary[['ITM_LONG_NAME', qty_col, val_col]])
        else:
            print("No Kapa sales found in CSV matching these codes.")
            
    except Exception as e:
        print(f"Error analyzing sales CSV: {e}")
        
    conn.close()

if __name__ == "__main__":
    extended_kapa_analysis()
