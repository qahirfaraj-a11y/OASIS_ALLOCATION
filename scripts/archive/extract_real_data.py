import sqlite3
import pandas as pd
import os

DB_PATH = r'C:\Oasis\oasis\data\mock_pos_erp.db'
OUTPUT_DIR = r'C:\Oasis\inbound_drops\bootstrap'
SCORECARD_SOURCE = r'c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\Full_Product_Allocation_Scorecard_vSim.csv'

def extract_real_world_data():
    conn = sqlite3.connect(DB_PATH)
    tables = [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]
    
    # 1. Extract Sales (from BI_SALES_REPORT if available, else POS_SALES_DTL)
    print("Extracting Sales...")
    if 'BI_SALES_REPORT' in tables:
        # This table usually contains joined Item data
        df_sales = pd.read_sql("SELECT * FROM BI_SALES_REPORT LIMIT 100000", conn)
    elif 'POS_SALES_DTL' in tables:
        # Need to join with ITEM_MST to get Barcode/ItemName
        query = """
        SELECT d.BILL_DT as Date, i.BARCODE as Barcode, i.ITEM_NM as Item_Name, d.QTY as Qty, d.NET_AMT as Net_Amount
        FROM POS_SALES_DTL d
        JOIN ITEM_MST i ON d.ITEM_CD = i.ITEM_CD
        LIMIT 100000
        """
        df_sales = pd.read_sql(query, conn)
    
    df_sales.to_csv(os.path.join(OUTPUT_DIR, 'ORG001_sales.csv'), index=False)
    print(f"Extracted {len(df_sales)} sales records.")

    # 2. Extract GRN
    print("Extracting GRN...")
    if 'GRN_HDR' in tables:
        # Join with ITEM_MST or similar? Usually GRN_DTL has items.
        # Let's check for GRN_DTL.
        if 'GRN_DTL' in [t[0] for t in conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()]:
             query = """
             SELECT h.GRN_DT as Received_Date, i.BARCODE as Barcode, d.QTY as Received_Qty, h.GRN_NO as PO_Number
             FROM GRN_HDR h
             JOIN GRN_DTL d ON h.GRN_ID = d.GRN_ID
             JOIN ITEM_MST i ON d.ITEM_CD = i.ITEM_CD
             LIMIT 20000
             """
             df_grn = pd.read_sql(query, conn)
        else:
             # Just take the header if details missing
             df_grn = pd.read_sql("SELECT * FROM GRN_HDR LIMIT 10000", conn)
        
        df_grn.to_csv(os.path.join(OUTPUT_DIR, 'ORG001_grn.csv'), index=False)
        print(f"Extracted {len(df_grn)} GRN records.")

    # 3. Use the 40k Scorecard for Stock
    print("Deploying 40k SKU Scorecard...")
    if os.path.exists(SCORECARD_SOURCE):
        df_stock = pd.read_csv(SCORECARD_SOURCE)
        col_map = {
            'product_name': 'Item_Name',
            'current_stocks': 'SOH',
            'avg_daily_sales': 'ADS',
            'cost_price': 'Unit_Cost',
            'barcode': 'Barcode',
            'supplier_name': 'Supplier',
            'department': 'Department'
        }
        df_stock = df_stock.rename(columns=col_map)
        df_stock.to_csv(os.path.join(OUTPUT_DIR, 'ORG001_stock.csv'), index=False)
        df_stock.to_csv(os.path.join(OUTPUT_DIR, 'ORG001.csv'), index=False)
        print(f"Deployed {len(df_stock)} SKUs to bootstrap.")
    
    conn.close()

if __name__ == "__main__":
    extract_real_world_data()
