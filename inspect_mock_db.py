import sqlite3
import os
import pandas as pd
from datetime import datetime

DB_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp_lite.db"
# Also check the main db
BIG_DB_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp.db"

def inspect_db(path):
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return

    print(f"\n--- Inspecting {os.path.basename(path)} ---")
    conn = sqlite3.connect(path)
    
    # Check Orgs
    try:
        orgs = pd.read_sql("SELECT ORG_CD, ORG_NAME FROM ORGANIZATION_MST", conn)
        print("\nOrganization Codes:")
        print(orgs)
    except Exception as e:
        print(f"Error reading organizations: {e}")

    # Check Sales Date Range
    try:
        sales_dates = pd.read_sql("SELECT MIN(BILL_DT), MAX(BILL_DT), COUNT(*) FROM POS_SALES_DTL", conn)
        print("\nSales Data Range:")
        print(sales_dates)
    except Exception as e:
        print(f"Error reading sales: {e}")

    # Check Stock Data
    try:
        stock_count = pd.read_sql("SELECT COUNT(*) FROM STOCK_MASTER", conn)
        print("\nStock Master Count:")
        print(stock_count)
    except Exception as e:
        print(f"Error reading stock: {e}")
        
    conn.close()

if __name__ == "__main__":
    inspect_db(DB_PATH)
    inspect_db(BIG_DB_PATH)
    print(f"\nCurrent Time: {datetime.now().strftime('%Y-%m-%d')}")
