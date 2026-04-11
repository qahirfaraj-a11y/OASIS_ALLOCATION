import sqlite3
import os
import pandas as pd

BIG_DB = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp.db"
LITE_DB = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp_lite.db"

def populate_lite():
    if not os.path.exists(BIG_DB):
        print("Big DB not found.")
        return

    print("Connecting to databases...")
    conn_big = sqlite3.connect(BIG_DB)
    conn_lite = sqlite3.connect(LITE_DB)
    
    # We want to transfer a subset of sales records
    # Let's take the last 30 days of sales for a few orgs, or just a random sample of 50k rows
    
    print("Clearing lite sales tables...")
    conn_lite.execute("DELETE FROM POS_SALES_DTL")
    conn_lite.execute("DELETE FROM POS_SALES_HDR")
    conn_lite.commit()
    
    print("Fetching sample sales from big DB...")
    # Get 50,000 recent sales records
    query_dtl = "SELECT * FROM POS_SALES_DTL ORDER BY BILL_DT DESC LIMIT 50000"
    df_dtl = pd.read_sql(query_dtl, conn_big)
    
    # Get the corresponding headers
    bill_nos = tuple(df_dtl['BILL_NO'].unique())
    query_hdr = f"SELECT * FROM POS_SALES_HDR WHERE BILL_NO IN {bill_nos}"
    df_hdr = pd.read_sql(query_hdr, conn_big)
    
    print(f"Writing {len(df_dtl)} dtl rows and {len(df_hdr)} hdr rows to lite DB...")
    df_dtl.to_sql("POS_SALES_DTL", conn_lite, if_exists="append", index=False)
    df_hdr.to_sql("POS_SALES_HDR", conn_lite, if_exists="append", index=False)
    
    conn_lite.commit()
    print("Done!")
    
    conn_big.close()
    conn_lite.close()

if __name__ == "__main__":
    populate_lite()
