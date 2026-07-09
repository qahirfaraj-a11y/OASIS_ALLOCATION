import sqlite3
import pandas as pd

db_path = r'C:\Oasis\oasis.db'

def kapa_assessment():
    conn = sqlite3.connect(db_path)
    
    # 1. Supplier Master Data
    sup_mst = pd.read_sql_query("SELECT * FROM SUPPLIER_MST WHERE SUPPLIER_CD = 'SUP_KAPA'", conn)
    print("--- Supplier Master Data ---")
    print(sup_mst)
    
    # 2. SKUs under Kapa
    skus = pd.read_sql_query("SELECT ITM_CD, ITM_LONG_NAME, CATEGORY FROM ITEM_MST WHERE SUPPLIER_CD = 'SUP_KAPA'", conn)
    print("\n--- SKUs under Kapa ---")
    print(skus)
    
    # 3. Recent Deliveries (GRN)
    grn_query = """
    SELECT * 
    FROM GRN_DTL 
    WHERE SUPPLIER_CD = 'SUP_KAPA'
    ORDER BY RECEIVED_DATE DESC
    """
    try:
        grns = pd.read_sql_query(grn_query, conn)
        print("\n--- Recent Deliveries (GRN) ---")
        print(grns.head(10))
        
        # Calculate delivery accuracy
        if not grns.empty:
            grns['accuracy'] = grns['RECEIVED_QTY'] / grns['ORDERED_QTY']
            print("\n--- Delivery Accuracy (Avg) ---")
            print(grns['accuracy'].mean())
    except Exception as e:
        print(f"\nCould not fetch GRN data: {e}")

    # 4. Sales Performance
    sales_query = """
    SELECT s.ITM_LONG_NAME, SUM(p.QTY) as TOTAL_QTY, SUM(p.TOTAL_VALUE) as TOTAL_REVENUE
    FROM POS_SALES_DTL p
    JOIN ITEM_MST s ON p.ITM_CD = s.ITM_CD
    WHERE s.SUPPLIER_CD = 'SUP_KAPA'
    GROUP BY s.ITM_LONG_NAME
    ORDER BY TOTAL_REVENUE DESC
    """
    try:
        sales = pd.read_sql_query(sales_query, conn)
        print("\n--- Sales Performance ---")
        print(sales)
    except Exception as e:
        print(f"\nCould not fetch Sales data: {e}")

    # 5. Pricing/Margins (Sample)
    margin_query = """
    SELECT s.ITM_LONG_NAME, cp.BCP_CP as COST, sp.BSP_SP as PRICE, (sp.BSP_SP - cp.BCP_CP) as MARGIN,
           ((sp.BSP_SP - cp.BCP_CP) / sp.BSP_SP) * 100 as MARGIN_PCT
    FROM ITEM_MST s
    JOIN BASIC_CP_MST cp ON s.ITM_CD = cp.BCP_ITEM_CD
    JOIN BASIC_SP_MST sp ON s.ITM_CD = sp.BSP_ITEM_CD
    WHERE s.SUPPLIER_CD = 'SUP_KAPA'
    """
    try:
        margins = pd.read_sql_query(margin_query, conn)
        print("\n--- Pricing and Margins ---")
        print(margins)
    except Exception as e:
        print(f"\nCould not fetch Margin data: {e}")

    conn.close()

if __name__ == "__main__":
    kapa_assessment()
