import sqlite3
import pprint

conn = sqlite3.connect('oasis/data/mock_pos_erp.db')
c = conn.cursor()
c.execute("SELECT ITM_CD, ITM_LONG_NAME, DEPARTMENT, SUPPLIER_CD, UOM_CD FROM ITEM_MST WHERE ITM_LONG_NAME LIKE '%FRYER%' OR ITM_LONG_NAME LIKE '%DISPENSER%' LIMIT 10")
pprint.pprint(c.fetchall())
