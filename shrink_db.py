import sqlite3
import shutil
import os

src = 'oasis/data/mock_pos_erp.db'
dst = 'oasis/data/mock_pos_erp_lite.db'

print("Copying DB...")
if os.path.exists(dst):
    os.remove(dst)
shutil.copy2(src, dst)

print("Connecting and shrinking...")
conn = sqlite3.connect(dst)
cursor = conn.cursor()

# Empty the massive, unused detailed POS transaction tables
# The dashboard primarily relies on BI_SALES_REPORT for daily aggregates.
cursor.execute("DELETE FROM POS_SALES_DTL")
cursor.execute("DELETE FROM POS_SALES_HDR")

conn.commit()
print("Vacuuming to reclaim space...")
conn.execute("VACUUM")
conn.close()

print(f"Original size: {os.path.getsize(src) / 1024 / 1024:.2f} MB")
print(f"Lite size: {os.path.getsize(dst) / 1024 / 1024:.2f} MB")
