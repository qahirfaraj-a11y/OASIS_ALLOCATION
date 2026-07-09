import shutil
import os

src = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp.db"
dst = r"C:\Oasis\oasis\data\mock_pos_erp.db"

if os.path.exists(dst):
    try:
        os.remove(dst)
        print(f"Removed existing incomplete DB: {dst}")
    except Exception as e:
        print(f"Could not remove existing DB (maybe it's open in another terminal?): {e}")

try:
    shutil.copy2(src, dst)
    print(f"Successfully copied real mock database from {src} to {dst}")
except Exception as e:
    print(f"Failed to copy DB: {e}")
