import sys
import traceback

try:
    import st_gat_dashboard
    print("Import successful")
except Exception as e:
    print(f"Import failed: {e}")
    traceback.print_exc()
