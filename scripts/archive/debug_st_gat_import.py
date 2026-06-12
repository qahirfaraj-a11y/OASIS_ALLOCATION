import traceback
import sys
import os

print("--- Start Debug ST-GAT Import ---")
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

try:
    print("Importing st_gat_dashboard...")
    # Add a hook to trace calls if needed, but first let's try basic traceback
    import st_gat_dashboard
    print("Import successful")
except Exception as e:
    print(f"\n!!! Caught Exception: {type(e).__name__}: {e}")
    traceback.print_exc()
    
    # Try to find the line that failed
    tb = sys.exc_info()[2]
    while tb:
        frame = tb.tb_frame
        print(f"File: {frame.f_code.co_filename}, Line: {tb.tb_lineno}, Func: {frame.f_code.co_name}")
        tb = tb.tb_next
