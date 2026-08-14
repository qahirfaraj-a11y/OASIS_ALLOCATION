import os
import sys
import socket
import logging
import sqlite3
from datetime import datetime

# Setup paths
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))  # devkit/ -> repo root
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))  # devkit/ -> repo root

logger = logging.getLogger("OASIS.Diagnostics")
logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s")

def check_port(port):
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        return s.connect_ex(('localhost', port)) != 0

def run_diagnostics():
    print("\n" + "="*50)
    print("   O.A.S.I.S. PRODUCTION DIAGNOSTIC SUITE")
    print("="*50 + "\n")

    errors = 0
    warnings = 0

    # 1. Environment Check
    print(f"[*] Checking Python Environment: {sys.version.split()[0]}")
    if ".oasis_venv" not in sys.executable:
        print("    [WARNING] Not running in .oasis_venv.")
        warnings += 1
    else:
        print("    [OK] Virtual Environment identified.")

    # 2. Directory Health
    dirs = ["oasis/data", "shadow_logs", "monitoring/inbound", "monitoring/archive"]
    for d in dirs:
        path = os.path.join(ROOT, d)
        if os.path.exists(path):
            print(f"    [OK] Directory exists: {d}")
        else:
            print(f"    [ERR] Missing directory: {d}")
            errors += 1

    # 3. Database Connectivity
    db_path = os.path.join(ROOT, "oasis", "data", "mock_pos_erp.db")
    if os.path.exists(db_path):
        try:
            conn = sqlite3.connect(db_path)
            res = conn.execute("SELECT COUNT(*) FROM ITEM_MST").fetchone()
            print(f"    [OK] DB Connected: {res[0]} SKUs indexed.")
            
            # Check for production tables
            tables = conn.execute("SELECT name FROM sqlite_master WHERE type='table'").fetchall()
            table_names = [t[0] for t in tables]
            if "OASIS_SYSTEM_CONFIG" in table_names:
                print("    [OK] Production Config schema detected.")
            else:
                print("    [ERR] Missing production tables.")
                errors += 1
            conn.close()
        except Exception as e:
            print(f"    [ERR] DB Integrity check failed: {e}")
            errors += 1
    else:
        print("    [ERR] Database file not found.")
        errors += 1

    # 4. Port Availability
    for app, port in [("Dashboard", 8501), ("KUBER", 8502)]:
        if check_port(port):
            print(f"    [OK] Port {port} ({app}) is available.")
        else:
            # v1.2: All ports are now Warnings. Launchers (BAT files) have internal
            # Port Hunting logic to find the next available slot automatically.
            print(f"    [WARNING] Port {port} ({app}) is busy. Launchers will auto-hunt for the next open port.")
            warnings += 1

    print("\n" + "="*50)
    if errors == 0:
        print(f"   DIAGNOSTIC COMPLETE: SYSTEM HEALTHY ({warnings} warnings)")
    else:
        print(f"   DIAGNOSTIC FAILED: {errors} Errors Found.")
    print("="*50 + "\n")
    
    return errors == 0

if __name__ == "__main__":
    if not run_diagnostics():
        sys.exit(1)
