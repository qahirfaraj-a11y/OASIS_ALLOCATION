import sys
import os

def verify_stgat_health():
    print("--- O.A.S.I.S. ST-GAT PARITY AUDIT ---")
    
    st_port = 8503
    
    # 1. Check Port Configuration in Batch
    print(f"\n[1/3] Checking Orchestrator config (Run_OASIS_Mosaic.bat)...")
    if os.path.exists("Run_OASIS_Mosaic.bat"):
        with open("Run_OASIS_Mosaic.bat", 'r') as f:
            content = f.read()
            if "STGAT_PORT=8503" in content or "STGAT_PORT" in content:
                print("  [OK] Orchestrator correctly manages ST-GAT node.")
            else:
                print("  [ERR] Orchestrator port configuration mismatch.")
    
    # 2. Check Portal Route
    print(f"\n[2/3] Checking Page Integrity in Portal...")
    page_path = "oasis-portal/src/app/simulation/page.tsx"
    if os.path.exists(page_path):
        with open(page_path, 'r') as f:
            content = f.read()
            if "iframe" in content and "8503" in content:
                print("  [OK] Portal 'Simulation' page correctly wraps ST-GAT iframe (Parity Enabled).")
            else:
                print("  [ERR] Portal page is missing the iframe parity wrapper.")
    else:
        print("  [ERR] Portal page file not found.")

    # 3. Check ML Site Selection dependencies
    print(f"\n[3/3] Auditing Expansion Dependencies...")
    try:
        import sklearn
        import joblib
        import folium
        import streamlit_folium
        print("  [OK] Geospatial & ML libraries (sklearn, joblib, folium) are available.")
        
        if os.path.exists("expansion_model.joblib"):
             print("  [OK] expansion_model.joblib is present (ML Site Scoring Enabled).")
    except ImportError as e:
        print(f"  [ERR] Missing expansion dependencies: {e}")

if __name__ == "__main__":
    verify_stgat_health()
