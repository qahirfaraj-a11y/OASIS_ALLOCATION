import pandas as pd
import os

def check():
    files = {
        'supplier_fragility_map.csv': r"C:\Users\iLink\.gemini\antigravity\scratch\supplier_fragility_map.csv",
        'Supplier_Fulfillment_Summary.xlsx': r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Fulfillment_Summary.xlsx",
        'Supplier_Intelligence_Report_2025_v3.xlsx': r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Intelligence_Report_2025_v3.xlsx",
        'Supplier_Lead_Time_Analysis.csv': r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Lead_Time_Analysis.csv",
    }
    
    for name, fp in files.items():
        if not os.path.exists(fp):
            print(f"{name} not found")
            continue
        print(f"\n=== Inspecting {name} ===")
        try:
            if fp.endswith(".csv"):
                df = pd.read_csv(fp)
            else:
                df = pd.read_excel(fp)
            print("Columns:", list(df.columns))
            # Search for Kapa
            kapa = df[df.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)]
            print(f"KAPA rows ({len(kapa)}):")
            print(kapa.to_string())
        except Exception as e:
            print(f"Error inspecting {name}: {e}")

check()
