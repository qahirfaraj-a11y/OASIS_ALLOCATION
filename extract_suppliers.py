import pandas as pd
import re

FILE_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx"

def extract_suppliers():
    try:
        df = pd.read_excel(FILE_PATH)
        # Find the column with long strings (e.g., > 100 chars) or name it.
        # Based on previous output, it was the last column or column index 5.
        target_col = None
        for col in df.columns:
            if df[col].dtype == 'object' and df[col].str.len().mean() > 50:
                target_col = col
                break
        
        if not target_col:
            print("Could not find Supplier column.")
            return

        print(f"Found Supplier Column: {target_col}")
        
        all_suppliers = set()
        for row in df[target_col].dropna():
            # Split by comma (assuming comma separation as seen in sample)
            # Sample: "Sa0024 - Aquamist, Sc0167 - Chandaria Industries..."
            parts = str(row).split(',')
            for p in parts:
                p = p.strip()
                if p:
                    all_suppliers.add(p)
        
        # Sort and print Top 100 and search for specific ones
        sorted_supp = sorted(list(all_suppliers))
        
        print(f"Total Unique Suppliers Found: {len(sorted_supp)}")
        print("First 20 Suppliers:")
        for s in sorted_supp[:20]:
            print(s)
            
        print("-" * 30)
        print("Searching for 'Brookside' variations:")
        for s in sorted_supp:
            if "BROOK" in s.upper() or "DAIRY" in s.upper():
                print(f"MATCH: {s}")

    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    extract_suppliers()
