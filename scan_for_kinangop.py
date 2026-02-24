import pandas as pd
import glob

def scan_excel_for_term(term):
    files = ["Supplier_Order_Calendar_2026.xlsx", "Supplier_Master_Schedule_V2_2026.pdf"] # Can't read PDF easily, skipping
    files = glob.glob("*.xlsx")
    
    print(f"Scanning for '{term}' in excel files...")
    
    for f in files:
        if "simulation_results" in f: continue
        try:
            xl = pd.ExcelFile(f)
            for sheet in xl.sheet_names:
                df = xl.parse(sheet)
                # Convert all to string and search
                mask = df.astype(str).apply(lambda x: x.str.contains(term, case=False, na=False)).any(axis=1)
                matches = df[mask]
                if not matches.empty:
                    print(f"FOUND in {f} [{sheet}]:")
                    print(matches.iloc[:, :5].to_string())
                    print("-" * 40)
        except Exception as e:
            print(f"Error reading {f}: {e}")

if __name__ == "__main__":
    scan_excel_for_term("Kinangop")
