import pandas as pd
import json

file_in = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trn_1_12.xlsx"
file_out = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trout_1_12.xlsx"

def dump_excel(path, name):
    print(f"\n--- Raw Dump: {name} ---")
    try:
        df = pd.read_excel(path).head(10)
        # Convert to records
        records = df.to_dict(orient='records')
        print(json.dumps(records, indent=2, default=str))
    except Exception as e:
        print(f"Error: {e}")

dump_excel(file_in, "TRN")
dump_excel(file_out, "TROUT")
