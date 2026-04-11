import pandas as pd
import os

file_in = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trn_1_12.xlsx"
file_out = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\trout_1_12.xlsx"

def inspect_excel(path, name):
    print(f"--- Inspecting {name} ---")
    if not os.path.exists(path):
        print(f"File not found: {path}")
        return
    
    try:
        # Read only a few rows to get headers and sample data
        df = pd.read_excel(path, nrows=10)
        print(f"Columns: {df.columns.tolist()}")
        print("Sample Data:")
        print(df.to_string())
        
        # Get basic stats if possible
        full_df = pd.read_excel(path)
        print(f"Total Rows: {len(full_df)}")
        if 'Quantity' in full_df.columns:
            print(f"Total Quantity: {full_df['Quantity'].sum()}")
        elif 'Qty' in full_df.columns:
            print(f"Total Quantity: {full_df['Qty'].sum()}")
            
    except Exception as e:
        print(f"Error reading {path}: {e}")

inspect_excel(file_in, "Transfer In")
inspect_excel(file_out, "Transfer Out")
