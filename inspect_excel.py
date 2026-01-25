
import pandas as pd
import glob
import os

# Define the path to one of the excel files
file_path = r"C:\Users\iLink\.gemini\antigravity\scratch\app\data\dept_1_50.xlsx"

try:
    df = pd.read_excel(file_path)
    print(f"--- Columns in {os.path.basename(file_path)} ---")
    print(df.columns.tolist())
    print("\n--- First 3 rows ---")
    print(df.head(3).to_string())
except Exception as e:
    print(f"Error reading excel file: {e}")
