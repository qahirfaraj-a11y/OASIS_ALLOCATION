import pandas as pd
import os

file_path = r"c:\Users\iLink\.gemini\antigravity\scratch\app\data\Cooking.XLSX"

try:
    df = pd.read_excel(file_path, nrows=5)
    print(f"Inspecting {os.path.basename(file_path)}")
    print("Columns found:")
    print(df.columns.tolist())
    print("\nFirst row:")
    print(df.iloc[0])
except Exception as e:
    print(f"Error reading file: {e}")
