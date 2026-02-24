import pandas as pd
import os

FILE_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx"

try:
    df = pd.read_excel(FILE_PATH)
    print("COLUMNS:", df.columns.tolist())
    print("-" * 30)
    print(df.head(5).to_string())
except Exception as e:
    print(f"Error reading file: {e}")
