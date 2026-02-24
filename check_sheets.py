import pandas as pd

FILE_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx"

try:
    xls = pd.ExcelFile(FILE_PATH)
    print("SHEET NAMES:", xls.sheet_names)
except Exception as e:
    print(f"Error: {e}")
