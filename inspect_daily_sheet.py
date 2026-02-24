import pandas as pd

FILE_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Order_Calendar_2026.xlsx"
SHEET_NAME = "Daily Suppliers & Info"

try:
    df = pd.read_excel(FILE_PATH, sheet_name=SHEET_NAME)
    print(f"--- SHEET: {SHEET_NAME} ---")
    print("COLUMNS:", df.columns.tolist())
    print("-" * 30)
    print(df.head(20).to_string())
    
    # Check for Brookside directly here
    print("-" * 30)
    print("Searching for 'Brookside' in this sheet:")
    for col in df.columns:
        if df[col].dtype == 'object':
            matches = df[df[col].astype(str).str.contains("BROOKSIDE", case=False, na=False)]
            if not matches.empty:
                print(f"Found in column '{col}':")
                print(matches[col].unique())

except Exception as e:
    print(f"Error: {e}")
