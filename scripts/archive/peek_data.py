import pandas as pd

def check_dates(file_path):
    print(f"--- {file_path} ---")
    df = pd.read_excel(file_path, header=1)
    print("Columns:", df.columns.tolist())
    print(df.head())

check_dates(r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\jan_cash.xlsx")
