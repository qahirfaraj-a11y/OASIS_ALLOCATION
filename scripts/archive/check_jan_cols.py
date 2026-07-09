import pandas as pd
import os

def check_cols():
    pos_path = "oasis/data/jan_cash.xlsx"
    if os.path.exists(pos_path):
        df = pd.read_excel(pos_path, nrows=5)
        print(f"Columns for {pos_path}: {df.columns.tolist()}")
        print(f"First row: {df.iloc[0].tolist()}")

if __name__ == "__main__":
    check_cols()
