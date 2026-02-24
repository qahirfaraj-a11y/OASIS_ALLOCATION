"""
Check Simulation Output Structure
"""
import pandas as pd

sim_file = r"C:\Users\iLink\Downloads\simulation_results_Medium_1M_20260208_182418.xlsx"

# Check all sheets
xl = pd.ExcelFile(sim_file)
print("Sheets:", xl.sheet_names)

for sheet in xl.sheet_names:
    df = pd.read_excel(sim_file, sheet_name=sheet)
    print(f"\n{'='*80}")
    print(f"Sheet: {sheet}")
    print(f"{'='*80}")
    print(f"Columns: {list(df.columns)}")
    print(f"Rows: {len(df)}")
    
    if len(df) > 0:
        print(f"\nFirst few rows:")
        print(df.head(3))
