import pandas as pd

kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
df = pd.read_excel(kapa_excel_path, sheet_name='Report', header=None)
print("Shape of kapa.xlsx Report sheet after update:", df.shape)
print("Row 0:", list(df.iloc[0]))
print("Row 1:", list(df.iloc[1]))
print("Row 2 (Columns):", list(df.iloc[2]))
print("Row 3:", list(df.iloc[3]))
print("Row 4:", list(df.iloc[4]))
print("Row 5:", list(df.iloc[5]))
