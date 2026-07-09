import pandas as pd

kapa_excel_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
df = pd.read_excel(kapa_excel_path, header=2)
matches = df[df['DESCRIPTION'].astype(str).str.contains('SOLIO|CLEANROL', case=False, na=False)]
print(matches)
