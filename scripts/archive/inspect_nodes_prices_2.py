import pandas as pd

nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
df = pd.read_csv(nodes_csv)
matches = df[df['id'].astype(str).str.contains('SOLIO|CLEANROL', case=False, na=False)]
pd.set_option('display.max_columns', None)
print(matches)
