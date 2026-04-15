
import logging
import sys
import os
import pandas as pd
sys.path.append(os.getcwd())
from oasis.simulation.data_loader import HistoricalDataLoader

logging.basicConfig(level=logging.INFO)
loader = HistoricalDataLoader("oasis/data")
indices = loader.load_seasonality_indices()

# Trace logic
total_dict = {}
months = ['jan', 'feb', 'mar', 'apr', 'may', 'jun', 'jul', 'aug', 'sep', 'oct']
for m in months:
    fpath = os.path.join("oasis/data", f"{m}_cash.xlsx")
    if os.path.exists(fpath):
        df = pd.read_excel(fpath)
        max_sum = 0
        for col in df.columns:
            s = pd.to_numeric(df[col], errors='coerce').fillna(0).sum()
            if s > max_sum: max_sum = s
        total_dict[m.upper()] = max_sum

valid_totals = [v for v in total_dict.values() if v > 100.0]
avg_vol = sum(valid_totals) / len(valid_totals) if valid_totals else 1.0

print(f"MONTHLY TOTALS: {total_dict}")
print(f"AVG VOL: {avg_vol}")
print(f"INDICES: {indices}")
