from __future__ import annotations
import sys, json
from pathlib import Path
from collections import defaultdict
import openpyxl

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.amit_return import norm

FILES = ["jan_cash.xlsx","mar_cash.xlsx","may_cash.xlsx","jun_cash.xlsx","sep_cash.xlsx","oct_cash.xlsx"]
DATA = ROOT / "oasis" / "data"

monthly = {}
for fn in FILES:
    wb = openpyxl.load_workbook(DATA / fn, read_only=True, data_only=True)
    ws = wb["Report"]
    d = defaultdict(float)
    header = None
    name_i = qty_i = None
    for row in ws.iter_rows(values_only=True):
        if header is None:
            # header is the row with 'Item Name' in it (row 0 is a store-name banner)
            if row and any(str(c).strip() == "Item Name" for c in row if c is not None):
                header = [str(c).strip() if c is not None else "" for c in row]
                name_i = header.index("Item Name")
                qty_i = header.index("Qty")
            continue
        if name_i is None or qty_i >= len(row):
            continue
        name = row[name_i]; qty = row[qty_i]
        if not name or qty is None:
            continue
        try:
            q = float(qty)
        except (TypeError, ValueError):
            continue
        d[norm(str(name))] += q
    monthly[fn] = dict(d)
    print(f"{fn}: header_cols={header} -> {len(d)} distinct normed items, total qty {sum(d.values()):,.0f}", flush=True)

out = ROOT / "devkit" / "cash_monthly_by_sku.json"
out.write_text(json.dumps(monthly))
print("wrote", out)
