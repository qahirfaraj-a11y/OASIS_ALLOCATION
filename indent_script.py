import sys

with open("ops_dashboard.py", "r", encoding="utf-8") as f:
    lines = f.readlines()

start_idx = 925
end_idx = 1093

# Lines 925 is: "    engine = get_order_engine()\n"
# Line 1092 is: "                           {\"items\": len(pos_recs), \"total_qty\": sum(r.get('recommended_quantity', 0) for r in pos_recs)})\n"

for i in range(start_idx, end_idx):
    lines[i] = "    " + lines[i]

with open("ops_dashboard_indented.py", "w", encoding="utf-8") as f:
    f.writelines(lines)
print("Indentation complete into ops_dashboard_indented.py")
