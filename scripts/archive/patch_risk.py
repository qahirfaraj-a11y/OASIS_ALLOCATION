import sys

file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\ops_dashboard.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_loop = """        for i, src in enumerate(gnn_sim.stores_data):
            org_cd = src.get('store_id', '')
            stocks = all_stocks.get(org_cd, [])"""

new_loop = """        for i, src in enumerate(gnn_sim.stores_data):
            store_id = src.get('store_id', '')
            org_cd = store_id.replace('CFP-', 'ORG')
            stocks = all_stocks.get(org_cd, [])"""

if old_loop in content:
    content = content.replace(old_loop, new_loop)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("ops_dashboard.py risk score ORG mapping patched!")
else:
    print("Failed to find old loop in ops_dashboard.py.")
