import sys

file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\ops_dashboard.py'
with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_code = """                    so_ratio = stats.n_stockouts / denom
                    crit_ratio = stats.n_critical / denom"""
new_code = """                    so_ratio = stats.n_stockouts / denom
                    crit_ratio = getattr(stats, 'n_critical', 0) / denom"""

if old_code in content:
    content = content.replace(old_code, new_code)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("ops_dashboard.py n_critical patched!")
else:
    print("Failed to find old code in ops_dashboard.py.")
