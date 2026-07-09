import sys

file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\ops_dashboard.py'

with open(file_path, 'r', encoding='utf-8') as f:
    content = f.read()

old_code = """            if _sim_state and stocks:
                stats = _sim_state['hour_stats'].get(org_cd)
                if stats:
                    so_ratio = stats.n_stockouts / len(stocks)
                    crit_ratio = stats.n_critical / len(stocks)
            elif stocks:
                so_count = sum(1 for item in stocks if float(item.get('current_stocks', 0)) <= 0)
                crit_count = sum(1 for item in stocks if 0 < float(item.get('current_stocks', 0)) <= 10)
                so_ratio = so_count / len(stocks)
                crit_ratio = crit_count / len(stocks)"""

new_code = """            if stocks:
                active_skus = sum(1 for item in stocks if float(item.get('avg_daily_sales', 0)) > 0 or float(item.get('current_stocks', 0)) > 0)
                denom = max(1, active_skus)
            else:
                denom = 1
                
            if _sim_state and stocks:
                stats = _sim_state['hour_stats'].get(org_cd)
                if stats:
                    so_ratio = stats.n_stockouts / denom
                    crit_ratio = stats.n_critical / denom
            elif stocks:
                so_count = sum(1 for item in stocks if float(item.get('current_stocks', 0)) <= 0 and float(item.get('avg_daily_sales', 0)) > 0)
                crit_count = sum(1 for item in stocks if 0 < float(item.get('current_stocks', 0)) <= 10 and float(item.get('avg_daily_sales', 0)) > 0)
                so_ratio = so_count / denom
                crit_ratio = crit_count / denom"""

if old_code in content:
    content = content.replace(old_code, new_code)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("ops_dashboard.py patched successfully.")
else:
    print("Error: Could not find old code in ops_dashboard.py.")
