import os
import json
import re
import yaml

VAULT_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes\SKUs"
METRICS_PATH = "rhapta_master_metrics.json"

def inject_data():
    with open(METRICS_PATH, 'r') as f:
        metrics = json.load(f)
        
    print("Calculating sales ranks based on revenue...")
    sorted_skus = sorted(metrics.items(), key=lambda x: x[1].get('live_revenue', 0), reverse=True)
    for rank, (sku, m) in enumerate(sorted_skus, 1):
        m['sales_rank'] = rank
        
    print("Updating Obsidian nodes with Velocity and Volume...")
    files = [f for f in os.listdir(VAULT_PATH) if f.endswith('.md')]
    updated_count = 0
    
    for filename in files:
        sku_name = filename.replace('.md', '')
        filepath = os.path.join(VAULT_PATH, filename)
        
        if sku_name in metrics:
            m = metrics[sku_name]
            
            with open(filepath, 'r', encoding='utf-8') as f:
                content = f.read()
            
            fm_match = re.search(r'^---\n(.*?)\n---', content, re.DOTALL)
            if fm_match:
                try:
                    fm_data = yaml.safe_load(fm_match.group(1))
                    
                    # Update metrics
                    fm_data['revenue'] = m.get('live_revenue', 0)
                    fm_data['velocity_ads'] = m.get('live_ads', 0)
                    fm_data['total_quantity'] = m.get('live_qty', 0)
                    fm_data['margin_pct'] = m.get('live_margin', 0)
                    fm_data['sales_rank'] = m.get('sales_rank', 9999)
                    
                    # Calculated profit
                    if 'revenue' in fm_data and 'margin_pct' in fm_data:
                        fm_data['gross_profit'] = round(fm_data['revenue'] * (fm_data['margin_pct'] / 100), 2)
                    
                    fm_data['rhapta_fill_rate'] = m.get('live_fill_rate', 0)
                    
                    new_fm = yaml.dump(fm_data, sort_keys=False).strip()
                    new_content = f"---\n{new_fm}\n---" + content[fm_match.end():]
                    
                    with open(filepath, 'w', encoding='utf-8') as f:
                        f.write(new_content)
                    updated_count += 1
                except:
                    pass
                    
    print(f"Update complete! {updated_count} nodes updated with Velocity and Ranking.")

if __name__ == "__main__":
    inject_data()
