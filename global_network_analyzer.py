import os
import re
import yaml
import json
import pandas as pd

VAULT_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes\SKUs"

def parse_sku_file(filepath):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Extract Frontmatter
    fm_match = re.search(r'^---\n(.*?)\n---', content, re.DOTALL)
    data = {}
    if fm_match:
        try:
            data = yaml.safe_load(fm_match.group(1))
        except:
            pass
            
    # Extract Substitutes from content if not in YAML
    substitutes = re.findall(r'\[substitution\]:: \[\[(.*?)\]\]', content)
    data['substitute_count'] = len(substitutes)
    data['sku_name'] = os.path.basename(filepath).replace('.md', '')
    
    return data

def run_census():
    results = []
    files = [f for f in os.listdir(VAULT_PATH) if f.endswith('.md')]
    print(f"Starting census of {len(files)} SKUs...")
    
    for filename in files:
        filepath = os.path.join(VAULT_PATH, filename)
        results.append(parse_sku_file(filepath))
        
    df = pd.DataFrame(results)
    
    # Cleaning Department and Supplier (removing brackets)
    if 'department' in df.columns:
        df['department'] = df['department'].astype(str).str.replace('[[', '', regex=False).str.replace(']]', '', regex=False)
    if 'supplier' in df.columns:
        df['supplier'] = df['supplier'].astype(str).str.replace('[[', '', regex=False).str.replace(']]', '', regex=False)
        
    # Analysis 1: Departmental Connectivity
    dept_stats = df.groupby('department').agg({
        'sku_name': 'count',
        'substitute_count': 'mean',
        'gross_profit': 'sum'
    }).rename(columns={'sku_name': 'sku_count', 'substitute_count': 'avg_substitutes'})
    
    # Analysis 2: Supplier Dominance
    supp_stats = df.groupby('supplier').agg({
        'sku_name': 'count',
        'revenue': 'sum'
    }).rename(columns={'sku_count': 'sku_count', 'revenue': 'total_revenue'})
    
    # Analysis 3: Fragility (High Revenue, low subs)
    fragile_skus = df[(df['revenue'] > df['revenue'].quantile(0.9)) & (df['substitute_count'] == 0)]
    
    # Save Outputs
    output_data = {
        'total_skus': len(df),
        'total_departments': len(dept_stats),
        'total_suppliers': len(supp_stats),
        'top_depts': dept_stats.sort_values('sku_count', ascending=False).head(10).to_dict('index'),
        'top_suppliers': supp_stats.sort_values('total_revenue', ascending=False).head(10).to_dict('index'),
        'most_interconnected_depts': dept_stats.sort_values('avg_substitutes', ascending=False).head(10).to_dict('index'),
        'fragile_nodes': fragile_skus[['sku_name', 'revenue', 'supplier']].head(10).to_dict('records')
    }
    
    with open('network_census_results.json', 'w') as f:
        json.dump(output_data, f, indent=4)
        
    print("Census complete. Results saved to network_census_results.json")

if __name__ == "__main__":
    run_census()
