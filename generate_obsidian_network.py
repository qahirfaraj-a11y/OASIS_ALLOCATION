import os
import json
import csv
import pandas as pd
from typing import Dict, List, Set
from collections import defaultdict

# Paths
VAULT_PATH = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault"
NODES_PATH = os.path.join(VAULT_PATH, "Nodes")
SKU_PATH = os.path.join(NODES_PATH, "SKUs")
DEPT_PATH = os.path.join(NODES_PATH, "Departments")
SUPP_PATH = os.path.join(NODES_PATH, "Suppliers")

# Data Files
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
PROFIT_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\sales_profitability_intelligence_2025_reclassified.json"
SUPPLIER_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\product_supplier_map.json"
MASTER_DEPT_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\master_product_dept_map.json"
EDGES_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
DEPT_RATIO_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\supplier_dept_ratios.json"

def sanitize_filename(name: str) -> str:
    if not isinstance(name, str): name = str(name)
    return name.replace("/", "-").replace("\\", "-").replace(":", "-").replace("*", "-").replace("?", "-").replace("\"", "-").replace("<", "-").replace(">", "-").replace("|", "-")

def ensure_dirs():
    for p in [SKU_PATH, DEPT_PATH, SUPP_PATH]:
        if not os.path.exists(p):
            os.makedirs(p)

def generate_network():
    ensure_dirs()
    
    # 1. Load Data
    print("Loading data...")
    try:
        scorecard_df = pd.read_csv(SCORECARD_FILE)
    except Exception as e:
        print(f"Error loading scorecard: {e}")
        return

    with open(PROFIT_JSON, 'r') as f:
        profit_data = json.load(f)
    
    with open(SUPPLIER_JSON, 'r') as f:
        supplier_map = json.load(f)

    with open(MASTER_DEPT_JSON, 'r') as f:
        master_dept_map = json.load(f)

    try:
        with open(DEPT_RATIO_JSON, 'r') as f:
            dept_ratios = json.load(f)
    except:
        dept_ratios = {}

    # 2. Process SKUs and Aggregate Supplier Stats
    print("Processing SKUs and aggregating Supplier intelligence...")
    departments = set()
    suppliers = set()
    dept_skus = {}
    sku_records = []
    
    # Supplier Intelligence Containers
    supp_intelligence = defaultdict(lambda: {
        'reliability': [], 
        'lead_time': [], 
        'revenue': 0, 
        'profit': 0, 
        'sku_count': 0,
        'depts': set()
    })

    # SKU -> Supplier quick lookup for Halo mapping
    sku_supp_lookup = {}

    for _, row in scorecard_df.iterrows():
        name = str(row.get('Product', 'Unknown'))
        if name == 'Unknown' or not name: continue
        
        dept = master_dept_map.get(name.upper().strip())
        if not dept:
            dept = str(row.get('Department', 'GENERAL'))
        
        intel = profit_data.get(name, {})
        if not master_dept_map.get(name) and 'category' in intel:
            dept = intel['category']
            
        supplier = supplier_map.get(name, str(row.get('Supplier', 'Unknown')))
        price = float(row.get('Unit_Price', 0))
        margin = intel.get('margin_pct', 0)
        revenue = intel.get('revenue', 0)
        profit = intel.get('gross_profit', 0)
        rank = intel.get('sales_rank', 9999)

        reliability = float(row.get('Supplier_Reliability', 0.85))
        lead_time = float(row.get('Lead_Time_Days', 7))

        sku_records.append({
            'name': name, 'dept': dept, 'supplier': supplier,
            'price': price, 'margin': margin, 'revenue': revenue,
            'profit': profit, 'rank': rank
        })
        
        sku_supp_lookup[name] = supplier
        
        # Aggregation
        supp_intelligence[supplier]['reliability'].append(reliability)
        supp_intelligence[supplier]['lead_time'].append(lead_time)
        supp_intelligence[supplier]['revenue'] += revenue
        supp_intelligence[supplier]['profit'] += profit
        supp_intelligence[supplier]['sku_count'] += 1
        supp_intelligence[supplier]['depts'].add(dept)
        
        departments.add(dept)
        suppliers.add(supplier)
        
        if dept not in dept_skus:
            dept_skus[dept] = []
        dept_skus[dept].append({'name': name, 'price': price})

    # 3. Halo Mapping: Competitors and Complimentary
    print("Building Halo Mapping (Affinity & Competition)...")
    supp_affinity = defaultdict(lambda: defaultdict(int))
    
    if os.path.exists(EDGES_FILE):
        print(f"Analyzing {EDGES_FILE} for cross-supplier affinities...")
        edges_df = pd.read_csv(EDGES_FILE)
        for _, row in edges_df.iterrows():
            if row['relation'] == 'link': # Affinity link
                s_sku, t_sku = row['source'], row['target']
                s_supp = sku_supp_lookup.get(s_sku)
                t_supp = sku_supp_lookup.get(t_sku)
                if s_supp and t_supp and s_supp != t_supp:
                    supp_affinity[s_supp][t_supp] += 1
                    supp_affinity[t_supp][s_supp] += 1

    # Direct Competitors: Shared Departments from ratio map
    supp_competitors = defaultdict(set)
    for dept_name, supp_list in dept_ratios.items():
        active_supps = [s for s in supp_list if s in suppliers]
        for s1 in active_supps:
            for s2 in active_supps:
                if s1 != s2:
                    supp_competitors[s1].add(s2)

    # 4. Write SKU Files
    print(f"Writing {len(sku_records)} SKU nodes...")
    for rec in sku_records:
        filename = sanitize_filename(rec['name']) + ".md"
        filepath = os.path.join(SKU_PATH, filename)
        subs = [other['name'] for other in dept_skus.get(rec['dept'], []) 
                if other['name'] != rec['name'] and 0.8 * rec['price'] <= other['price'] <= 1.2 * rec['price']][:5]
        
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"---\ntype: SKU\ndepartment: \"[[{sanitize_filename(rec['dept'])}]]\"\nsupplier: \"[[{sanitize_filename(rec['supplier'])}]]\"\n")
            f.write(f"price: {rec['price']}\nmargin_pct: {rec['margin']}\nrevenue: {rec['revenue']}\ngross_profit: {rec['profit']}\nsales_rank: {rec['rank']}\n---\n\n")
            f.write(f"# {rec['name']}\n\n## Relationships\n- **Department**: [[{sanitize_filename(rec['dept'])}]]\n- **Supplier**: [[{sanitize_filename(rec['supplier'])}]]\n\n")
            if subs:
                f.write(f"## Potential Substitutes\n")
                for s in subs: f.write(f"- [substitution]:: [[{sanitize_filename(s)}]]\n")
            f.write(f"\n## Network Insights\n- [upstream_supply]:: [[{sanitize_filename(rec['supplier'])}]]\n- [downstream_demand]:: [[Retail Market]]\n")

    # 5. Write Higher-Level Nodes
    print("Writing enriched Department and Supplier nodes...")
    # 5. Write Higher-Level Nodes
    print("Writing enriched Department and Supplier nodes...")
    
    # Financial Stats by Department
    dept_stats = scorecard_df.groupby('Department').agg({
        'Total_Revenue': 'sum',
        'Margin_Pct': 'mean',
        'Product': 'count',
        'Capital_Required': 'sum'
    }).rename(columns={'Total_Revenue': 'revenue', 'Product': 'sku_count', 'Margin_Pct': 'avg_margin'})

    # Top Suppliers per Dept
    dept_top_suppliers = {}
    for dept_id, group in scorecard_df.groupby('Department'):
        top_supps = group.groupby('Supplier')['Total_Revenue'].sum().sort_values(ascending=False).head(3).index.tolist()
        dept_top_suppliers[dept_id] = top_supps

    # Neural Affinity Mapping (Hardcoded logic based on GNN clusters)
    NEURAL_AFFINITY = {
        "BABY PRODUCT": ["DIAPERS", "WIPES", "BABY FOOD"],
        "DIAPERS": ["WIPES", "BABY PRODUCT"],
        "WIPES": ["DIAPERS", "BABY PRODUCT"],
        "FRESH MILK": ["BREAD", "BUTTER", "CEREAL", "YOGHURT"],
        "BREAD": ["FRESH MILK", "BUTTER", "JAM", "FLOUR"],
        "BUTTER": ["BREAD", "FRESH MILK", "MARGARINE"],
        "ALCOHOL": ["SNACKS", "SODA", "CIGARETTES", "BEER"],
        "SNACKS": ["ALCOHOL", "SODA", "CONFECTIONERY"],
        "SODA": ["ALCOHOL", "SNACKS", "MINERAL WATER"],
        "TOILETRIES": ["SANITARY TOWELS", "SOAP", "DEODORANT", "TOILET ROLL"],
        "SANITARY TOWELS": ["TOILETRIES", "WIPES"],
        "SOAP": ["TOILETRIES", "ORAL CARE"],
        "FLOUR": ["SUGAR", "COOKING OIL", "BREAD"],
        "SUGAR": ["FLOUR", "FRESH MILK", "TEA"],
        "BEER": ["ALCOHOL", "SNACKS"],
        "EGGS": ["BREAD", "FRESH MILK"],
    }

    for dept in departments:
        filename = sanitize_filename(dept) + ".md"
        filepath = os.path.join(DEPT_PATH, filename)
        
        # Extract data
        data = dept_stats.loc[dept] if dept in dept_stats.index else None
        revenue = data['revenue'] if data is not None else 0
        margin = data['avg_margin'] if data is not None else 0
        skus = int(data['sku_count']) if data is not None else 0
        capital = data['Capital_Required'] if data is not None else 0
        suppliers = dept_top_suppliers.get(dept, ["Unknown"])
        
        # Neural Neighbor logic
        related = NEURAL_AFFINITY.get(dept.upper(), ["GENERAL HOUSEHOLD"])
        related_links = ", ".join([f"[[{r}]]" for r in related])

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"---\ntype: Department\nrevenue: {revenue:.2f}\navg_margin: {margin:.2f}%\nsku_count: {skus}\ncapital_weight: {capital:.0f}\nneural_neighbors: [{related_links}]\n---\n")
            f.write(f"# {dept}\n\n")
            f.write(f"## 🧠 Neural Relational Intelligence\n")
            f.write(f"- **High-Affinity Departments**: {related_links}\n")
            f.write(f"- **Primary Supplier Hubs**: {', '.join([f'[[{s}]]' for s in suppliers])}\n")
            f.write(f"- **Network Role**: {'Core Stability' if revenue > 1000000 else ('Growth Driver' if margin > 15 else 'Operational Support')}\n\n")
            f.write(f"## 📊 Financial Performance (Est. Monthly)\n")
            f.write(f"- **Projected Revenue**: Kes {revenue:,.2f}\n")
            f.write(f"- **Average Margin**: {margin:,.2f}%\n")
            f.write(f"- **Capital Commitment**: Kes {capital:,.0f}\n\n")
            f.write(f"## 🛒 SKUs in this Department\n```dataview\nLIST FROM \"oasis_vault/Nodes/SKUs\" WHERE department = [[{sanitize_filename(dept)}]]\n```")

    for supp in suppliers:
        filename = sanitize_filename(supp) + ".md"
        filepath = os.path.join(SUPP_PATH, filename)
        intel = supp_intelligence.get(supp, {})
        avg_rel = round(sum(intel['reliability']) / len(intel['reliability']), 2) if intel['reliability'] else 0.85
        avg_lt = round(sum(intel['lead_time']) / len(intel['lead_time']), 1) if intel['lead_time'] else 7
        
        # Halo partners: Top 5 by affinity weight
        complimentary = sorted(supp_affinity[supp].items(), key=lambda x: x[1], reverse=True)[:5]
        competitors = list(supp_competitors[supp])[:5]

        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(f"---\ntype: Supplier\nreliability: {avg_rel}\navg_lead_time: {avg_lt}\nrevenue: {round(intel['revenue'], 2)}\nsku_count: {intel['sku_count']}\n---\n\n")
            f.write(f"# Supplier: {supp}\n\n")
            f.write(f"## Performance Profile\n")
            f.write(f"- **Reliability Index**: {int(avg_rel * 100)}%\n")
            f.write(f"- **Avg Lead Time**: {avg_lt} days\n")
            f.write(f"- **Total Revenue Contribution**: KES {intel['revenue']:,.2f}\n")
            f.write(f"- **Active SKU Count**: {intel['sku_count']}\n\n")
            
            f.write(f"## Halo Relations\n")
            if competitors:
                f.write(f"### Direct Competitors (Category Rivals)\n")
                for comp in competitors: f.write(f"- [competitor]:: [[{sanitize_filename(comp)}]]\n")
            
            if complimentary:
                f.write(f"\n### Complimentary Partners (Affinity Links)\n")
                for partner, weight in complimentary: f.write(f"- [complimentary]:: [[{sanitize_filename(partner)}]] (Weight: {weight})\n")
                
            f.write(f"\n## SKUs Listing\n```dataview\nLIST FROM \"oasis_vault/Nodes/SKUs\" WHERE supplier = [[{sanitize_filename(supp)}]]\n```")

    print("Network generation complete!")

if __name__ == "__main__":
    generate_network()
