import os
import pandas as pd
import json
import re

# Paths
VAULT_DEPT_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes\Departments"
SCORECARD_PATH = r"C:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"

# Mock Neural Affinity Mapping (Derived from GNN category clusters)
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

def sanitize_filename(name):
    return re.sub(r'[\\/*?:"<>|]', "", name).strip()

def enrich_departments():
    if not os.path.exists(SCORECARD_PATH):
        print(f"Scorecard not found at {SCORECARD_PATH}")
        return

    # 1. Load Financial Data from Full Scorecard
    print(f"Loading data from {SCORECARD_PATH}...")
    df = pd.read_csv(SCORECARD_PATH)
    
    # Group by Department
    dept_stats = df.groupby('Department').agg({
        'Total_Revenue': 'sum',
        'Margin_Pct': 'mean',
        'Product': 'count',
        'Capital_Required': 'sum'
    }).rename(columns={'Total_Revenue': 'est_monthly_revenue', 'Product': 'sku_count', 'Margin_Pct': 'avg_margin'})

    # 2. Extract Top Suppliers per Dept
    dept_suppliers = {}
    for dept, group in df.groupby('Department'):
        top_supps = group.groupby('Supplier')['Total_Revenue'].sum().sort_values(ascending=False).head(3).index.tolist()
        dept_suppliers[dept] = top_supps

    # 3. Process each Department file in the vault
    if not os.path.exists(VAULT_DEPT_PATH):
        print(f"Vault path not found at {VAULT_DEPT_PATH}")
        return

    dept_files = [f for f in os.listdir(VAULT_DEPT_PATH) if f.endswith('.md')]
    print(f"Processing {len(dept_files)} department nodes...")

    updated_count = 0
    for filename in dept_files:
        dept_name = filename.replace('.md', '')
        filepath = os.path.join(VAULT_DEPT_PATH, filename)
        
        # Get stats for this dept (Case-insensitive match)
        data = None
        match_name = next((idx for idx in dept_stats.index if idx.upper() == dept_name.upper()), None)
        if match_name:
            data = dept_stats.loc[match_name]
        
        if data is not None:
            revenue = data['est_monthly_revenue']
            margin = data['avg_margin']
            skus = int(data['sku_count'])
            capital = data['Capital_Required']
            suppliers = dept_suppliers.get(match_name, ["Unknown"])
            updated_count += 1
        else:
            # If no data found, use defaults but still update format
            revenue = 0
            margin = 0
            skus = 0
            capital = 0
            suppliers = ["Unknown"]

        # Related Departments (Neural Affinity)
        related = NEURAL_AFFINITY.get(dept_name.upper(), ["GENERAL HOUSEHOLD"])
        related_links = ", ".join([f"[[{r}]]" for r in related])

        new_content = f"""---
type: Department
revenue: {revenue:,.2f}
avg_margin: {margin:,.2f}%
sku_count: {skus}
capital_weight: {capital:,.0f}
neural_neighbors: [{related_links}]
---
# {dept_name}

## 🧠 Neural Relational Intelligence
- **High-Affinity Departments**: {related_links}
- **Primary Supplier Hubs**: {", ".join([f"[[{s}]]" for s in suppliers])}
- **Network Role**: {"Core Stability" if revenue > 1000000 else ("Growth Driver" if margin > 15 else "Operational Support")}

## 📊 Financial Performance (Est. Monthly)
- **Projected Revenue**: Kes {revenue:,.2f}
- **Average Margin**: {margin:,.2f}%
- **Capital Commitment**: Kes {capital:,.0f}

## 🛒 SKUs in this Department
```dataview
LIST FROM "oasis_vault/Nodes/SKUs" WHERE department = [[{dept_name}]]
```
"""
        with open(filepath, 'w', encoding='utf-8') as f:
            f.write(new_content)

    print(f"Enrichment complete. {updated_count} nodes enriched with financial data.")

if __name__ == "__main__":
    enrich_departments()
