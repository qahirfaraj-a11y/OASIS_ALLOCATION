import pandas as pd
import os

# 1. Load your actual data from the export directory
print("Loading network data from export directory...")
nodes_path = r'c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv'
edges_path = r'c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv'

nodes_df = pd.read_csv(nodes_path)
edges_df = pd.read_csv(edges_path)

# 2. Filter for SKUs only
skus_df = nodes_df[nodes_df['type'] == 'SKU']

# 3. Create an output directory for the Obsidian notes
# We'll put them in a temporary folder first within the vault
output_dir = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes\SKU_DNA_Profiles"
os.makedirs(output_dir, exist_ok=True)

print(f"Generating DNA Profiles for {len(skus_df)} SKUs. This may take a minute...")

# 4. Loop through every single SKU and generate its scorecard
for index, row in skus_df.iterrows():
    sku_id = str(row['id'])
    # Clean filename for Obsidian (removing slashes/special characters that break file paths)
    safe_filename = sku_id.replace('/', '_').replace(':', '_').replace('"', '')
    
    # Extract Node Data
    department = str(row['department']).replace('[[', '').replace(']]', '')
    supplier = str(row['supplier']).replace('[[', '').replace(']]', '')
    price = float(row['price']) if pd.notna(row['price']) else 0.0
    margin = float(row['margin_pct']) if pd.notna(row['margin_pct']) else 0.0
    revenue = float(row['revenue']) if pd.notna(row['revenue']) else 0.0
    profit = float(row['gross_profit']) if pd.notna(row['gross_profit']) else 0.0
    rank = int(row['sales_rank']) if pd.notna(row['sales_rank']) else 9999
    velocity = float(row['velocity_ads']) if pd.notna(row['velocity_ads']) else 0.0
    qty = float(row['total_quantity']) if pd.notna(row['total_quantity']) else 0.0
    
    # Determine Network Role
    if revenue > 0:
        if margin < 0:
            role = "Active Node (Loss-Leader / Traffic Driver)"
        else:
            role = "Active Node (Profit/Volume Driver)"
    else:
        role = "Dormant Node (Baseline)"

    # Extract Edges
    # We use source/target from edges list
    halo_edges = edges_df[((edges_df['source'] == sku_id) | (edges_df['target'] == sku_id)) & (edges_df['relation'] == 'link')]
    sub_edges = edges_df[((edges_df['source'] == sku_id) | (edges_df['target'] == sku_id)) & (edges_df['relation'] == 'substitution')]
    
    exclude_list = ['Retail Market', 'Unknown', f'[[{department}]]', f'[[{supplier}]]']
    
    def get_links(df):
        linked = []
        for _, edge in df.iterrows():
            target = edge['target'] if edge['source'] == sku_id else edge['source']
            if target not in exclude_list and not str(target).startswith('[['):
                linked.append(f"[[{target}]]") # Format as Obsidian Wiki-link
        return list(set(linked))

    halo_links = get_links(halo_edges)
    sub_links = get_links(sub_edges)

    # 5. Build the Markdown Card Template
    md_content = f"""---
type: SKU
department: "{department}"
supplier: "{supplier}"
price: {price:.2f}
margin: {margin:.2f}
revenue: {revenue:.2f}
gross_profit: {profit:.2f}
sales_rank: {rank}
velocity_ads: {velocity:.4f}
total_quantity: {qty:.2f}
tags: [{ 'active' if revenue > 0 else 'dormant' }{ ', loss_leader' if margin < 0 and revenue > 0 else '' }]
---

# {sku_id}

### [NODE IDENTITY]
* **Network Role:** {role}
* **Macro-Habitat:** [[{department}]]
* **Origin Point:** [[{supplier}]]

### [UNIT ECONOMICS & VELOCITY]
* **Average Retail Price:** {price:,.2f} KES
* **Gross Margin:** {margin:.2f}%
* **Historical Revenue:** {revenue:,.2f} KES
* **Gross Profit:** {profit:,.2f} KES
* **Sales Rank:** #{rank}
* **Velocity (Average Daily Sales):** {velocity:.2f} units/day
* **Total Quantity Moved:** {qty:,.0f} units

### [THE PHYSICS OF THE SHELF]
**The Halo Effect ({len(halo_links)} Links):** *Items frequently bought alongside this SKU.*
{chr(10).join([f'- {link}' for link in halo_links]) if halo_links else '- No distinct basket affinities found.'}

**Cannibalization ({len(sub_links)} Substitutions):**
*Direct shelf competitors fighting for the same basket.*
{chr(10).join([f'- {link}' for link in sub_links]) if sub_links else '- No direct substitutes identified.'}
"""

    # 6. Write to file
    with open(os.path.join(output_dir, f"{safe_filename}.md"), "w", encoding="utf-8") as f:
        f.write(md_content)

print(f"Successfully generated Obsidian DNA Profiles for {len(skus_df)} SKUs in the '{output_dir}' folder!")
