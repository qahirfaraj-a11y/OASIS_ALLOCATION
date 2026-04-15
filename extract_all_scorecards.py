import os
import yaml
import glob
import pandas as pd

def extract_nodes(vault_path, node_type, output_csv):
    path_pattern = os.path.join(vault_path, "Nodes", node_type, "*.md")
    active_nodes = []
    
    for filepath in glob.glob(path_pattern):
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
            if content.startswith('---'):
                end_idx = content.find('---', 3)
                if end_idx != -1:
                    frontmatter_str = content[3:end_idx]
                    try:
                        metadata = yaml.safe_load(frontmatter_str) or {}
                        
                        # Filter by active tags, but also include them if they're Departments/Suppliers and might not have tags
                        tags = metadata.get('tags', [])
                        
                        # Only strictly enforce 'active' tag for SKUs assuming Departments/Suppliers might always be active, 
                        # OR if they have 'active' in tags. Let's just check if it's active.
                        is_active = False
                        if isinstance(tags, list) and 'active' in tags:
                            is_active = True
                        elif isinstance(tags, str) and 'active' in tags:
                            is_active = True
                            
                        # Keep it simple: if filtering for active, let's just make sure active is in tags, 
                        # or if node_type isn't SKU and they might not use active tags, let's include them.
                        if is_active or node_type != "SKUs":
                            scorecard = {'name': os.path.basename(filepath).replace('.md', '')}
                            scorecard.update(metadata) # add all metadata fields
                            active_nodes.append(scorecard)
                            
                    except yaml.YAMLError:
                        continue
                        
    if active_nodes:
        df = pd.DataFrame(active_nodes)
        df.to_csv(output_csv, index=False)
        return len(df)
    return 0

if __name__ == "__main__":
    vault_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis_vault"
    
    # Extract all three
    print(f"SKUs extracted: {extract_nodes(vault_dir, 'SKUs', 'active_sku_scorecards.csv')}")
    print(f"Departments extracted: {extract_nodes(vault_dir, 'Departments', 'department_scorecards.csv')}")
    print(f"Suppliers extracted: {extract_nodes(vault_dir, 'Suppliers', 'supplier_scorecards.csv')}")
