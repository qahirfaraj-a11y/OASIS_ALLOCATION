import os
import re
import yaml
import json
import pandas as pd

VAULT_ROOT = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis_vault\Nodes"
EXPORT_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export"

def sanitize_link(link):
    return link.replace('[[', '').replace(']]', '').strip()

def parse_markdown_file(filepath, node_type):
    with open(filepath, 'r', encoding='utf-8') as f:
        content = f.read()
        
    # Extract Frontmatter
    fm_match = re.search(r'^---\n(.*?)\n---', content, re.DOTALL)
    metadata = {}
    if fm_match:
        try:
            metadata = yaml.safe_load(fm_match.group(1))
        except:
            pass
            
    node_id = os.path.basename(filepath).replace('.md', '')
    metadata['id'] = node_id
    metadata['type'] = node_type
    
    # Extract all [link] relationships
    # 1. Standard [[wiki-links]]
    wiki_links = re.findall(r'\[\[(.*?)\]\]', content)
    
    # 2. Dataview-style [key]:: [[link]]
    dv_links = re.findall(r'\[(.*?)\]:: \[\[(.*?)\]\]', content)
    
    edges = []
    for link in wiki_links:
        edges.append({'source': node_id, 'target': sanitize_link(link), 'relation': 'link'})
        
    for key, link in dv_links:
        edges.append({'source': node_id, 'target': sanitize_link(link), 'relation': key})
        
    return metadata, edges

def run_export():
    if not os.path.exists(EXPORT_DIR):
        os.makedirs(EXPORT_DIR)
        
    all_nodes = []
    all_edges = []
    
    subdirs = {
        'SKUs': 'SKU',
        'Departments': 'Department',
        'Suppliers': 'Supplier'
    }
    
    for subdir, ntype in subdirs.items():
        path = os.path.join(VAULT_ROOT, subdir)
        if not os.path.exists(path):
            continue
            
        print(f"Exporting {subdir}...")
        files = [f for f in os.listdir(path) if f.endswith('.md')]
        for filename in files:
            metadata, edges = parse_markdown_file(os.path.join(path, filename), ntype)
            all_nodes.append(metadata)
            all_edges.extend(edges)
            
    # Save CSVs
    nodes_df = pd.DataFrame(all_nodes)
    edges_df = pd.DataFrame(all_edges)
    
    nodes_df.to_csv(os.path.join(EXPORT_DIR, 'nodes.csv'), index=False)
    edges_df.to_csv(os.path.join(EXPORT_DIR, 'edges.csv'), index=False)
    
    # Save JSON for visualization (Gephi/D3)
    graph_json = {
        'nodes': all_nodes,
        'links': all_edges
    }
    with open(os.path.join(EXPORT_DIR, 'full_graph.json'), 'w') as f:
        json.dump(graph_json, f, indent=4)
        
    print(f"Export Complete!")
    print(f"Nodes: {len(all_nodes)}")
    print(f"Edges: {len(all_edges)}")
    print(f"Saved to: {EXPORT_DIR}")

if __name__ == "__main__":
    run_export()
