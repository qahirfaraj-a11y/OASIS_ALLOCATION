import pandas as pd
import os

def audit():
    export_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export"
    nodes_path = os.path.join(export_dir, "nodes.csv")
    edges_path = os.path.join(export_dir, "edges.csv")
    
    print("Loading nodes...")
    nodes_df = pd.read_csv(nodes_path)
    node_ids = set(nodes_df['id'].unique())
    print(f"Total Unique Node IDs: {len(node_ids)}")
    
    print("Loading edges...")
    edge_ids = set()
    for chunk in pd.read_csv(edges_path, chunksize=100000):
        edge_ids.update(chunk['source'].unique())
        edge_ids.update(chunk['target'].unique())
    print(f"Total Unique IDs in Edges: {len(edge_ids)}")
    
    intersection = node_ids.intersection(edge_ids)
    print(f"Intersection Size: {len(intersection)}")
    
    missing_from_edges = node_ids - edge_ids
    print(f"Node IDs missing from Edges: {len(missing_from_edges)}")
    
    print("\nSample missing IDs:")
    for m in list(missing_from_edges)[:15]:
        print(f" - {m}")

    # Special check for Kensalt
    print("\nKensalt Check:")
    kensalts_node = [n for n in node_ids if "KENSALT" in str(n)]
    kensalts_edge = [e for e in edge_ids if "KENSALT" in str(e)]
    print(f"Kensalt nodes: {kensalts_node[:5]}")
    print(f"Kensalt edges: {kensalts_edge[:5]}")

if __name__ == "__main__":
    audit()
