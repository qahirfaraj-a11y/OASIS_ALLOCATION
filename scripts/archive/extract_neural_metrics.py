import pandas as pd
import os
import networkx as nx

def extract_neural_metrics():
    export_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export"
    nodes_path = os.path.join(export_dir, "nodes.csv")
    edges_path = os.path.join(export_dir, "edges.csv")
    
    print("Loading graph data...")
    nodes_df = pd.read_csv(nodes_path)
    edges_df = pd.read_csv(edges_path)
    
    # Filter for SKUs
    sku_nodes = nodes_df[nodes_df['type'] == 'SKU']
    sku_metadata = sku_nodes.set_index('id')
    sku_ids = set(sku_metadata.index)
    
    print(f"Loaded {len(sku_ids)} SKUs. Building relation maps...")
    
    # 1. Map Substitutes (Top 5)
    print("Extracting Substitutes...")
    sub_edges = edges_df[edges_df['relation'] == 'substitution']
    sub_map = {}
    for _, row in sub_edges.iterrows():
        source = row['source']
        target = row['target']
        if source in sku_ids and target in sku_ids:
            if source not in sub_map: sub_map[source] = []
            if len(sub_map[source]) < 5:
                # Use SKU ID as label since 'label' column is missing
                sub_map[source].append(target)

    # 2. Map Neural Affinities (Top 10 Links)
    print("Extracting Neural Affinities (Links)...")
    link_edges = edges_df[edges_df['relation'] == 'link']
    link_map = {}
    for _, row in link_edges.iterrows():
        source = row['source']
        target = row['target']
        if source in sku_ids and target in sku_ids:
            if source not in link_map: link_map[source] = []
            if len(link_map[source]) < 10:
                link_map[source].append(target)
        
        # Bi-directional for links
        if target in sku_ids and source in sku_ids:
            if target not in link_map: link_map[target] = []
            if len(link_map[target]) < 10:
                link_map[target].append(source)

    # 3. Calculate Global Relation Count (Centrality)
    print("Calculating Relation Density...")
    combined_edges = edges_df[edges_df['relation'].isin(['substitution', 'link'])]
    source_counts = combined_edges['source'].value_counts()
    target_counts = combined_edges['target'].value_counts()
    
    # Total Degree = Source + Target counts
    total_counts = source_counts.add(target_counts, fill_value=0)
    
    print("Consolidating Metrics...")
    results = []
    for sku_id in sku_ids:
        # Clean up metadata (strip [[ ]])
        dept = str(sku_metadata.loc[sku_id, 'department']).replace("[[", "").replace("]]", "")
        results.append({
            'id': sku_id,
            'Item_Name': sku_id,
            'Department': dept,
            'ADS': sku_metadata.loc[sku_id, 'velocity_ads'],
            'Top_5_Substitutes': ", ".join(sub_map.get(sku_id, [])),
            'Top_10_Neural_Affinities': ", ".join(link_map.get(sku_id, [])),
            'Total_Relation_Count': int(total_counts.get(sku_id, 0))
        })
        
    metrics_df = pd.DataFrame(results)
    
    # Export all metrics
    metrics_df.to_csv("neural_sku_metrics_full.csv", index=False)
    
    # Filter for Top 1,000 for document generation (By Velocity * Density)
    metrics_df['impact_score'] = metrics_df['ADS'] * (metrics_df['Total_Relation_Count'] + 1)
    top_1000 = metrics_df.sort_values(by='impact_score', ascending=False).head(1000)
    top_1000.to_csv("neural_sku_metrics_top1000.csv", index=False)
    
    print("Metrics Extraction Complete. Files: neural_sku_metrics_full.csv, neural_sku_metrics_top1000.csv")

if __name__ == "__main__":
    extract_neural_metrics()
