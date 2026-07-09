import pandas as pd
import os
import networkx as nx

def analyze_advanced_strategies():
    export_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export"
    nodes_path = os.path.join(export_dir, "nodes.csv")
    edges_path = os.path.join(export_dir, "edges.csv")
    
    print("Loading graph data...")
    nodes_df = pd.read_csv(nodes_path)
    edges_df = pd.read_csv(edges_path)
    
    # Pre-process nodes for fast lookup
    sku_metadata = nodes_df[nodes_df['type'] == 'SKU'].set_index('id')
    
    # 1. STRATEGY: BACKSTOP ANCHORS (Hub Detection)
    print("Identifying Backstop Anchors...")
    sub_edges = edges_df[edges_df['relation'] == 'substitution']
    
    # Count in-degree for targets (fallback potential)
    hub_counts = sub_edges['target'].value_counts().reset_index()
    hub_counts.columns = ['id', 'backstop_count']
    
    # Join with metadata
    backstops = hub_counts.merge(sku_metadata, on='id')
    # Filter for SKU type
    backstops = backstops[backstops['id'].isin(sku_metadata.index)]
    
    # Sort by backstop potential
    backstops = backstops.sort_values(by='backstop_count', ascending=False)
    backstops.to_csv("backstop_anchors_analysis.csv", index=False)
    
    # 2. STRATEGY: SUPPLIER FRAGILITY (Risk Mapping)
    print("Mapping Supplier Fragility...")
    # Find items where substitutes come from only 1 supplier
    fragility_data = []
    
    # Iterate through SKUs with at least one substitute
    all_targets = sub_edges['source'].unique()
    for sku in all_targets:
        if sku not in sku_metadata.index: continue
        
        subs = sub_edges[sub_edges['source'] == sku]['target'].tolist()
        sub_suppliers = set()
        for s in subs:
            if s in sku_metadata.index:
                sub_suppliers.add(sku_metadata.loc[s, 'supplier'])
        
        if len(sub_suppliers) == 1:
            fragility_data.append({
                'id': sku,
                'substitute_count': len(subs),
                'monopoly_supplier': list(sub_suppliers)[0],
                'ads': sku_metadata.loc[sku, 'velocity_ads']
            })
            
    fragility_df = pd.DataFrame(fragility_data).sort_values(by='ads', ascending=False)
    fragility_df.to_csv("supplier_fragility_map.csv", index=False)
    
    # 3. STRATEGY: COMMODITIZATION INDEX (Private Label Targeting)
    print("Calculating Commoditization Index...")
    # Density of category substitution
    depts = sku_metadata['department'].unique()
    commod_index = []
    
    for dept in depts:
        skus_in_dept = sku_metadata[sku_metadata['department'] == dept].index.tolist()
        if len(skus_in_dept) < 5: continue # Skip small categories
        
        # Count internal substitution edges
        internal_edges = sub_edges[
            sub_edges['source'].isin(skus_in_dept) & 
            sub_edges['target'].isin(skus_in_dept)
        ]
        
        # Density = Edges / Nodes
        # Higher density = more interchangeable
        density = len(internal_edges) / len(skus_in_dept)
        
        commod_index.append({
            'department': dept,
            'sku_count': len(skus_in_dept),
            'substitution_density': density,
            'total_velocity': sku_metadata[sku_metadata['department'] == dept]['velocity_ads'].sum()
        })
        
    commod_df = pd.DataFrame(commod_index).sort_values(by='substitution_density', ascending=False)
    commod_df.to_csv("private_label_index.csv", index=False)
    
    print("Advanced Strategy Analysis Complete.")

if __name__ == "__main__":
    analyze_advanced_strategies()
