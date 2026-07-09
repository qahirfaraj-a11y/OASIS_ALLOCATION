import pandas as pd
import os
import json

def analyze_uniques():
    export_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export"
    nodes_path = os.path.join(export_dir, "nodes.csv")
    edges_path = os.path.join(export_dir, "edges.csv")
    
    print("Loading nodes...")
    nodes_df = pd.read_csv(nodes_path)
    # Filter for SKUs only
    skus_df = nodes_df[nodes_df['type'] == 'SKU'].copy()
    all_skus = set(skus_df['id'].unique())
    print(f"Total SKUs found: {len(all_skus)}")
    
    print("Scanning edges for substitutions...")
    # Read edges and identify all SKUs that have at least one substitute
    has_substitutes = set()
    
    # Use chunking to be memory efficient even though 27MB is small
    for chunk in pd.read_csv(edges_path, chunksize=50000):
        subs = chunk[chunk['relation'] == 'substitution']
        has_substitutes.update(subs['source'].unique())
        has_substitutes.update(subs['target'].unique())
        
    print(f"SKUs with substitutes: {len(has_substitutes)}")
    
    # Set difference to find uniques
    unique_ids = all_skus - has_substitutes
    print(f"Unique SKUs identified: {len(unique_ids)}")
    
    # Filter metadata for uniques
    unique_skus_df = skus_df[skus_df['id'].isin(unique_ids)].copy()
    
    # Clean up department and supplier (remove [[ ]])
    unique_skus_df['department'] = unique_skus_df['department'].str.replace('[[', '', regex=False).str.replace(']]', '', regex=False)
    unique_skus_df['supplier'] = unique_skus_df['supplier'].str.replace('[[', '', regex=False).str.replace(']]', '', regex=False)
    
    # Calculate Moat Score: Velocity * Gross Profit (if available)
    # Some items might have profit 0 if they haven't sold yet, so fallback to velocity * price * default_margin
    unique_skus_df['moat_score'] = unique_skus_df['velocity_ads'] * unique_skus_df['price'] * (unique_skus_df['margin_pct'] / 100.0)
    
    # Sort by Moat Score
    unique_skus_df = unique_skus_df.sort_values(by='moat_score', ascending=False)
    
    # Save results
    output_path = "unique_skus_analysis.csv"
    unique_skus_df.to_csv(output_path, index=False)
    print(f"Analysis saved to {output_path}")
    
    # Summarize by Department
    dept_summary = unique_skus_df.groupby('department').agg({
        'id': 'count',
        'moat_score': 'sum'
    }).rename(columns={'id': 'sku_count'}).sort_values(by='moat_score', ascending=False)
    
    print("\nTop 5 Departments for Unique Moats:")
    print(dept_summary.head(5))

if __name__ == "__main__":
    analyze_uniques()
