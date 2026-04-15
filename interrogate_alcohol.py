import pandas as pd
import json
import os
import re

# Paths
ALCOHOL_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\filtered_queries"
EDGES_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"

def get_top_skus(dept):
    try:
        path = os.path.join(ALCOHOL_DIR, f"OASIS_{dept}_Analysis.xlsx")
        df = pd.read_excel(path)
        return df.nlargest(10, 'Revenue')
    except:
        return pd.DataFrame()

def get_halo_links(sku_list):
    try:
        edges_df = pd.read_csv(EDGES_FILE)
        # Filter for links where source is in our SKU list
        links = edges_df[(edges_df['source'].isin(sku_list)) & (edges_df['relation'] == 'link')]
        # Get target counts to see common basket buddies
        target_counts = links['target'].value_counts().head(10)
        return target_counts
    except:
        return {}

def run_alcohol_interrogation():
    print("Interrogating Alcohol Section Data...")
    
    # 1. Pareto Analysis
    beer_top = get_top_skus("BEER")
    wine_top = get_top_skus("WINES")
    spirit_top = get_top_skus("SPIRITS")
    cider_top = get_top_skus("CIDERS")
    
    print(f"Top Beer: {len(beer_top)} items")
    print(f"Top Wine: {len(wine_top)} items")
    
    # 2. Halo Interrogation (Deep Neural Search)
    anchor_skus = list(beer_top['SKU Name']) + list(wine_top['SKU Name']) + list(spirit_top['SKU Name'])
    halo_targets = get_halo_links(anchor_skus)
    
    # 3. Inventory Bloat Check
    summary_path = os.path.join(ALCOHOL_DIR, "OASIS_ALL_DEPARTMENTS_SUMMARY.xlsx")
    summary_df = pd.read_excel(summary_path)
    wine_summary = summary_df[summary_df['Department'] == 'WINES'].iloc[0]
    beer_summary = summary_df[summary_df['Department'] == 'BEER'].iloc[0]
    
    # Output temporary JSON for report generation
    intelligence = {
        "beer_pareto": beer_top.to_dict('records'),
        "wine_pareto": wine_top.to_dict('records'),
        "spirit_pareto": spirit_top.to_dict('records'),
        "cider_pareto": cider_top.to_dict('records'),
        "halo_affinities": halo_targets.to_dict(),
        "wine_metrics": wine_summary.to_dict(),
        "beer_metrics": beer_summary.to_dict()
    }
    
    with open('alcohol_intel.json', 'w') as f:
        json.dump(intelligence, f, indent=4)
        
    print("Alcohol Intelligence Exported: alcohol_intel.json")

if __name__ == "__main__":
    run_alcohol_interrogation()
