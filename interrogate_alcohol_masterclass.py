import pandas as pd
import json
import os
import re

# Paths
ANALYSIS_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\retail_analyses"
EDGES_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"

def run_alcohol_masterclass_interrogation():
    print("Beginning Deep Masterclass Interrogation...")
    
    # Load Master Summary
    master_df = pd.read_excel(os.path.join(ANALYSIS_DIR, "OASIS_RETAIL_SUMMARY_MASTER.xlsx"))
    
    # 1. Pareto Leaders (BEER)
    beer_df = pd.read_excel(os.path.join(ANALYSIS_DIR, "OASIS_BEER_Masterclass.xlsx"))
    beer_top_10 = beer_df.nlargest(10, 'Revenue (KES)').to_dict('records')
    
    # 2. Inventory Bloat (WINES)
    wine_df = pd.read_excel(os.path.join(ANALYSIS_DIR, "OASIS_WINES_Masterclass.xlsx"))
    wine_top_10 = wine_df.nlargest(10, 'Revenue (KES)').to_dict('records')
    # Count SKU's where velocity is near zero
    dead_stock_threshold = 0.05
    dead_skus = wine_df[wine_df['Velocity (ADS)'] < dead_stock_threshold]
    dead_percentage = len(dead_skus) / len(wine_df) * 100
    
    # 3. Spirits & Ciders
    spirit_df = pd.read_excel(os.path.join(ANALYSIS_DIR, "OASIS_SPIRITS_Masterclass.xlsx"))
    cider_df = pd.read_excel(os.path.join(ANALYSIS_DIR, "OASIS_CIDERS_Masterclass.xlsx"))
    
    # 4. Neural Affinity (Halo Interrogation)
    # We take the top Beer and Spirit names and find their strongest basket buddies
    top_sku_names = [s['SKU Name'] for s in beer_top_10[:5]]
    print(f"Investigating Halo links for {top_sku_names}...")
    
    try:
        edges_df = pd.read_csv(EDGES_FILE)
        halo_links = edges_df[(edges_df['relation'] == 'link') & (edges_df['source'].isin(top_sku_names))]
        affinity_summary = halo_targets = halo_links['target'].value_counts().head(8).to_dict()
    except Exception as e:
        print(f"Neural Error: {e}")
        affinity_summary = {}

    # 5. Synthesis
    intel = {
        "summary": {
            "total_beers": len(beer_df),
            "total_wines": len(wine_df),
            "total_spirits": len(spirit_df),
            "wine_dead_stock": len(dead_skus),
            "wine_dead_pct": round(dead_percentage, 1),
            "top_beer_revenue": beer_df['Revenue (KES)'].sum()
        },
        "beer_pareto": beer_top_10,
        "wine_pareto": wine_top_10,
        "halo_partners": affinity_summary,
        "spirit_insights": spirit_df.nlargest(5, 'Revenue (KES)').to_dict('records')
    }
    
    with open('alcohol_masterclass_intel.json', 'w') as f:
        json.dump(intel, f, indent=4)
        
    print("Intelligence synthesized in alcohol_masterclass_intel.json")

if __name__ == "__main__":
    run_alcohol_masterclass_interrogation()
