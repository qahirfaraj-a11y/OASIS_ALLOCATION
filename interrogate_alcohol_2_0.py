import pandas as pd
import json
import os
import numpy as np

def convert_types(obj):
    if isinstance(obj, np.integer):
        return int(obj)
    elif isinstance(obj, np.floating):
        return float(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {k: convert_types(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [convert_types(i) for i in obj]
    return obj

# Paths
ANALYSIS_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\retail_analyses"
EDGES_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
SCORECARD_FILE = r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"

def get_substitutes(sku_name, edges_df):
    """Interrogates neural network for direct substitution links."""
    subs = edges_df[(edges_df['relation'] == 'substitute') & (edges_df['source'] == sku_name)]
    return subs['target'].tolist()[:3]

def get_halo_partners(sku_name, edges_df):
    """Interrogates neural network for cross-category halo partners."""
    halos = edges_df[(edges_df['relation'] == 'link') & (edges_df['source'] == sku_name)]
    return halos['target'].tolist()[:3]

def run_deep_interrogation_2_0():
    print("Beginning Deep Masterclass 2.0 Interrogation...")
    
    # 1. Load Master Scorecard for Advanced Supply Chain Metrics
    sc_df = pd.read_csv(SCORECARD_FILE)
    
    # 2. Section Deep Dives (Beer, Wine, Spirit, Cider)
    results = {}
    for sector in ['BEER', 'WINES', 'SPIRITS', 'CIDERS']:
        print(f"Interrogating {sector} Section...")
        path = os.path.join(ANALYSIS_DIR, f"OASIS_{sector}_Masterclass.xlsx")
        df = pd.read_excel(path)
        
        # Pareto A/B/C calculation
        total_rev = df['Revenue (KES)'].sum()
        df = df.sort_values('Revenue (KES)', ascending=False)
        df['CumRev'] = df['Revenue (KES)'].cumsum() / total_rev
        
        a_class = df[df['CumRev'] <= 0.7] # Top 70% Revenue
        b_class = df[(df['CumRev'] > 0.7) & (df['CumRev'] <= 0.9)] # Next 20%
        c_class = df[df['CumRev'] > 0.9] # Final 10% (The Long Tail)
        
        # Sector Metrics
        results[sector] = {
            "rev_total": total_rev,
            "sku_count": len(df),
            "a_class_count": len(a_class),
            "c_class_count": len(c_class),
            "c_class_rev_pct": (c_class['Revenue (KES)'].sum() / total_rev) * 100,
            "velocity_leaders": a_class.head(10).to_dict('records')
        }
        
    # 3. Neural Interrogation (Substitutions & Halo)
    print("Mapping Neural Infrastructure...")
    edges_df = pd.read_csv(EDGES_FILE)
    
    # We take the top 5 Alcohol Anchors (Beer/Wine) for substitution shielding
    top_anchors = results['BEER']['velocity_leaders'][:3] + results['WINES']['velocity_leaders'][:2]
    substitution_map = {}
    for anchor in top_anchors:
        name = anchor['SKU Name']
        substitution_map[name] = {
            "substitutes": get_substitutes(name, edges_df),
            "halo": get_halo_partners(name, edges_df)
        }
    
    # 4. Supplier Resilience Map
    print("Auditing Supply Chain Resilience...")
    # Get top 5 Alcohol Suppliers and their reliability
    alc_scorecard = sc_df[sc_df['Department'].isin(['BEER', 'WINES', 'SPIRITS', 'CIDERS'])]
    supplier_perf = alc_scorecard.groupby('Supplier').agg({
        'Supplier_Reliability': 'mean',
        'Lead_Time_Days': 'mean',
        'Total_Revenue': 'sum'
    }).sort_values('Total_Revenue', ascending=False).head(8)
    
    # Convert to native types for JSON serialization
    supplier_perf_dict = supplier_perf.astype(float).to_dict('index')
    
    # 5. Final Synthesis
    data = {
        "sector_analyses": results,
        "substitution_shield": substitution_map,
        "supplier_audit": supplier_perf_dict,
        "timestamp": str(pd.Timestamp.now())
    }
    
    with open('alcohol_masterclass_2_0_intel.json', 'w') as f:
        json.dump(convert_types(data), f, indent=4)
    
    print("Masterclass 2.0 Intelligence Synthesized: alcohol_masterclass_2_0_intel.json")

if __name__ == "__main__":
    run_deep_interrogation_2_0()
