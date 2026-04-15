import os
import yaml
import glob
import pandas as pd

def extract_active_nodes(vault_path):
    # Paths to the different node types
    sku_path = os.path.join(vault_path, "Nodes", "SKUs", "*.md")
    
    active_skus = []
    
    # 1. Extract SKU Scorecards
    for filepath in glob.glob(sku_path):
        with open(filepath, 'r', encoding='utf-8') as f:
            content = f.read()
            
            # Extract YAML frontmatter
            if content.startswith('---'):
                end_idx = content.find('---', 3)
                if end_idx != -1:
                    frontmatter_str = content[3:end_idx]
                    try:
                        metadata = yaml.safe_load(frontmatter_str) or {}
                        
                        # Filter for active nodes
                        tags = metadata.get('tags', [])
                        if 'active' in tags:
                            scorecard = {
                                'sku_name': os.path.basename(filepath).replace('.md', ''),
                                'department': metadata.get('department'),
                                'supplier': metadata.get('supplier'),
                                'price': metadata.get('price'),
                                'margin': metadata.get('margin'),
                                'revenue': metadata.get('revenue'),
                                'gross_profit': metadata.get('gross_profit'),
                                'velocity_ads': metadata.get('velocity_ads'),
                            }
                            active_skus.append(scorecard)
                    except yaml.YAMLError:
                        continue
                        
    # Convert to DataFrame for easy viewing/export
    df_active_skus = pd.DataFrame(active_skus)
    
    return df_active_skus

if __name__ == "__main__":
    vault_dir = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis_vault"
    
    print("Extracting Active SKUs...")
    active_df = extract_active_nodes(vault_dir)
    print(f"Found {len(active_df)} active SKUs.")
    
    if not active_df.empty:
        # Save to CSV as the scorecard
        output_csv = "active_sku_scorecards.csv"
        active_df.to_csv(output_csv, index=False)
        print(f"Saved scorecards to {output_csv}")
        print("\nSample Data:")
        print(active_df.head())
