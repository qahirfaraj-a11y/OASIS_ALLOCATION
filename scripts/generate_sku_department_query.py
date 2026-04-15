import os
import pandas as pd
import json
from tqdm import tqdm

# Paths
SCORECARD_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv"
EDGES_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
OUTPUT_FILE = r"C:\Users\iLink\.gemini\antigravity\scratch\OASIS_SKU_Relational_Master_Query.xlsx"

def generate_full_query():
    print("Initiating Deep SKU Relational Extraction (23.5k SKUs)...")
    
    # 1. Load Data
    print("   [1/4] Loading O.A.S.I.S. Scorecard & Neural Edges...")
    scorecard_df = pd.read_csv(SCORECARD_FILE)
    edges_df = pd.read_csv(EDGES_FILE)
    
    # 2. Extract Relational Maps
    print("   [2/4] Aggregating 415k Neural Relations (No Information Loss)...")
    
    # Group edges by source and relation type
    # We create unique columns for each relation type
    rel_map = edges_df.groupby(['source', 'relation'])['target'].apply(lambda x: '; '.join(map(str, x))).unstack()
    
    # Rename columns for clarity in Excel
    rel_map = rel_map.rename(columns={
        'link': 'Affinity_Links',
        'substitution': 'Discovered_Substitutes',
        'upstream_supply': 'Upstream_Supply_Nodes',
        'downstream_demand': 'Downstream_Demand_Nodes'
    })

    # Count connectivity density
    conn_count = edges_df.groupby('source').size().to_frame('Total_Neural_Connectivity')

    # 3. Join with Scorecard
    print("   [3/4] Merging financial metrics with relational intelligence...")
    
    # Clean scorecard for merge
    # We keep core operational columns
    cols_to_keep = [
        'Product', 'Department', 'Supplier', 'Total_Revenue', 'ABC_Class', 
        'Margin_Pct', 'Velocity_Tier', 'Capital_Required', 'Strategy_Role',
        'Logic_Trace'
    ]
    master_df = scorecard_df[cols_to_keep].copy()
    
    # Left join to ensure all 23.5k SKUs remain
    master_df = master_df.merge(rel_map, left_on='Product', right_index=True, how='left')
    master_df = master_df.merge(conn_count, left_on='Product', right_index=True, how='left')
    
    # Fill NAs for SKUs with no relations
    master_df['Total_Neural_Connectivity'] = master_df['Total_Neural_Connectivity'].fillna(0).astype(int)
    for col in ['Affinity_Links', 'Discovered_Substitutes', 'Upstream_Supply_Nodes', 'Downstream_Demand_Nodes']:
        if col in master_df.columns:
            master_df[col] = master_df[col].fillna("No direct links identified")

    # 4. Final Formatting & Excel Export
    print("   [4/4] Writing Master Intelligence Register to Excel...")
    
    # Sort: Primary group by Department, secondary by ABC Class importance, tertiary by Revenue
    master_df['ABC_Rank'] = master_df['ABC_Class'].map({'A': 0, 'B': 1, 'C': 2, 'D': 3}).fillna(4)
    master_df = master_df.sort_values(by=['Department', 'ABC_Rank', 'Total_Revenue'], ascending=[True, True, False])
    master_df = master_df.drop(columns=['ABC_Rank'])

    # Create Glossary Data
    glossary_data = {
        "Metric": [
            "ABC_Class", "Margin_Pct", "Velocity_Tier", "Capital_Required", 
            "Strategy_Role", "Logic_Trace", "Downstream_Demand_Nodes", 
            "Affinity_Links", "Discovered_Substitutes", "Upstream_Supply_Nodes", 
            "Total_Neural_Connectivity"
        ],
        "Definition": [
            "Inventory classification based on value and volume (A=High, B=Medium, C=Low) for prioritization.",
            "The percentage of revenue that exceeds COGS, indicating SKU-level profitability.",
            "Sales throughput speed (High/Med/Low) derived from historical transaction frequency mapping.",
            "Total liquidity tied up in the procurement and storage of the SKU at any given time.",
            "The GNN-assigned designation (e.g., Cash Cow, Star, Anchor) based on its role in the network.",
            "A diagnostic string showing the primary neural data points used for current allocation logic.",
            "Number of customer-facing or consumption nodes influenced by this SKU's availability.",
            "Count of cross-category relationships where this SKU serves as a purchase driver.",
            "Neural-mapped alternative SKUs that capture demand if this SKU is out-of-stock.",
            "Count of primary and secondary supplier nodes connected to this SKU via the supply chain.",
            "Aggregate degree of the SKU node within the O.A.S.I.S. global neural network graph."
        ]
    }
    glossary_df = pd.DataFrame(glossary_data)

    # Creating the Excel file with specific sheet name
    with pd.ExcelWriter(OUTPUT_FILE, engine='openpyxl') as writer:
        master_df.to_excel(writer, sheet_name='SKU_Relational_Audit', index=False)
        glossary_df.to_excel(writer, sheet_name='Metric_Definitions', index=False)
        
        # Access the workbook to apply basic styling
        workbook = writer.book
        
        # Style Sheet 1: SKU_Relational_Audit
        worksheet1 = writer.sheets['SKU_Relational_Audit']
        for i, col in enumerate(master_df.columns):
            column_letter = chr(65 + i) if i < 26 else f"A{chr(65 + (i-26))}" # Simple column letter logic
            worksheet1.column_dimensions[column_letter].width = 25

        # Style Sheet 2: Metric_Definitions
        worksheet2 = writer.sheets['Metric_Definitions']
        worksheet2.column_dimensions['A'].width = 30
        worksheet2.column_dimensions['B'].width = 80
        
        # Enable text wrapping for definitions
        from openpyxl.styles import Alignment
        for row in range(2, len(glossary_df) + 2):
            worksheet2[f'B{row}'].alignment = Alignment(wrapText=True)

    print(f"\nAudit Complete! Master Query generated at: {OUTPUT_FILE}")
    print(f"   Sheets included: SKU_Relational_Audit, Metric_Definitions")
    print(f"   Total SKUs processed: {len(master_df)}")

if __name__ == "__main__":
    generate_full_query()
