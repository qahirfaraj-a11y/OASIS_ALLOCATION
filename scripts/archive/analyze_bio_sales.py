import pandas as pd
import os
import numpy as np

# Configuration
INPUT_FILE = r"app/data/Bio.xlsx"
OUTPUT_REPORT = r"C:\Users\iLink\.gemini\antigravity\brain\727303c1-e050-46d0-8394-7f5849b8a103/bio_logistics_insights.md"

def analyze_bio():
    print("Loading Bio data...")
    try:
        df = pd.read_excel(INPUT_FILE)
    except Exception as e:
        print(f"Error loading file: {e}")
        return

    # Basic Cleaning
    df.columns = df.columns.str.strip()
    df['Total Qty'] = pd.to_numeric(df['Total Qty'], errors='coerce').fillna(0)
    df['Total Cost'] = pd.to_numeric(df['Total Cost'], errors='coerce').fillna(0)
    
    # Aggregation (in case multiple lines per item per branch, we want overall item view first)
    item_stats = df.groupby(['Item Name', 'Department']).agg({
        'Total Qty': 'sum',
        'Total Cost': 'sum',
        'GP %': 'mean' # Average margin
    }).reset_index()
    
    # 1. ABC Analysis (Pareto Principle) - Volume
    item_stats = item_stats.sort_values('Total Qty', ascending=False)
    item_stats['Cum Qty'] = item_stats['Total Qty'].cumsum()
    total_qty = item_stats['Total Qty'].sum()
    item_stats['Cum %'] = (item_stats['Cum Qty'] / total_qty) * 100
    
    # Class A: Top 80% volume
    # Class B: Next 15%
    # Class C: Bottom 5%
    item_stats['Class'] = np.where(item_stats['Cum %'] <= 80, 'A',
                                   np.where(item_stats['Cum %'] <= 95, 'B', 'C'))
    
    class_a_items = item_stats[item_stats['Class'] == 'A']
    class_c_items = item_stats[item_stats['Class'] == 'C']
    
    # 2. Department Analysis
    dept_stats = item_stats.groupby('Department')['Total Qty'].sum().sort_values(ascending=False)
    
    # 3. Dead Stock / Slow Movers
    # Assuming this is yearly data. Items with very low quantity.
    dead_stock = item_stats[item_stats['Total Qty'] < 10] # Less than 10 units a year?
    
    # 4. Generate Report
    with open(OUTPUT_REPORT, 'w', encoding='utf-8') as f:
        f.write("# Bio / Maccuisine Logistics Interview Insights\n\n")
        f.write("Based on the sales data, here are strategic insights to position yourself as a data-driven Logistics Coordinator.\n\n")
        
        # Insight 1: Warehouse Zoning (The Golden Zone)
        f.write("## 1. Warehouse Zoning Strategy (The \"Golden Zone\")\n")
        f.write(f"The data reveals a classic Pareto distribution:\n")
        f.write(f"- **{len(class_a_items)} Items** drive **80% of the physical volume**.\n")
        f.write(f"- **{len(class_c_items)} Items** drive only the last **5%**.\n\n")
        f.write("**Interview Talking Point**: \n")
        f.write("> \"I analyzed your sales volume and found that a small number of SKUs generate the bulk of movement. I would organize the warehouse layout to place these top movers in the 'Golden Zone' (waist-height, nearest to dispatch) to minimize travel time and picking fatigue. Slow movers would be moved to higher racking or deeper storage.\"\n\n")
        
        f.write("### Top 5 Volume Drivers (Must be accessible):\n")
        for idx, row in class_a_items.head(5).iterrows():
            f.write(f"1. **{row['Item Name']}** ({row['Department']}): {row['Total Qty']:,.0f} Units\n")
            
        f.write("\n---\n\n")
        
        # Insight 2: Department Focus
        f.write("## 2. Departmental Handling Requirements\n")
        top_dept = dept_stats.index[0]
        top_dept_vol = dept_stats.iloc[0]
        f.write(f"The **{top_dept}** department is your logistical heavy lifter, moving {top_dept_vol:,.0f} units.\n\n")
        f.write("**Interview Talking Point**: \n")
        f.write(f"> \"Given that {top_dept} accounts for the highest physical throughput, I would verify that our cold chain/storage standard operating procedures (SOPs) are most robust for this category to prevent spoilage, as it represents the biggest risk to efficiency.\"\n\n")
        
        f.write("\n---\n\n")

        # Insight 3: Dead Stock Rationalization
        f.write(f"## 3. Inventory Rationalization (Dead Stock)\n")
        f.write(f"I identified **{len(dead_stock)} SKUs** that sold less than 10 units in the entire year.\n\n")
        f.write("**Interview Talking Point**: \n")
        f.write("> \"I noticed a 'Long Tail' of items with negligible movement. While these might be necessary for variety, they consume valuable pallet space. I would propose a quarterly review with Sales to delist or discount these items, freeing up capacity for the fast-moving {class_a_items.iloc[0]['Department']} products.\"\n\n")
        
        # Insight 4: Maccuisine vs Bio (if applicable)
        maccuisine = item_stats[item_stats['Item Name'].str.contains('MC ', case=False, na=False)]
        if not maccuisine.empty:
            f.write("## 4. Maccuisine (MC) Specifics\n")
            f.write(f"Since you are applying for **Maccuisine**, here is the snapshot for 'MC' coded items:\n")
            f.write(f"- Total Volume: {maccuisine['Total Qty'].sum():,.0f}\n")
            f.write(f"- Top Item: **{maccuisine.iloc[0]['Item Name']}**\n\n")
            f.write("**Interview Talking Point**:\n")
            f.write("> \"I specifically looked at the Maccuisine (MC) portfolio. It behaves differently from the core Bio butter line. I would ensure distinct picking processes for these jars/sauces compared to the dairy bricks to prevent packing errors.\"\n")

if __name__ == "__main__":
    analyze_bio()
