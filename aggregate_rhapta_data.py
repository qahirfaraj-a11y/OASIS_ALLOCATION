import os
import json

INTELLIGENCE_JSON = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data\sales_profitability_intelligence_2025_updated.json"

def aggregate_rhapta_data():
    rhapta_metrics = {}
    
    # Assuming the intelligence file covers a 12-month period (2024 or 2025 full year)
    # 265k Brookside pouches / 365 = ~727 units/day (Highly realistic for Rhapta)
    DAYS = 365
    
    print("Loading high-fidelity intelligence master...")
    with open(INTELLIGENCE_JSON, 'r') as f:
        intelligence_data = json.load(f)
        
    print("Mastering SKU metrics...")
    for sku, data in intelligence_data.items():
        qty = data.get('total_qty_sold', 0)
        revenue = data.get('revenue', 0)
        margin = data.get('margin_pct', 0)
        rank = data.get('sales_rank', 9999)
        
        rhapta_metrics[sku] = {
            'live_qty': round(qty, 2),
            'live_revenue': round(revenue, 2),
            'live_margin': round(margin, 2),
            'live_ads': round(qty / DAYS, 4),
            'sales_rank': rank,
            'source': 'Profitability Intelligence (2025)'
        }
        
    # No fallback to other sources needed if this is the master file
    with open('rhapta_master_metrics.json', 'w') as f:
        json.dump(rhapta_metrics, f, indent=4)
        
    print(f"Aggregation complete. Mastered {len(rhapta_metrics)} SKUs from Intelligence Master.")

if __name__ == "__main__":
    aggregate_rhapta_data()
