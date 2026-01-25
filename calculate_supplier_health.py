import json
import pandas as pd

file_path = r"C:\Users\iLink\.gemini\antigravity\scratch\app\data\supplier_patterns_2025 (3).json"

target_suppliers = [
    "HAYAT KIMYA  K  H PRODUCTS LTD",
    "HASBAH KENYA LTD P AND G",
    "KIM FAY EAST AFRICA LTD",
    "CHANDARIA INDUSTRIES LTD HYGIENE",
    "BABY BRANDS DIRECTS LTD" # Niche competitor
]

try:
    with open(file_path, 'r') as f:
        data = json.load(f)

    health_metrics = []

    for supplier in target_suppliers:
        # Fuzzy match logic again just in case
        match_key = None
        if supplier in data:
            match_key = supplier
        else:
             for k in data.keys():
                if supplier.split()[0] in k and supplier.split()[1] in k:
                   match_key = k
                   break
        
        if match_key:
            s_data = data[match_key]
            lead_time = s_data.get('estimated_delivery_days', 0)
            avg_gap = s_data.get('avg_gap_days', 1) # avoid div by zero
            
            # Stress Index Calculation
            # Index > 1.0 means you wait longer for goods than the time between your orders.
            # (i.e., you have multiple orders "on the water" at once).
            stress_index = lead_time / avg_gap if avg_gap > 0 else 0
            
            health_metrics.append({
                "Supplier": supplier,
                "Lead_Time": lead_time,
                "Ordering_Gap_Days": avg_gap,
                "Stress_Index": round(stress_index, 2),
                "Reliability": s_data.get('reliability_score'),
                "Total_Orders": s_data.get('total_orders_2025')
            })

    df = pd.DataFrame(health_metrics)
    print("--- Supplier Health & Logistics Stress Scorecard ---")
    print(df.to_string())

except Exception as e:
    print(f"Error: {e}")
