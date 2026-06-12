import json
import pandas as pd
import os

# Define file paths
supplier_patterns_path = r"C:\Users\iLink\.gemini\antigravity\scratch\app\data\supplier_patterns_2025 (3).json"
market_master_path = r"C:\Users\iLink\.gemini\antigravity\scratch\market_competitiveness_master.xlsx"

# Competitors to analyze (Distributor/Vendor Names)
target_suppliers = [
    "HAYAT KIMYA  K  H PRODUCTS LTD",
    "HASBAH KENYA LTD P AND G",  # P&G
    "KIM FAY EAST AFRICA LTD",   # often handles Huggies/KC or similar segments
    "CHANDARIA INDUSTRIES LTD HYGIENE", # Local giant
    "D P L FESTIVE BREAD LTD", # Benchmark for high freq if needed, or maybe just stick to hygiene
    "AFRICAN COTTON INDUSTRIES LTD" # Hygiene competitor
]

# 1. Analyze Operational Metrics (Supplier Patterns)
print("--- Operational Metrics (Supplier Patterns) ---")
try:
    with open(supplier_patterns_path, 'r') as f:
        patterns = json.load(f)
        
    metrics_data = []
    for supplier in target_suppliers:
        # Fuzzy matching or exact match check
        match = None
        if supplier in patterns:
            match = supplier
        else:
            # Try to find partial match
            for k in patterns.keys():
                if supplier.split()[0] in k and supplier.split()[1] in k:
                   match = k
                   break
        
        if match:
            data = patterns[match]
            metrics_data.append({
                "Supplier": supplier, # Use our label
                "Matched_Name": match,
                "Reliability": data.get('reliability_score'),
                "Order_Freq": data.get('order_frequency'),
                "Lead_Time": data.get('estimated_delivery_days'),
                "Avg_Order_Val": data.get('avg_order_value_kes')
            })
        else:
            print(f"Warning: Could not find pattern data for {supplier}")

    df_ops = pd.DataFrame(metrics_data)
    print(df_ops.to_string())

except Exception as e:
    print(f"Error reading supplier patterns: {e}")

# 2. Analyze Market Share (Excel)
print("\n--- Market Share (Excel Master) ---")
try:
    df_market = pd.read_excel(market_master_path, sheet_name='Vendor Performance')
    # Filter for our targets
    # Simple keyword filter
    keywords = ["HAYAT", "HASBAH", "KIM FAY", "CHANDARIA", "AFRICAN COTTON"]
    
    filtered_market = df_market[df_market['VENDOR_NAME'].apply(lambda x: any(k in str(x).upper() for k in keywords))]
    print(filtered_market[['VENDOR_NAME', 'Volume_Share_%', 'Revenue_Share_%', 'Avg_Price']].to_string())

except Exception as e:
    print(f"Error reading market master: {e}")
