import pandas as pd
import json

def analyze_kapa_deep():
    # 1. Fulfillment Anomaly > 7 Days
    print("--- 1. PO FULFILLMENT ANOMALY (>7 Days) ---")
    detail_file = r"C:\Users\iLink\.gemini\antigravity\scratch\All_Suppliers_Fulfillment_Detail.xlsx"
    anomalies_data = []
    try:
        df = pd.read_excel(detail_file)
        kapa_mask = df['Vendor Name'].astype(str).str.contains('KAPA', case=False, na=False)
        kapa_df = df[kapa_mask]
        anomalies = kapa_df[kapa_df['Fulfillment Days'] > 7]
        print(f"Total Kapa Lines: {len(kapa_df)}, Anomalies > 7 days: {len(anomalies)}")
        for _, row in anomalies.iterrows():
            anomalies_data.append({
                'po': row['PO No'],
                'po_date': str(row['PO Date']),
                'grn': row['GRN No'],
                'grn_date': str(row['GRN Date']),
                'days': row['Fulfillment Days'],
                'item': row['Item Name']
            })
    except Exception as e:
        print(f"Error reading detail xlsx: {e}")

    # 2. Network Metrics (ROI, Connectors, Attractors)
    print("\n--- 2. NETWORK METRICS ---")
    nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
    edges_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
    
    skus_data = []
    try:
        df_nodes = pd.read_csv(nodes_csv)
        df_edges = pd.read_csv(edges_csv)
        
        # Calculate In-Degree (Attractors) and Out-Degree (Connectors)
        in_degree = df_edges['target'].value_counts().to_dict()
        out_degree = df_edges['source'].value_counts().to_dict()
        
        kapa_nodes = df_nodes[df_nodes.astype(str).apply(lambda x: x.str.contains('KAPA', case=False, na=False)).any(axis=1)]
        
        for _, row in kapa_nodes.iterrows():
            sku_id = row['id']
            # ROI approximation: GP / Revenue or just margin
            rev = float(row.get('revenue', 0))
            gp = float(row.get('gross_profit', 0))
            roi = (gp / rev * 100) if rev > 0 else 0
            margin = float(row.get('margin_pct', 0))
            if roi == 0 and margin > 0:
                roi = margin
                
            in_d = in_degree.get(sku_id, 0)
            out_d = out_degree.get(sku_id, 0)
            
            skus_data.append({
                'sku': sku_id,
                'department': str(row.get('department', '')).replace('[[','').replace(']]',''),
                'roi': roi,
                'revenue': rev,
                'attractor_score': in_d,
                'connector_score': out_d,
                'total_connections': in_d + out_d
            })
            
    except Exception as e:
        print(f"Error calculating network metrics: {e}")

    with open('kapa_deep_dive.json', 'w') as f:
        json.dump({'anomalies': anomalies_data, 'skus': skus_data}, f, indent=2)

    print("Data exported to kapa_deep_dive.json")

if __name__ == "__main__":
    analyze_kapa_deep()
