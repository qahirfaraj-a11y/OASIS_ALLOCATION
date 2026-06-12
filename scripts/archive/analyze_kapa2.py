import pandas as pd
import json

def extract_kapa_details():
    kapa_term = "KAPA"
    report_data = {}

    # 1. kapa.xlsx
    kapa_xlsx_path = r"C:\Users\iLink\Downloads\kapa.xlsx"
    df_kapa = pd.read_excel(kapa_xlsx_path, header=1) # Note: header might be row 1
    # Clean column names
    df_kapa.columns = [str(c).strip() for c in df_kapa.columns]
    report_data['kapa_excel_rows'] = len(df_kapa)
    
    # Let's just convert it to dict for top items
    if 'DESCRIPTION' in df_kapa.columns:
        report_data['kapa_sample_skus'] = df_kapa['DESCRIPTION'].dropna().head(10).tolist()

    # 2. neutral_network_export/nodes.csv
    nodes_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\nodes.csv"
    df_nodes = pd.read_csv(nodes_csv)
    kapa_nodes = df_nodes[df_nodes.astype(str).apply(lambda x: x.str.contains(kapa_term, case=False, na=False)).any(axis=1)]
    report_data['nodes'] = kapa_nodes.to_dict('records')

    # 3. neutral_network_export/edges.csv
    edges_csv = r"C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export\edges.csv"
    df_edges = pd.read_csv(edges_csv)
    kapa_edges = df_edges[df_edges.astype(str).apply(lambda x: x.str.contains(kapa_term, case=False, na=False)).any(axis=1)]
    report_data['edges_count'] = len(kapa_edges)
    
    # 4. Supplier_Fulfillment_Summary.xlsx
    supp_fulf = r"C:\Users\iLink\.gemini\antigravity\scratch\Supplier_Fulfillment_Summary.xlsx"
    df_supp = pd.read_excel(supp_fulf)
    kapa_supp = df_supp[df_supp.astype(str).apply(lambda x: x.str.contains(kapa_term, case=False, na=False)).any(axis=1)]
    report_data['fulfillment'] = kapa_supp.to_dict('records')

    with open('kapa_report_data.json', 'w', encoding='utf-8') as f:
        json.dump(report_data, f, indent=2, default=str)
        
    print("Exported to kapa_report_data.json")

if __name__ == "__main__":
    extract_kapa_details()
