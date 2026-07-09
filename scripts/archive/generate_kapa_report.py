import json

def generate_markdown_report():
    with open('kapa_report_data.json', 'r', encoding='utf-8') as f:
        data = json.load(f)

    report = ["# Kapa Oil Refineries - Full SKU Intelligence Report\n"]
    
    report.append("## Executive Summary")
    report.append(f"- **Total Raw SKUs (Excel Catalog):** {data.get('kapa_excel_rows', 0)}")
    report.append(f"- **Active Network Nodes (Graph):** {len(data.get('nodes', []))}")
    report.append(f"- **Network Edges (Relationships):** {data.get('edges_count', 0)}")
    
    fulfillment = data.get('fulfillment', [])
    if fulfillment:
        f = fulfillment[0]
        report.append(f"- **Supplier Fulfillment (Within 3 Days):** {f.get('% Within 3 Days', 'N/A')}%")
        report.append(f"- **Max Delivery Delay:** {f.get('max', 'N/A')} Days")
        report.append(f"- **Order Count Recorded:** {f.get('count', 'N/A')} POs")
    
    report.append("\n## Category Breakdown (from Graph Data)")
    
    departments = {}
    total_revenue = 0
    total_gp = 0
    top_velocity_skus = []
    
    for node in data.get('nodes', []):
        dept = str(node.get('department', 'UNKNOWN')).replace('[[', '').replace(']]', '')
        departments[dept] = departments.get(dept, 0) + 1
        total_revenue += float(node.get('revenue', 0.0) or 0.0)
        total_gp += float(node.get('gross_profit', 0.0) or 0.0)
        
        # Track velocity
        top_velocity_skus.append({
            'sku': node.get('id'),
            'velocity': float(node.get('velocity_ads', 0.0) or 0.0),
            'dept': dept
        })
        
    report.append("| Department | SKU Count |")
    report.append("|------------|-----------|")
    for dept, count in sorted(departments.items(), key=lambda x: x[1], reverse=True):
        report.append(f"| {dept} | {count} |")
        
    report.append("\n## Financial Snapshot")
    report.append(f"- **Total 30D Revenue (Sampled):** {total_revenue:,.2f} KES")
    report.append(f"- **Total 30D Gross Profit:** {total_gp:,.2f} KES")
    
    report.append("\n## Top 15 Highest Velocity SKUs")
    report.append("| SKU | Department | Daily Velocity (ADS) |")
    report.append("|-----|------------|----------------------|")
    top_velocity_skus.sort(key=lambda x: x['velocity'], reverse=True)
    for item in top_velocity_skus[:15]:
        report.append(f"| {item['sku']} | {item['dept']} | {item['velocity']:.2f} |")
        
    report.append("\n## Sample Excel Catalog Extraction")
    report.append("```text")
    for sample in data.get('kapa_sample_skus', []):
        report.append(f"- {sample}")
    report.append("```")

    report.append("\n## Strategic Insights")
    report.append("> [!WARNING]")
    report.append("> **Fulfillment Concern**: Only ~30% of orders are fulfilled within 3 days, with maximum delays extending up to 11 days. This necessitates higher safety stocks for Kapa products (lead time + buffer).")
    report.append("")
    report.append("> [!TIP]")
    report.append("> **Assortment Width**: With 110 active SKUs across categories ranging from Cooking Oil to Toilet Roll and Bath Soaps, Kapa serves as a mega-anchor supplier.")

    with open('kapa_sku_intelligence_report.md', 'w', encoding='utf-8') as f:
        f.write('\n'.join(report))

if __name__ == "__main__":
    generate_markdown_report()
