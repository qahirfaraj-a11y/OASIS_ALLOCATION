import json

def verify():
    filepath = 'app/data/sales_profitability_intelligence_2025_reclassified.json'
    try:
        with open(filepath, 'r') as f:
            data = json.load(f)
    except FileNotFoundError:
        print(f"File not found: {filepath}")
        return

    unknowns = [k for k, v in data.items() if v.get('category') == 'unknown']
    reclassified = [(k, v) for k, v in data.items() if 'reclassification_method' in v]

    print(f"Total items: {len(data)}")
    print(f"Remaining unknowns: {len(unknowns)}")
    if unknowns:
        print("\nItems still unknown:")
        for k in unknowns:
            print(f"- {k}")

    print(f"\nTotal reclassified: {len(reclassified)}")
    print("\nSample reclassified items:")
    for k, v in reclassified[:20]:
        print(f"- {k}: {v.get('category')} (via {v.get('reclassification_method')})")

if __name__ == "__main__":
    verify()
