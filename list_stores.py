import json
with open('stores_network.json', 'r', encoding='utf-8') as f:
    data = json.load(f)
stores = data['stores']
print(f'Total stores: {len(stores)}')
for s in stores:
    n_skus = len(s.get('stock_profile', []))
    sid = s['store_id']
    name = s['name'][:35]
    dsf = s.get('demand_scale_factor', 1.0)
    print(f"{sid:10s} | {name:35s} | dsf={dsf:.1f} | SKUs_in_json={n_skus:,}")
