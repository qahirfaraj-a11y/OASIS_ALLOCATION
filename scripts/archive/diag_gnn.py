import sys, os
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')
from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
conn = UniversalConnector(uri, SchemaMapper.for_pos_erp())
adapter = PosErpAdapter(conn)

stocks = adapter.fetch_stock_snapshot('ORG001')
enriched = adapter.fetch_enriched_products('ORG001')

raw_with_ads = sum(1 for s in stocks if float(s.get('avg_daily_sales', 0)) > 0)
raw_so = sum(1 for s in stocks if float(s.get('current_stocks', 0)) <= 0)
print(f'Raw stock snapshot items: {len(stocks)}')
print(f'Raw with avg_daily_sales > 0: {raw_with_ads}')
print(f'Raw with current_stocks <= 0: {raw_so}')
print(f'Raw stock sample fields: {list(stocks[0].keys()) if stocks else "NONE"}')

enriched_ads_map = {p.get('item_code',''): float(p.get('avg_daily_sales',0) or 0) for p in enriched}
print(f'Enriched ADS > 0: {sum(1 for v in enriched_ads_map.values() if v > 0)}')

so_count_after_injection = 0
for s in stocks:
    ic = s.get('item_code', '')
    curr = float(s.get('current_stocks', 0))
    ads = enriched_ads_map.get(ic, 0)
    if curr <= 0 and ads > 0:
        so_count_after_injection += 1
print(f'After ADS injection: items with stock<=0 AND ads>0: {so_count_after_injection}')
print(f'=> This is so_count used by GNN risk. If 0, inv_risk=0.')

print()
try:
    import pickle, gzip, torch
    cache_path = os.path.join(r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data', 'st_gat_intel_cache.pkl.gz')
    with gzip.open(cache_path, 'rb') as f:
        gat_data = pickle.load(f)
    
    from oasis.logic.st_gat_sim import StGatNetworkSim
    from oasis.logic.gnn_model import InventoryGNN
    
    sim = StGatNetworkSim(gat_data)
    model = InventoryGNN(sim.feature_dim)
    model.eval()
    
    x_t = sim.get_feature_matrix()
    with torch.no_grad():
        out = model(x_t, sim.edge_index)
    
    risks = out['risk'].squeeze().tolist()
    if not isinstance(risks, list): risks = [risks]
    print(f'GNN raw risk scores ({len(risks)} stores):')
    for i, r in enumerate(risks):
        sid = sim.stores_data[i].get('store_id', f'store_{i}')
        print(f'  {sid}: {r:.6f}')
    
    if len(set(round(r, 4) for r in risks)) == 1:
        print('  => ALL IDENTICAL! GNN model not trained or features are identical.')
    else:
        print(f'  => Range: {min(risks):.6f} - {max(risks):.6f}')
except Exception as e:
    print(f'GNN check error: {e}')
    import traceback
    traceback.print_exc()
