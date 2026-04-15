import json

data = json.load(open('rhapta_demo_preloaded.json'))

print('=== CATALOG (Tab 1) ===')
cat = data.get('catalog', {})
print('  SKUs:', cat.get('total_skus_scanned', 0))
print('  Capital Tied:', cat.get('total_capital_tied', 0))
print('  Dead Stock Value:', cat.get('dead_stock_value', 0))
print('  Dead Stock Count:', cat.get('dead_stock_count', 0))
print('  Ghost Demand Value:', cat.get('ghost_demand_value', 0))
print('  Ghost Demand Count:', cat.get('ghost_demand_count', 0))

print()
print('=== SUPPLIERS (Tab 2) ===')
sup = data.get('suppliers', {})
print('  Total Suppliers:', sup.get('total_suppliers', 'MISSING'))
print('  Criminal Count:', sup.get('criminal_count', 'MISSING'))
sup_list = sup.get('supplier_list', [])
print('  Supplier List Length:', len(sup_list))
if sup_list:
    for s in sup_list[:5]:
        print('   ', s)

print()
print('=== NETWORK (Tab 3) ===')
net = data.get('network', {})
print('  Shrink Events:', net.get('shrink_events', 'MISSING'))
print('  Transfer Events:', net.get('transfer_events', 'MISSING'))
print('  Entropy Cost:', net.get('entropy_cost_est', 'MISSING'))
