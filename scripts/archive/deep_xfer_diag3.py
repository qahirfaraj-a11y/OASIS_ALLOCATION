"""
DEEP DIAGNOSTIC: Transfer Intelligence Module — All Sub-Sections
================================================================
Investigates:
  0. Live Simulation — why always "No urgent transfers"
  A. ST-GAT — why risk scores uniform
  B. Heuristic — why "No stockouts" despite 437 MOQ failures
  B2. Network Transfer — fractional units investigation
"""
import sys, json, os, math
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')

DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
mapper = SchemaMapper.for_pos_erp()
conn = UniversalConnector(uri, mapper)
adapter = PosErpAdapter(conn)

orgs = adapter.fetch_all_organizations()
org_cds = [o["ORG_CD"] for o in orgs]
org_names = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}

# =====================================================================
print("=" * 70)
print("SECTION 0: LIVE SIMULATION — Why always 'No urgent transfers'")
print("=" * 70)
# Check if IntraDaySimulator exists and what it requires
try:
    from intraday_sim import IntraDaySimulator
    print(f"IntraDaySimulator class found: {IntraDaySimulator}")
    import inspect
    init_sig = inspect.signature(IntraDaySimulator.__init__)
    print(f"__init__ signature: {init_sig}")
    
    # Check advance_to_hour method
    if hasattr(IntraDaySimulator, 'advance_to_hour'):
        adv_sig = inspect.signature(IntraDaySimulator.advance_to_hour)
        print(f"advance_to_hour signature: {adv_sig}")
    
    # Try to instantiate with minimal args
    print("\nAttempting to instantiate IntraDaySimulator...")
    try:
        sim = IntraDaySimulator(adapter, org_cds[0])
        print(f"  Created successfully for {org_cds[0]}")
        state = sim.advance_to_hour(10)
        print(f"  advance_to_hour(10) returned type: {type(state)}")
        if state:
            if isinstance(state, dict):
                print(f"  Keys: {list(state.keys())[:10]}")
                for k, v in list(state.items())[:3]:
                    print(f"    {k}: {v}")
            elif isinstance(state, list):
                print(f"  Length: {len(state)}")
                if state:
                    print(f"  First item: {state[0]}")
        else:
            print(f"  STATE IS FALSY: {repr(state)}")
            print("  => This is why 'if _sim_state:' is always False!")
    except Exception as e:
        print(f"  Instantiation error: {e}")
        import traceback
        traceback.print_exc()
except ImportError as e:
    print(f"IntraDaySimulator not importable: {e}")

# =====================================================================
print("\n" + "=" * 70)
print("SECTION A: ST-GAT — Why are risk scores uniform?")
print("=" * 70)
import pickle, gzip

cache_path = os.path.join(DATA_DIR, 'st_gat_intel_cache.pkl.gz')
try:
    with gzip.open(cache_path, 'rb') as f:
        gat_data = pickle.load(f)
    print(f"Cache type: {type(gat_data)}, items: {len(gat_data)}")
    
    if isinstance(gat_data, list) and gat_data:
        # Check if it's per-store or global
        sample = gat_data[0]
        if isinstance(sample, dict):
            print(f"First item keys: {list(sample.keys())[:15]}")
            # Check if there's an org/store field
            store_fields = [k for k in sample.keys() if 'org' in k.lower() or 'store' in k.lower()]
            print(f"Store-related fields: {store_fields}")
except Exception as e:
    print(f"Error loading GNN cache: {e}")

# Check the get_all_store_risks function
print("\nAnalyzing get_all_store_risks logic in ops_dashboard.py...")
with open(r'C:\Users\iLink\.gemini\antigravity\scratch\ops_dashboard.py', 'r', encoding='utf-8') as f:
    src = f.read()

# Find get_all_store_risks
import re
match = re.search(r'def get_all_store_risks.*?(?=\ndef |\nclass )', src, re.DOTALL)
if match:
    func_text = match.group()
    lines = func_text.split('\n')
    print(f"Function is {len(lines)} lines long")
    for line in lines[:30]:
        print(f"  {line}")
    if len(lines) > 30:
        print(f"  ... ({len(lines) - 30} more lines)")

# =====================================================================
print("\n" + "=" * 70)
print("SECTION B: Heuristic — Why always 'No stockouts'?")
print("=" * 70)
# The fix used load_network_stock. Let's check what it returns
# and whether the hours_to_so math is producing meaningful results

print("Testing the actual math with live data...")
enriched = adapter.fetch_enriched_products(org_cds[0])
print(f"Enriched products for {org_cds[0]}: {len(enriched)}")

sim_hour = 10  # typical midday
hours_remaining = max(1, 22 - sim_hour)
print(f"sim_hour={sim_hour}, hours_remaining={hours_remaining}")

at_risk = []
ads_zero = 0
for p in enriched:
    name = p.get('product_name', '')
    qty = float(p.get('current_stocks', 0) or 0)
    ads = float(p.get('avg_daily_sales', 0) or 0)
    if ads > 0:
        hours_to_so = (qty / ads) * 16  # days * 16 trading hours
        if qty <= 0 or hours_to_so <= hours_remaining:
            at_risk.append({
                'name': name[:40],
                'qty': qty,
                'ads': ads,
                'hours_to_so': round(hours_to_so, 2),
                'days_cover': round(qty / ads, 2)
            })
    else:
        ads_zero += 1

print(f"\nADS=0 items: {ads_zero}")
print(f"Items with ADS > 0: {len(enriched) - ads_zero}")
print(f"Items at risk (hours_to_so <= {hours_remaining}): {len(at_risk)}")

if at_risk:
    print("\nSample at-risk items:")
    for item in sorted(at_risk, key=lambda x: x['hours_to_so'])[:10]:
        print(f"  {item['name']}: qty={item['qty']:.1f}, ads={item['ads']:.2f}, "
              f"hrs_to_so={item['hours_to_so']}, days={item['days_cover']}")
else:
    print("\n*** NO ITEMS AT RISK! ***")
    # Investigate why
    sample_items = [(float(p.get('current_stocks',0) or 0), 
                     float(p.get('avg_daily_sales',0) or 0),
                     p.get('product_name','')[:40])
                    for p in enriched if float(p.get('avg_daily_sales',0) or 0) > 0]
    sample_items.sort(key=lambda x: (x[0] / max(x[1], 0.001)))
    print("\nLowest days-cover items with ADS>0:")
    for qty, ads, name in sample_items[:15]:
        days_cover = qty / ads if ads > 0 else 999
        hours_to_so = days_cover * 16
        print(f"  {name}: qty={qty:.1f}, ads={ads:.2f}, days={days_cover:.2f}, hours={hours_to_so:.1f} (vs remaining={hours_remaining})")

# =====================================================================
print("\n" + "=" * 70)
print("SECTION B2: Network Transfer — Fractional units investigation")
print("=" * 70)

# Check what UOM (unit of measure) these items use
fractional_items = []
integer_items = []
for p in enriched:
    curr = float(p.get('current_stocks', 0) or 0)
    uom = p.get('uom', '')
    name = p.get('product_name', '')
    dept = p.get('department', '')
    if curr > 0 and curr != int(curr):
        fractional_items.append({
            'name': name[:40], 'stock': curr, 'uom': uom, 'dept': dept,
            'ads': float(p.get('avg_daily_sales', 0) or 0)
        })
    elif curr > 0:
        integer_items.append({'name': name[:40], 'stock': curr, 'uom': uom})

print(f"Items with fractional stock: {len(fractional_items)}")
print(f"Items with integer stock: {len(integer_items)}")

# Check UOM distribution
from collections import Counter
uom_frac = Counter(i['uom'] for i in fractional_items)
uom_int = Counter(i['uom'] for i in integer_items)
print(f"\nFractional items UOM distribution: {dict(uom_frac.most_common(10))}")
print(f"Integer items UOM distribution: {dict(uom_int.most_common(10))}")

# Are fractional items sold by weight (KG) or by piece?
print("\nSample fractional stock items:")
for item in sorted(fractional_items, key=lambda x: x['stock'])[:10]:
    print(f"  {item['name']}: stock={item['stock']:.4f}, uom={item['uom']}, dept={item['dept']}")
print("\nSample high-value fractional items:")
for item in sorted(fractional_items, key=lambda x: -x['stock'])[:5]:
    print(f"  {item['name']}: stock={item['stock']:.4f}, uom={item['uom']}, dept={item['dept']}")

# Check transfer qty generation — are we producing fractional transfer quantities?
print("\n\nChecking what transfer quantities the push logic would generate...")
# Items with excess > 0 and ADS > 0
excess_items = []
for p in enriched:
    ads = float(p.get('avg_daily_sales', 0) or 0)
    curr = float(p.get('current_stocks', 0) or 0)
    uom = p.get('uom', '')
    safe = ads * 2.0
    exc = curr - safe
    if exc > 0 and curr != int(curr):
        excess_items.append({
            'name': p.get('product_name','')[:40],
            'stock': curr, 'ads': ads, 'excess': exc, 'uom': uom,
            'xfer_40pct': round(exc * 0.4, 4),
        })
excess_items.sort(key=lambda x: -x['excess'])
print(f"\nFractional-stock items with excess > 0: {len(excess_items)}")
print("Top 10 would-be transfer quantities:")
for e in excess_items[:10]:
    print(f"  {e['name']}: excess={e['excess']:.2f}, xfer_40%={e['xfer_40pct']:.4f}, uom={e['uom']}")

# =====================================================================
print("\n" + "=" * 70)
print("MOQ FAILURE CROSS-CHECK: 437 items failed MOQ in Smart Ordering")
print("=" * 70)
# These items have demand but can't be ordered because qty < MOQ.
# They SHOULD show as transfer opportunities if another store has them.
# Let's check the order engine output for MOQ failures
try:
    from oasis.logic.order_engine_v8 import OrderEngine
    print("OrderEngine imported successfully")
    
    # Check if there's a recent order output or log
    output_dir = os.path.join(DATA_DIR, 'outputs')
    if os.path.exists(output_dir):
        files = os.listdir(output_dir)
        print(f"Output files: {len(files)}")
        for f in sorted(files)[-5:]:
            print(f"  {f}")
except Exception as e:
    print(f"OrderEngine check: {e}")

# Check how many items have ADS > 0 but very low stock across ALL stores
print("\nCross-store deficit analysis:")
all_enriched = {}
for oc in org_cds:
    all_enriched[oc] = adapter.fetch_enriched_products(oc)

# Find items that are low everywhere
item_coverage = {}
for oc, prods in all_enriched.items():
    for p in prods:
        ic = p.get('item_code', '')
        ads = float(p.get('avg_daily_sales', 0) or 0)
        curr = float(p.get('current_stocks', 0) or 0)
        dc = (curr / ads) if ads > 0 else (0 if curr < 1 else 999)
        if ic not in item_coverage:
            item_coverage[ic] = {'name': p.get('product_name','')[:40], 'stores': {}}
        item_coverage[ic]['stores'][oc] = {'days': round(dc, 2), 'curr': curr, 'ads': ads}

# Items where ALL stores have < 7 days cover AND ADS > 0 at ANY store
network_deficits = []
for ic, data in item_coverage.items():
    stores = data['stores']
    all_low = all(s['days'] < 7 for s in stores.values() if s['ads'] > 0)
    any_demand = any(s['ads'] > 0 for s in stores.values())
    if all_low and any_demand:
        network_deficits.append(data)

print(f"Items with ALL stores below 7 days cover (and ADS > 0 at any store): {len(network_deficits)}")
print("Top 10:")
for nd in network_deficits[:10]:
    stores_str = ", ".join(f"{oc}:{s['days']}d" for oc, s in nd['stores'].items() if s['ads'] > 0)
    print(f"  {nd['name']}: {stores_str}")

print("\n" + "=" * 70)
print("SUMMARY")
print("=" * 70)
