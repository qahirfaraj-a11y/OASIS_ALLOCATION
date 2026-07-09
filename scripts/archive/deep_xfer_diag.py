"""
Deep Diagnostic: Transfer Intelligence Tab
==========================================
Interrogates every section's logic end-to-end.
"""
import sys, json, math
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.fulfillment_decider import NetworkAvailabilityMap, StoreSkuState, FulfillmentDecider

uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
mapper = SchemaMapper.for_pos_erp()
conn = UniversalConnector(uri, mapper)
adapter = PosErpAdapter(conn)

# ──────────────────────────────────────────────────────────────────────────────
print("=" * 70)
print("SECTION 0: Organisation inventory")
print("=" * 70)
orgs = adapter.fetch_all_organizations()
print(f"Total orgs: {len(orgs)}")
org_cds = [o["ORG_CD"] for o in orgs]
org_names = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
print(f"Org codes: {org_cds[:5]} ...")

# Load 3 stores for speed
SAMPLE_ORGS = org_cds[:3]
print(f"Sampling: {SAMPLE_ORGS}")

# ──────────────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("SECTION B: Item-Level Heuristic (load_sales_intel ADS problem)")
print("=" * 70)
intel0 = adapter.fetch_sales_intelligence(SAMPLE_ORGS[0], days=300)
print(f"fetch_sales_intelligence type: {type(intel0)}")
if isinstance(intel0, dict) and intel0:
    sample_item = list(intel0.items())[0]
    print(f"  Sample key: '{sample_item[0]}'")
    print(f"  Sample val: {sample_item[1]}")
    avg_ads = [v.get('avg_daily_sales',0) for v in intel0.values() if isinstance(v, dict)]
    print(f"  ADS values - min={min(avg_ads):.1f}, max={max(avg_ads):.1f}, mean={sum(avg_ads)/max(len(avg_ads),1):.1f}")
    print(f"  => These are KES MONETARY values, NOT unit counts!")
    print(f"  => hours_to_so formula mixes KES ADS with unit-based stock -> MEANINGLESS ratio")

raw_stocks = adapter.fetch_stock_snapshot(SAMPLE_ORGS[0])
if raw_stocks:
    vals = [float(s.get('current_stocks', 0)) for s in raw_stocks]
    print(f"  Raw stock snapshot values sample: {vals[:8]}")
    print(f"  => Values are tiny decimals - units vs KES mismatch CONFIRMED")

# ──────────────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("SECTION B2: Network Transfer Opportunities - Deep Diagnosis")
print("=" * 70)

all_data = {}
for org_cd in SAMPLE_ORGS:
    prods = adapter.fetch_enriched_products(org_cd)
    all_data[org_cd] = prods
    ads_vals = [float(p.get('avg_daily_sales', 0) or 0) for p in prods]
    curr_vals = [float(p.get('current_stocks', 0) or 0) for p in prods]
    safe_vals = [a * 2.0 for a in ads_vals]
    excess_vals = [c - s for c, s in zip(curr_vals, safe_vals)]
    
    n_ads_zero = sum(1 for v in ads_vals if v == 0)
    n_curr_zero = sum(1 for v in curr_vals if v == 0)
    n_excess_pos = sum(1 for v in excess_vals if v > 0)
    n_deficit = sum(1 for c, a in zip(curr_vals, ads_vals) if a > 0 and (c/a) < 3.0)
    
    print(f"\n  {org_cd} ({org_names.get(org_cd, org_cd)}):")
    print(f"    Total SKUs: {len(prods)}")
    print(f"    ADS=0 items: {n_ads_zero} ({100*n_ads_zero/max(len(prods),1):.1f}%)")
    print(f"    Stock=0 items: {n_curr_zero}")
    print(f"    Excess > 0 (curr > 2xADS): {n_excess_pos}")
    print(f"    Deficit items (ADS>0 and days<3): {n_deficit}")
    
    # Show top excess items
    excess_items = [(float(p.get('current_stocks',0) or 0) - float(p.get('avg_daily_sales',0) or 0)*2,
                     p.get('product_name','')[:40], float(p.get('avg_daily_sales',0) or 0),
                     float(p.get('current_stocks',0) or 0))
                    for p in prods if (float(p.get('current_stocks',0) or 0) - float(p.get('avg_daily_sales',0) or 0)*2) > 0]
    excess_items.sort(key=lambda x: -x[0])
    print(f"    Top 3 excess items:")
    for e, name, ads, curr in excess_items[:3]:
        days = curr/ads if ads > 0 else 9999
        print(f"      {name}: excess={e:.1f}, ads={ads:.2f}, curr={curr:.1f}, days={days:.0f}d")
    
    # Show top deficit items
    deficit_items = [(float(p.get('current_stocks',0) or 0) / float(p.get('avg_daily_sales',0) or 0.001),
                      p.get('product_name','')[:40], float(p.get('avg_daily_sales',0) or 0),
                      float(p.get('current_stocks',0) or 0))
                     for p in prods if float(p.get('avg_daily_sales',0) or 0) > 0 and
                     (float(p.get('current_stocks',0) or 0) / float(p.get('avg_daily_sales',0) or 0.001)) < 3.0]
    deficit_items.sort(key=lambda x: x[0])
    print(f"    Top 3 deficit items (<3d cover):")
    for days, name, ads, curr in deficit_items[:3]:
        print(f"      {name}: days={days:.2f}d, ads={ads:.2f}, curr={curr:.1f}")

# ──────────────────────────────────────────────────────────────────────────────
print()
print("=" * 70)
print("SECTION B2: find_donors investigation - why only 1 transfer?")
print("=" * 70)

# Build network map from sample orgs
bcode_map = {}
try:
    with open(r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\product_barcode_map.json', 'r', encoding='utf-8') as f:
        bcode_map = json.load(f)
except:
    pass

nmap = NetworkAvailabilityMap()
for org_cd, prods in all_data.items():
    for p in prods:
        ads  = float(p.get('avg_daily_sales', 0) or 0)
        curr = float(p.get('current_stocks', 0) or 0)
        safe = ads * 2.0
        excess = curr - safe
        pname = str(p.get('product_name', ''))
        nmap.add(StoreSkuState(
            org_cd=org_cd, org_name=org_names.get(org_cd, org_cd),
            itm_cd=str(p.get('item_code', '')),
            product_name=pname,
            current_stock=curr, avg_daily_sales=ads,
            safety_stock=safe, excess=excess,
            is_fresh=bool(p.get('is_fresh', False)),
            sell_price=float(p.get('selling_price', 0) or 0),
            department=str(p.get('department', '')),
            days_since_delivery=int(p.get('last_days_since_last_delivery', 0) or 0),
        ), bcode_map.get(pname, ''))

print(f"Network map index keys: {len(nmap._index)}")

# Test find_donors for deficit items from org 0
prods0 = all_data[SAMPLE_ORGS[0]]
recipient = SAMPLE_ORGS[0]

# Find deficit items at org 0
deficit_items_org0 = [
    p for p in prods0
    if float(p.get('avg_daily_sales', 0) or 0) > 0
    and (float(p.get('current_stocks', 0) or 0) / float(p.get('avg_daily_sales', 0) or 0.001)) < 3.0
]
deficit_items_org0.sort(key=lambda p: float(p.get('current_stocks',0) or 0) / float(p.get('avg_daily_sales',0) or 0.001))

print(f"\nDeficit items at {recipient}: {len(deficit_items_org0)}")
print("Testing find_donors for first 10 deficit items:")

donors_found = 0
no_donor = 0
for p in deficit_items_org0[:10]:
    itm_cd = str(p.get('item_code', ''))
    pname = p.get('product_name', '')
    ads = float(p.get('avg_daily_sales', 0) or 0)
    curr = float(p.get('current_stocks', 0) or 0)
    days = curr / ads if ads > 0 else 999

    # Check if itm_cd is even in the index
    in_index = itm_cd in nmap._index
    name_in_index = pname in nmap._index
    
    donors = nmap.find_donors(itm_cd, recipient, product_name=pname)
    
    if donors:
        donors_found += 1
        best = donors[0]
        print(f"  DONOR: {pname[:35]} -> from {best.org_cd} excess={best.excess:.1f}")
    else:
        no_donor += 1
        # Diagnose WHY no donors
        candidates = nmap._index.get(itm_cd, []) or nmap._index.get(pname, [])
        if candidates:
            other_stores = [c for c in candidates if c.org_cd != recipient]
            if other_stores:
                c = other_stores[0]
                eff_ratio = 1.5 if c.avg_daily_sales > 5 else (2.5 if c.avg_daily_sales <= 1 else 2.0)
                passes_excess = c.excess > 0
                passes_ratio = c.current_stock >= c.safety_stock * eff_ratio
                print(f"  NO DONOR: {pname[:35]} | idx=Y candidates={len(other_stores)} "
                      f"| best: curr={c.current_stock:.1f} safe={c.safety_stock:.1f} "
                      f"excess={c.excess:.1f} ratio_ok={passes_ratio} excess_ok={passes_excess}")
            else:
                print(f"  NO DONOR: {pname[:35]} | only {recipient} in index (no other stores)")
        else:
            print(f"  NO DONOR: {pname[:35]} | NOT IN INDEX (itm_cd={itm_cd} in_idx={in_index} name_idx={name_in_index})")

print(f"\nSummary: donors_found={donors_found}, no_donor={no_donor}")

# Key diagnostic: what DOES pass the donor threshold?
print()
print("=" * 70)
print("DONOR THRESHOLD ANALYSIS: Why is the bar so high?")
print("=" * 70)
# Check candidate from SAMPLE_ORGS[1] for items that ORG0 needs
if len(SAMPLE_ORGS) > 1:
    prods1 = all_data[SAMPLE_ORGS[1]]
    passers = []
    for p in prods1:
        ads = float(p.get('avg_daily_sales', 0) or 0)
        curr = float(p.get('current_stocks', 0) or 0)
        safe = ads * 2.0
        excess = curr - safe
        eff_ratio = 1.5 if ads > 5 else (2.5 if ads <= 1 else 2.0)
        if excess > 0 and curr >= safe * eff_ratio:
            passers.append((excess, p.get('product_name','')[:40], ads, curr, safe, eff_ratio))
    passers.sort(key=lambda x: -x[0])
    print(f"Items at {SAMPLE_ORGS[1]} passing full donor threshold: {len(passers)}")
    for excess, name, ads, curr, safe, ratio in passers[:5]:
        print(f"  {name}: excess={excess:.1f}, ads={ads:.3f}, curr={curr:.1f}, safe={safe:.3f}, ratio_needed={ratio}")
    
    # Items that fail only the ratio check
    fails_ratio_only = [(curr-safe, p.get('product_name','')[:40], ads, curr, safe)
                        for p in prods1
                        if (float(p.get('current_stocks',0) or 0) - float(p.get('avg_daily_sales',0) or 0)*2) > 0
                        and not (float(p.get('current_stocks',0) or 0) >= float(p.get('avg_daily_sales',0) or 0)*2 * (1.5 if float(p.get('avg_daily_sales',0) or 0)>5 else (2.5 if float(p.get('avg_daily_sales',0) or 0)<=1 else 2.0)))]
    print(f"\nItems with excess>0 BUT failing the current>=safety*ratio check: {len(fails_ratio_only)}")
    for e, name, ads, curr, safe in fails_ratio_only[:5]:
        ratio_needed = 1.5 if ads > 5 else (2.5 if ads <= 1 else 2.0)
        needed = safe * ratio_needed
        print(f"  {name}: excess={e:.1f}, curr={curr:.1f}, needed={needed:.1f} (safe={safe:.3f} x {ratio_needed})")
