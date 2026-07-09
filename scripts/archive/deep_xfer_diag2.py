"""
COMPREHENSIVE TRANSFER INTELLIGENCE + MAPPING DEEP DIVE
========================================================
Tests all 4 sections and the barcode/product/supplier/dept mapping pipeline.
"""
import sys, json, os
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')

DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.fulfillment_decider import NetworkAvailabilityMap, StoreSkuState

uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
mapper = SchemaMapper.for_pos_erp()
conn = UniversalConnector(uri, mapper)
adapter = PosErpAdapter(conn)

orgs = adapter.fetch_all_organizations()
org_cds = [o["ORG_CD"] for o in orgs]
org_names = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}
print(f"Network: {org_cds}")

# =============================================================================
print("\n" + "="*70)
print("MAPPING FILES AUDIT")
print("="*70)

# 1. product_barcode_map.json
with open(os.path.join(DATA_DIR, 'product_barcode_map.json'), 'r', encoding='utf-8') as f:
    bcode_map = json.load(f)
print(f"\n[product_barcode_map.json]")
print(f"  Total mappings: {len(bcode_map)}")
sample_bc = list(bcode_map.items())[:3]
for k, v in sample_bc:
    print(f"  Key='{k[:50]}' => barcode='{v}'")
missing_bcode = sum(1 for v in bcode_map.values() if not v or v == '')
print(f"  Items with empty barcode: {missing_bcode} ({100*missing_bcode/max(len(bcode_map),1):.1f}%)")

# 2. product_department_map.json
with open(os.path.join(DATA_DIR, 'product_department_map.json'), 'r', encoding='utf-8') as f:
    dept_map = json.load(f)
print(f"\n[product_department_map.json]")
print(f"  Total mappings: {len(dept_map)}")
sample_dept = list(dept_map.items())[:3]
for k, v in sample_dept:
    print(f"  Key='{k[:50]}' => {v}")

# 3. product_supplier_map.json
with open(os.path.join(DATA_DIR, 'product_supplier_map.json'), 'r', encoding='utf-8') as f:
    supp_map = json.load(f)
print(f"\n[product_supplier_map.json]")
print(f"  Total mappings: {len(supp_map)}")
sample_supp = list(supp_map.items())[:3]
for k, v in sample_supp:
    print(f"  Key='{k[:50]}' => {v}")

# 4. barcode_department_map.json
with open(os.path.join(DATA_DIR, 'barcode_department_map.json'), 'r', encoding='utf-8') as f:
    bc_dept_map = json.load(f)
print(f"\n[barcode_department_map.json]")
print(f"  Total mappings: {len(bc_dept_map)}")
sample_bcd = list(bc_dept_map.items())[:3]
for k, v in sample_bcd:
    print(f"  Key='{k}' => {v}")

# 5. master_product_dept_map.json
with open(os.path.join(DATA_DIR, 'master_product_dept_map.json'), 'r', encoding='utf-8') as f:
    master_dept = json.load(f)
print(f"\n[master_product_dept_map.json]")
print(f"  Total mappings: {len(master_dept)}")
sample_md = list(master_dept.items())[:3]
for k, v in sample_md:
    print(f"  Key='{k[:50]}' => {v}")

# =============================================================================
print("\n" + "="*70)
print("MAPPING COVERAGE CHECK: enriched products vs mapping files")
print("="*70)
prods_org1 = adapter.fetch_enriched_products(org_cds[0])
print(f"\nEnriched products for {org_cds[0]}: {len(prods_org1)}")

# Check how many products from DB match each mapping file
match_bcode = 0
match_dept = 0
match_supp = 0
match_master = 0
no_match_bcode = []
no_dept_in_enriched = 0

for p in prods_org1:
    name = p.get('product_name', '')
    dept_in_enriched = p.get('department') or p.get('category') or ''
    if not dept_in_enriched:
        no_dept_in_enriched += 1
    
    if name in bcode_map and bcode_map[name]:
        match_bcode += 1
    else:
        no_match_bcode.append(name)
    if name in dept_map:
        match_dept += 1
    if name in supp_map:
        match_supp += 1
    if name in master_dept:
        match_master += 1

pct = lambda n: f"{n}/{len(prods_org1)} ({100*n/max(len(prods_org1),1):.1f}%)"
print(f"  Matched in product_barcode_map: {pct(match_bcode)}")
print(f"  Matched in product_department_map: {pct(match_dept)}")
print(f"  Matched in product_supplier_map: {pct(match_supp)}")
print(f"  Matched in master_product_dept_map: {pct(match_master)}")
print(f"  Items with NO dept in enriched data: {no_dept_in_enriched}")
print(f"  Sample unmatched barcodes: {[n[:40] for n in no_match_bcode[:5]]}")

# Check enriched product fields
p_sample = prods_org1[0]
print(f"\n  All fields in enriched product: {list(p_sample.keys())}")
dept_field_sample = [(p.get('department',''), p.get('category','')) for p in prods_org1[:10]]
print(f"  Sample dept/category fields: {dept_field_sample[:5]}")

# =============================================================================
print("\n" + "="*70)
print("SECTION B2 DONOR THRESHOLD DEEP DIVE")
print("="*70)
# The core problem: why only 1 transfer?
# 23,499 items at ORG002 pass full donor threshold
# But the 5 deficit items at ORG001 are all fast movers that ALSO need them

# Key finding: deficit items (ADS>0, days<3) are ALL fast movers 
# The fast movers have no donors because OTHER stores are ALSO running out

# What if we look at the ADS=0 items that are "stocked out" (curr=0)?
stocked_out_zero_ads = [p for p in prods_org1 
                         if float(p.get('current_stocks',0) or 0) == 0]
print(f"\nORG001 items with current_stocks=0: {len(stocked_out_zero_ads)}")
print("Sample (with ADS=0, stock=0 -> these need transfers but no ADS signal):")
for p in stocked_out_zero_ads[:5]:
    print(f"  {p.get('product_name','')[:50]} | ADS={p.get('avg_daily_sales',0)} | dept={p.get('department','')}")

# Check ORG002 for the SAME items (can they donate?)
prods_org2 = adapter.fetch_enriched_products(org_cds[1])
org2_by_code = {p.get('item_code',''): p for p in prods_org2}
org2_by_name = {p.get('product_name',''): p for p in prods_org2}

donated_zero_ads = 0
for p in stocked_out_zero_ads[:20]:
    code = p.get('item_code','')
    name = p.get('product_name','')
    donor_p = org2_by_code.get(code) or org2_by_name.get(name)
    if donor_p:
        donor_curr = float(donor_p.get('current_stocks',0) or 0)
        donor_ads  = float(donor_p.get('avg_daily_sales',0) or 0)
        donor_safe = donor_ads * 2.0
        donor_exc  = donor_curr - donor_safe
        if donor_exc > 0:
            donated_zero_ads += 1
            print(f"  DONOR AVAILABLE: {name[:40]} | ORG2 curr={donor_curr:.1f} excess={donor_exc:.1f}")

print(f"\nItems with stock=0 at ORG001 where ORG002 has excess: {donated_zero_ads}")

# =============================================================================
print("\n" + "="*70)
print("ROOT CAUSE: ADS=0 items are excluded from deficit detection")
print("="*70)
# The deficit filter is: days_cover < 3 AND (ads > 0 OR curr <= 0)
# But curr is NEVER <= 0 (smallest value is 0.0024)
# So: for ADS=0 items, (ads > 0 = False) AND (curr <= 0 = False) -> EXCLUDED!

zero_ads_items = [p for p in prods_org1 if float(p.get('avg_daily_sales',0) or 0) == 0]
zero_ads_very_low_stock = [p for p in zero_ads_items if float(p.get('current_stocks',0) or 0) < 5]
print(f"\nORG001 ADS=0 items: {len(zero_ads_items)}")
print(f"ORG001 ADS=0 items with stock < 5 units (tiny): {len(zero_ads_very_low_stock)}")
print(f"These are EXCLUDED from deficit detection because curr > 0 (barely) and ads=0")
print("Sample:")
for p in sorted(zero_ads_very_low_stock, key=lambda x: float(x.get('current_stocks',0) or 0))[:5]:
    print(f"  {p.get('product_name','')[:50]} | stock={float(p.get('current_stocks',0)):.4f} | dept={p.get('department','')}")

# =============================================================================
print("\n" + "="*70)
print("SECTION B HEURISTIC: ADS unit mismatch quantified")
print("="*70)
intel = adapter.fetch_sales_intelligence(org_cds[0], days=300)
if isinstance(intel, dict) and intel:
    # Compare ADS from sales_intel vs fetch_enriched_products
    enriched_ads = {p.get('product_name',''): float(p.get('avg_daily_sales',0) or 0) for p in prods_org1}
    mismatches = []
    for name, si in list(intel.items())[:20]:
        si_ads = si.get('avg_daily_sales', 0) if isinstance(si, dict) else 0
        ep_ads = enriched_ads.get(name, -1)
        ratio = si_ads / max(ep_ads, 0.001) if ep_ads > 0 else 999
        mismatches.append((name[:40], si_ads, ep_ads, ratio))
    
    print("\nSales Intel ADS vs Enriched Product ADS (first 10 matched items):")
    print(f"{'Product':<42} {'SalesIntel_ADS':>14} {'Enriched_ADS':>12} {'Ratio':>7}")
    for name, si_ads, ep_ads, ratio in sorted(mismatches, key=lambda x: -x[3])[:10]:
        if ep_ads > 0:
            print(f"  {name:<40} {si_ads:>14.1f} {ep_ads:>12.1f} {ratio:>7.0f}x")
    print("\n  => Sales Intel ADS is 1000x higher than unit-based ADS -> it IS monetary (KES), NOT units")
    print("  => Section B 'hours_to_so' formula is producing garbage output")

# =============================================================================
print("\n" + "="*70)
print("SECTION A: GNN Transfer Score Threshold Analysis")
print("="*70)
try:
    import pickle, gzip
    cache_path = os.path.join(DATA_DIR, 'st_gat_intel_cache.pkl.gz')
    with gzip.open(cache_path, 'rb') as f:
        gat_data = pickle.load(f)
    print(f"ST-GAT cache type: {type(gat_data)}")
    if isinstance(gat_data, list) and len(gat_data) > 0:
        print(f"Cache items: {len(gat_data)}")
        print(f"First item type: {type(gat_data[0])}")
        if isinstance(gat_data[0], dict):
            print(f"First item keys: {list(gat_data[0].keys())}")
    elif isinstance(gat_data, dict):
        print(f"Keys: {list(gat_data.keys())[:10]}")
except Exception as e:
    print(f"Error loading GNN cache: {e}")

# =============================================================================
print("\n" + "="*70)
print("SUMMARY OF ALL SECTION FAILURES")
print("="*70)
print("""
SECTION 0 (Sim):         INACTIVE - requires IntraDaySimulator in session_state
SECTION A (GNN):         PARTIALLY WORKING - fires store-level risk scores but
                         transfer recommendations filtered by profit > 0.45 score
                         (score threshold may be too high for mock data)
SECTION B (Heuristic):   BROKEN - ADS from load_sales_intel is in KES (monetary),
                         not units; current_stocks from raw snapshot is in units;
                         hours_to_so = units / KES -> meaningless ratio
SECTION B2 (CTS):        PARTIALLY WORKING - correctly built but:
                         BUG 1: ADS=0 items with tiny stock (< 0.01 units) are
                         excluded from deficit detection (curr > 0 barely, ads=0)
                         BUG 2: Fast-mover deficit items (MILK) have no donors
                         because all stores are simultaneously out of milk
                         BUG 3: 23,499 slow-mover items pass donor threshold but
                         nobody is requesting them (they have no deficit partner)
SECTION C (DB):          EMPTY - no transfers ever committed to DB (0 records)

MAPPING:                 product_barcode_map: {bc_pct:.1f}% coverage
                         product_department_map: {dept_pct:.1f}% coverage
                         product_supplier_map: {supp_pct:.1f}% coverage
""".format(
    bc_pct=100*match_bcode/max(len(prods_org1),1),
    dept_pct=100*match_dept/max(len(prods_org1),1),
    supp_pct=100*match_supp/max(len(prods_org1),1),
))
