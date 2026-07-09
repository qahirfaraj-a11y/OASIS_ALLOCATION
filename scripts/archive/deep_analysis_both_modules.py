"""
Deep Analysis: Smart Ordering + Transfer Intelligence
=====================================================
Probes the full pipeline: products → enrichment → order calc → MOQ gate → 
network transfer → final PO. Detects gaps, logic errors, data mismatches.
"""
import sys, os, json, math, traceback
from collections import Counter, defaultdict

sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.logic.simulation_bridge import SimulationOrderUtil
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
from oasis.logic.fulfillment_decider import FulfillmentDecider, NetworkAvailabilityMap, StoreSkuState

DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'
uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
conn = UniversalConnector(uri, SchemaMapper.for_pos_erp())
adapter = PosErpAdapter(conn)

orgs = adapter.fetch_all_organizations()
org_cds = [o["ORG_CD"] for o in orgs]
org_name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}

print("=" * 80)
print("DEEP ANALYSIS: SMART ORDERING + TRANSFER INTELLIGENCE")
print("=" * 80)

# ============================================================
# SECTION 1: Product Data Integrity
# ============================================================
print("\n" + "=" * 80)
print("SECTION 1: PRODUCT DATA INTEGRITY")
print("=" * 80)

for org_cd in org_cds[:2]:
    products = adapter.fetch_enriched_products(org_cd)
    print(f"\n--- {org_name_map[org_cd]} ({org_cd}) ---")
    print(f"  Total products: {len(products)}")
    
    # Field presence check
    key_fields = ['item_code', 'product_name', 'current_stocks', 'avg_daily_sales', 
                  'selling_price', 'supplier_name', 'department', 'uom', 
                  'lead_time_days', 'on_order_qty', 'pack_size', 'cost_price',
                  'is_fresh', 'sales_rank', 'reorder_point', 'total_units_sold_last_90d',
                  'median_gap_days', 'demand_cv', 'is_top_sku', 'is_key_sku']
    
    missing_counts = {}
    for f in key_fields:
        missing = sum(1 for p in products if p.get(f) is None or p.get(f) == '')
        if missing > 0:
            missing_counts[f] = missing
    
    if missing_counts:
        print(f"  ⚠️ MISSING FIELDS:")
        for f, c in sorted(missing_counts.items(), key=lambda x: -x[1]):
            pct = c / len(products) * 100
            print(f"    {f}: {c}/{len(products)} ({pct:.0f}%)")
    
    # ADS distribution
    ads_values = [float(p.get('avg_daily_sales', 0) or 0) for p in products]
    ads_zero = sum(1 for a in ads_values if a == 0)
    ads_positive = sum(1 for a in ads_values if a > 0)
    print(f"  ADS: {ads_positive} positive, {ads_zero} zero ({ads_zero/len(products)*100:.0f}% blind)")
    
    # Stock analysis
    stocks = [float(p.get('current_stocks', 0) or 0) for p in products]
    stock_zero = sum(1 for s in stocks if s <= 0)
    stock_tiny = sum(1 for s in stocks if 0 < s < 1.0)
    stock_fractional_ea = sum(1 for p in products if str(p.get('uom', 'EA')).upper() == 'EA' and float(p.get('current_stocks', 0)) > 0 and float(p.get('current_stocks', 0)) != int(float(p.get('current_stocks', 0))))
    print(f"  Stock: {stock_zero} at zero, {stock_tiny} effectively depleted (0<s<1)")
    
    # UOM check
    uom_counter = Counter(str(p.get('uom', 'MISSING')).upper() for p in products)
    print(f"  UOM distribution: {dict(uom_counter)}")
    
    # Supplier distribution
    suppliers = Counter(str(p.get('supplier_name', 'UNKNOWN')).upper().strip() for p in products)
    print(f"  Unique suppliers: {len(suppliers)}")
    unknown_sup = suppliers.get('UNKNOWN', 0) + suppliers.get('', 0) + suppliers.get('NONE', 0)
    print(f"  ⚠️ Unknown/missing supplier: {unknown_sup}")

# ============================================================
# SECTION 2: ORDER ENGINE PIPELINE
# ============================================================
print("\n" + "=" * 80)
print("SECTION 2: ORDER ENGINE PIPELINE")
print("=" * 80)

engine = OrderEngine(DATA_DIR)
engine.load_local_databases()

sim_util = SimulationOrderUtil(DATA_DIR, engine=engine)

test_org = 'ORG001'
products = adapter.fetch_enriched_products(test_org)
print(f"\nTesting pipeline for {test_org} ({len(products)} products)...")

# Step 2a: prepare_sku_data
enriched = sim_util.prepare_sku_data(products)
print(f"  After prepare_sku_data: {len(enriched)} items")

# Check enrichment quality
rop_zero = sum(1 for e in enriched if float(e.get('reorder_point', 0)) <= 0)
rop_positive = len(enriched) - rop_zero
ads_for_rop = sum(1 for e in enriched if float(e.get('avg_daily_sales', 0)) > 0 and float(e.get('reorder_point', 0)) <= 0)
print(f"  ROP: {rop_positive} have reorder_point > 0, {rop_zero} are 0/missing")
print(f"  ⚠️ Items with ADS > 0 but ROP = 0: {ads_for_rop} (ROP fallback will activate)")

# Step 2b: calculate_order_quantity
raw_recs = sim_util.calculate_order_quantity(enriched, gnn_risk_score=0.3, use_real_date=True)
print(f"\n  After calculate_order_quantity:")
qty_positive = sum(1 for r in raw_recs if float(r.get('recommended_quantity', 0)) > 0)
qty_zero = len(raw_recs) - qty_positive
print(f"    Recommended > 0: {qty_positive}")
print(f"    Recommended = 0: {qty_zero}")

# Reasoning analysis
reason_counter = Counter()
for r in raw_recs:
    reasoning = r.get('reasoning', '')
    if 'Schedule:' in reasoning: reason_counter['Schedule Block'] += 1
    elif 'Above ROP' in reasoning: reason_counter['Above ROP'] += 1
    elif 'Blocked: AMIT' in reasoning: reason_counter['AMIT Blocked'] += 1
    elif 'Blocked: MANDE' in reasoning: reason_counter['MANDE Blocked'] += 1
    elif 'Blocked: Stale' in reasoning: reason_counter['Stale Fresh'] += 1
    elif 'Blocked: Dead' in reasoning: reason_counter['Dead Stock'] += 1
    elif 'CRITICAL OVERRIDE' in reasoning: reason_counter['Critical Override'] += 1
    elif 'Net Req:' in reasoning: reason_counter['Net Req Calculated'] += 1
    elif 'Adequate Coverage' in reasoning: reason_counter['Adequate Coverage'] += 1
    else: reason_counter['Other'] += 1
print(f"  Reasoning breakdown:")
for reason, count in reason_counter.most_common():
    print(f"    {reason}: {count}")

# Step 2c: finalize_orders
final_recs = sim_util.finalize_orders(raw_recs)
final_pos = sum(1 for r in final_recs if float(r.get('recommended_quantity', 0)) > 0)
print(f"\n  After finalize_orders (safety guards):")
print(f"    Items with qty > 0: {final_pos}")
total_units = sum(float(r.get('recommended_quantity', 0)) for r in final_recs if float(r.get('recommended_quantity', 0)) > 0)
print(f"    Total PO units: {total_units:,.0f}")

# Step 2d: Check for fractional PO quantities
frac_po = sum(1 for r in final_recs 
              if float(r.get('recommended_quantity', 0)) > 0 
              and float(r.get('recommended_quantity', 0)) != int(float(r.get('recommended_quantity', 0))))
print(f"    ⚠️ Fractional PO quantities: {frac_po}")
if frac_po > 0:
    frac_samples = [(r['product_name'][:30], r['recommended_quantity']) 
                     for r in final_recs 
                     if float(r.get('recommended_quantity', 0)) > 0 
                     and float(r.get('recommended_quantity', 0)) != int(float(r.get('recommended_quantity', 0)))][:5]
    for name, qty in frac_samples:
        print(f"      {name}: {qty}")

# ============================================================
# SECTION 3: MOQ GATE
# ============================================================
print("\n" + "=" * 80)
print("SECTION 3: MOQ / MINIMUM ORDER GATE")
print("=" * 80)

# First run network optimization to get network_adjusted_recs
print("  Running CTS network optimization...")
enriched_network_stock = {}
for oc in org_cds:
    enriched_network_stock[oc] = adapter.fetch_enriched_products(oc)

cts = ConsolidatedTransferService(
    org_names=org_name_map,
    stock_data=enriched_network_stock,
    registry_path=None,
    distance_map={},
)
network_plan = cts.optimize_network({test_org: final_recs}, risk_scores={})
network_adjusted_recs = network_plan.adjusted_orders.get(test_org, [])

print(f"  Network transfers identified: {len(network_plan.transfers)}")
print(f"  Orders reduced by network: {network_plan.total_orders_reduced}")

# Now apply MOQ gate
mot_result = sim_util.apply_minimum_order_gate(network_adjusted_recs)
po_recs = mot_result['po_recs']
dropped_recs = mot_result['transfer_recs']
supplier_summary = mot_result['supplier_summary']

print(f"\n  MOQ Gate Results:")
print(f"    Items passing MOQ (→ PO): {len(po_recs)}")
print(f"    Items FAILING MOQ (→ Transfer): {len(dropped_recs)}")
print(f"    Items with qty=0 (no order): {len(mot_result['no_order'])}")

print(f"\n  Supplier-Level MOT Summary:")
for supplier, info in sorted(supplier_summary.items(), key=lambda x: -x[1]['value']):
    emoji = "✅" if info['status'] == 'PO' else "❌"
    print(f"    {emoji} {supplier}: {info['item_count']} items, {info['units']:.0f} units, KES {info['value']:,.0f} → {info['status']}")

# Check which dropped items have transfer opportunities
print(f"\n  ⚠️ MOQ-Failed Items Analysis:")
drop_with_ads = sum(1 for d in dropped_recs if float(d.get('avg_daily_sales', 0)) > 0)
drop_with_stock = sum(1 for d in dropped_recs if float(d.get('current_stocks', d.get('current_stock', 0))) > 0)
print(f"    With ADS > 0: {drop_with_ads}")
print(f"    With current stock > 0: {drop_with_stock}")

# ============================================================
# SECTION 4: TRANSFER INTELLIGENCE MODULE
# ============================================================
print("\n" + "=" * 80)
print("SECTION 4: TRANSFER INTELLIGENCE MODULE")
print("=" * 80)

# Check how many PULL vs PUSH opportunities exist
bcode_path = os.path.join(DATA_DIR, 'product_barcode_map.json')
with open(bcode_path, 'r', encoding='utf-8') as f:
    _bcode_map = json.load(f)

nmap = NetworkAvailabilityMap()
store_deficit_items = {}
store_excess_count = {}

for _org_cd, _prods in enriched_network_stock.items():
    _excess_n = 0
    _deficits = []
    for _p in _prods:
        _ads   = float(_p.get("avg_daily_sales", 0) or 0)
        _curr  = float(_p.get("current_stocks", 0) or 0)
        _safe  = _ads * 2.0
        _excs  = _curr - _safe
        _pname = str(_p.get("product_name", ""))
        _bcode = _bcode_map.get(_pname, "")
        _dept  = str(_p.get("department", _p.get("category", "GENERAL"))).upper()
        _fresh = bool(_p.get("is_fresh", False)) or any(
            k in _dept for k in ["MILK","DAIRY","FRESH","MEAT","BREAD","BAKERY"]
        )
        nmap.add(StoreSkuState(
            org_cd=_org_cd,
            org_name=org_name_map.get(_org_cd, _org_cd),
            itm_cd=str(_p.get("item_code", "")),
            product_name=_pname,
            current_stock=_curr,
            avg_daily_sales=_ads,
            safety_stock=_safe,
            excess=_excs,
            is_fresh=_fresh,
            sell_price=float(_p.get("selling_price", 0) or 0),
            department=_dept,
            days_since_delivery=int(_p.get("last_days_since_last_delivery", 0) or 0),
            velocity_ratio=float(_ads / max(1.0, _curr)) if _curr > 0 else 0.0,
        ), _bcode)
        if _excs > 0:
            _excess_n += 1
        _days_cover = (_curr / _ads) if _ads > 0 else 999.0
        _pull_trigger = (
            (_ads > 0 and _days_cover < 7.0) or
            (_ads == 0 and _curr < 1.0)
        )
        if _pull_trigger:
            _deficits.append({
                "itm_cd": str(_p.get("item_code", "")),
                "product_name": _pname,
                "current_stock": _curr,
                "avg_daily_sales": _ads,
                "days_cover": round(_days_cover, 1),
                "sell_price": float(_p.get("selling_price", 0) or 0),
                "department": _dept,
                "supplier": str(_p.get("supplier_name", "") or ""),
                "uom": str(_p.get("uom", "EA")).upper(),
            })
    store_excess_count[_org_cd] = _excess_n
    store_deficit_items[_org_cd] = _deficits

print(f"\n  Network Map Summary:")
for org_cd in org_cds:
    n_def = len(store_deficit_items.get(org_cd, []))
    n_exc = store_excess_count.get(org_cd, 0)
    print(f"    {org_name_map[org_cd]}: {n_def} deficit items, {n_exc} excess items")

# Generate PULL transfer opportunities
decider = FulfillmentDecider(transfer_cost_kes=500.0, distance_map={}, warehouse_hubs=[])
pull_opps = 0
pull_no_donor = 0
for _rec_org, _deficits in store_deficit_items.items():
    for _item in _deficits[:50]:
        _itm_cd = _item["itm_cd"]
        _ads = _item["avg_daily_sales"]
        _curr = _item["current_stock"]
        _target_qty = max(_ads * 7.0, 1.0) if _ads > 0 else 2.0
        _shortfall = max(0.0, _target_qty - _curr)
        if _shortfall < 0.1:
            continue
        _donors = nmap.find_donors(
            _itm_cd, _rec_org,
            product_name=_item["product_name"],
            distance_calc=decider._calculate_distance_km,
        )
        if _donors:
            pull_opps += 1
        else:
            pull_no_donor += 1

print(f"\n  PULL Analysis:")
print(f"    Opportunities with donor: {pull_opps}")
print(f"    ⚠️ Deficits with NO donor: {pull_no_donor}")

# ============================================================
# SECTION 5: CROSS-MODULE GAPS
# ============================================================
print("\n" + "=" * 80)
print("SECTION 5: CROSS-MODULE GAP ANALYSIS")
print("=" * 80)

# Gap 1: MOQ failures vs Transfer Intelligence overlap
moq_item_set = set(d.get('product_name', '') for d in dropped_recs)
xfer_item_set = set()
for org_deficits in store_deficit_items.values():
    for d in org_deficits:
        xfer_item_set.add(d['product_name'])

overlap = moq_item_set & xfer_item_set
only_moq = moq_item_set - xfer_item_set
only_xfer = xfer_item_set - moq_item_set

print(f"\n  MOQ-Failed Items vs Transfer Intelligence Deficit Items:")
print(f"    In BOTH (properly routed): {len(overlap)}")
print(f"    ⚠️ MOQ-failed but NOT in Transfer deficits: {len(only_moq)}")
print(f"    In Transfer deficits but NOT MOQ-failed: {len(only_xfer)}")
if only_moq:
    print(f"    → These {len(only_moq)} items can't be ordered AND won't be transferred!")
    for item in list(only_moq)[:5]:
        print(f"      - {item[:50]}")

# Gap 2: Overstock items with no transfer opportunities
total_overstock = sum(store_excess_count.values())
total_deficits = sum(len(d) for d in store_deficit_items.values())
print(f"\n  Overstock vs Deficit balance:")
print(f"    Total overstock items (across all stores): {total_overstock}")
print(f"    Total deficit items (across all stores): {total_deficits}")
ratio = total_deficits / max(1, total_overstock)
print(f"    Deficit-to-Overstock ratio: {ratio:.2f}")
if ratio < 0.1:
    print(f"    ⚠️ Very few deficits relative to overstock — threshold may be too lenient")
elif ratio > 5:
    print(f"    ⚠️ Many more deficits than overstock — insufficient network supply")

# Gap 3: Calendar day blocking
from datetime import datetime
today_dow = datetime.today().strftime('%A')
schedule_blocked = sum(1 for r in raw_recs if 'Schedule:' in r.get('reasoning', ''))
critical_overrides = sum(1 for r in raw_recs if 'CRITICAL OVERRIDE' in r.get('reasoning', ''))
print(f"\n  Calendar Analysis (Today = {today_dow}):")
print(f"    Schedule-blocked items: {schedule_blocked}")
print(f"    Critical overrides: {critical_overrides}")
if schedule_blocked > len(raw_recs) * 0.7:
    print(f"    ⚠️ >70% items schedule-blocked — check if calendar is loaded correctly")

# Gap 4: UOM consistency between modules  
print(f"\n  UOM Consistency Check:")
for org_cd in org_cds[:1]:
    prods = enriched_network_stock[org_cd]
    ea_fractional = sum(1 for p in prods 
                        if str(p.get('uom', 'EA')).upper() == 'EA' 
                        and float(p.get('current_stocks', 0)) != int(float(p.get('current_stocks', 0)))
                        and float(p.get('current_stocks', 0)) > 0)
    print(f"    {org_cd}: {ea_fractional} EA items with fractional stock")

# Gap 5: Price data for value calculations
print(f"\n  Price Data Quality:")
for org_cd in org_cds[:1]:
    prods = enriched_network_stock[org_cd]
    zero_price = sum(1 for p in prods if float(p.get('selling_price', 0) or 0) == 0)
    zero_cost = sum(1 for p in prods if float(p.get('cost_price', 0) or 0) == 0)
    print(f"    {org_cd}: {zero_price} items with sell_price=0, {zero_cost} with cost_price=0")
    if zero_price > len(prods) * 0.1:
        print(f"    ⚠️ >10% items have no sell price — transfer values will be KES 0")

print("\n" + "=" * 80)
print("ANALYSIS COMPLETE")
print("=" * 80)
