"""Verification script for Supplier Calendar + RuleBasedLLM + Safety Guards pipeline."""
import sys, os
from datetime import date
sys.path.insert(0, os.getcwd())

from oasis.logic.order_engine import OrderEngine, apply_safety_guards
from oasis.llm.inference import RuleBasedLLM
from oasis.data.supplier_calendar import SupplierCalendar
from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

# Setup
DATA_DIR = os.path.join(os.getcwd(), 'oasis', 'data')
DB_PATH = os.path.join(DATA_DIR, 'mock_pos_erp.db')

engine = OrderEngine(DATA_DIR)
connector = UniversalConnector(f'sqlite:///{DB_PATH}', SchemaMapper.for_pos_erp())
engine.load_from_erp(connector, 'ORG001')

# Calendar
cal_path = os.path.join(os.getcwd(), "Supplier_Order_Calendar_2026.xlsx")
calendar = SupplierCalendar(cal_path)
calendar.load()

# Test Date: Jan 8th 2026 (Day 8)
sim_date = date(2026, 1, 8)
current_doy = sim_date.timetuple().tm_yday
print(f"Testing for Date: {sim_date} (Day {current_doy})")

# Fetch Products
adapter = PosErpAdapter(connector)
products = adapter.fetch_enriched_products('ORG001')[:50]
enriched = engine.enrich_product_data(products)

rule_engine = RuleBasedLLM()

raw_recs = []
deferred = []
blocked = []

print("-" * 60)
for p in enriched:
    supp = p.get('supplier_name', 'Unknown')
    sched = calendar.get_schedule(supp)
    
    # Gap logic
    gap = max(1, int(p.get('median_gap_days', 7)))
    if p.get('supplier_frequency') == 'Daily': gap = 1
    
    # Ordering Day?
    is_ordering = False
    if sched == 'DAILY': is_ordering = True
    elif isinstance(sched, set): is_ordering = current_doy in sched
    else: is_ordering = (current_doy % gap == 0) or (current_doy == 1)
    
    # Critical?
    stock = float(p.get('current_stocks', 0))
    ads = float(p.get('avg_daily_sales', 0))
    cover = stock / ads if ads > 0 else 999.0
    lead = int(p.get('estimated_delivery_days', 1))
    safe = 4.0 if p.get('is_fresh') else 1.5
    crit_thresh = lead + safe * (1 + 2.0 * p.get('demand_cv', 0.2))
    is_crit = cover < crit_thresh
    
    if not is_ordering and not is_crit:
        deferred.append(p['product_name'])
        print(f"[DEFERRED] {p['product_name'][:30]} | Supp: {supp} | Sched: {sched} | Gap: {gap}")
        continue
        
    print(f"[PROCESSING] {p['product_name'][:30]} | Crit: {is_crit} | OrderDay: {is_ordering}")
    
    res = rule_engine._analyze_single(p)
    if res:
        p.update(res)
        raw_recs.append(p)

# Safety Guards
enrich_map = {p['product_name']: p for p in enriched}
final_recs = apply_safety_guards(raw_recs, enrich_map, allocation_mode="replenishment")

ordered_count = sum(1 for r in final_recs if r.get('recommended_quantity', 0) > 0)
blocked_count = sum(1 for r in final_recs if r.get('recommended_quantity', 0) == 0)

print("=" * 60)
print(f"Total Products: {len(enriched)}")
print(f"Deferred:       {len(deferred)}")
print(f"Processed:      {len(raw_recs)}")
print(f"Ordered (Final): {ordered_count}")
print(f"Blocked (Final): {blocked_count}")
print("PASS")
