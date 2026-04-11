"""Smoke test: verify RuleBasedLLM ordering pipeline works end-to-end."""
import sys, os
sys.path.insert(0, os.getcwd())

from oasis.logic.order_engine import OrderEngine
from oasis.llm.inference import RuleBasedLLM
from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

DATA_DIR = os.path.join(os.getcwd(), 'oasis', 'data')
DB_PATH = os.path.join(DATA_DIR, 'mock_pos_erp.db')

# Setup
engine = OrderEngine(DATA_DIR)
connector = UniversalConnector(f'sqlite:///{DB_PATH}', SchemaMapper.for_pos_erp())
engine.load_from_erp(connector, 'ORG001')

adapter = PosErpAdapter(connector)
products = adapter.fetch_enriched_products('ORG001')
enriched = engine.enrich_product_data(products)

# Run RuleBasedLLM
rule_engine = RuleBasedLLM()

orders = 0
blocked = 0
total_cost = 0

for prod in enriched:
    result = rule_engine._analyze_single(prod)
    if result is None:
        continue
    qty = result['recommended_quantity']
    reasoning = result.get('reasoning', '')
    if qty == 0:
        blocked += 1
        print(f"  BLOCKED: {prod['product_name'][:40]} -> {reasoning[:60]}")
        continue
    orders += 1
    total_cost += result.get('est_cost', 0)
    print(f"  ORDER:   {prod['product_name'][:40]} -> Qty: {qty}, Cost: KES {result.get('est_cost',0):,.0f} | {reasoning[:60]}")

print(f"\n{'='*60}")
print(f"Products Analyzed: {len(enriched)}")
print(f"Orders Generated:  {orders}")
print(f"Blocked/Zero:      {blocked}")
print(f"Total Order Cost:  KES {total_cost:,.0f}")
print("PASS")
