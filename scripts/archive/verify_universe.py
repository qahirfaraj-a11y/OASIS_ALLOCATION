"""Verify that oasis.db has a queryable retail universe."""
import sys
sys.path.insert(0, "C:/Oasis")

from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

conn = UniversalConnector("sqlite:///C:/Oasis/oasis.db", SchemaMapper.for_pos_erp())
adapter = PosErpAdapter(conn)

orgs = adapter.fetch_all_organizations()
print(f"Organizations: {len(orgs)}")
for o in orgs:
    print(f"  {o['ORG_CD']}: {o['ORG_NAME']}")

products = adapter.fetch_product_master("ORG001")
print(f"\nProducts (ORG001): {len(products)}")
for p in products[:5]:
    print(f"  {p['product_name']}: stock={p['current_stocks']}, cost={p['cost_price']}, sell={p['selling_price']}")

stock = adapter.fetch_stock_snapshot("ORG001")
print(f"\nStock snapshot: {len(stock)} items")

sales = adapter.fetch_sales_history("ORG001", days=30)
print(f"Sales history: {len(sales)} rows")

health = conn.health_check()
print(f"\nHealth: {health['status']} | Tables: {health['tables_found']} | Latency: {health['latency_ms']}ms")
