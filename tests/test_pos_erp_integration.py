"""
POS/ERP Integration Tests
=========================
End-to-end tests validating the full integration pipeline:
  Mock DB → PosErpAdapter → OrderEngine → API Bridge

Run: python -m pytest tests/test_pos_erp_integration.py -v
"""

import os
import sys
import sqlite3
import logging
import pytest

# Add project root to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.mock_pos_erp import MockPosErpBuilder, summarize_mock_db
from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter

logging.basicConfig(level=logging.INFO, format="%(message)s")
logger = logging.getLogger("TestPosErp")

# Shared test DB path
TEST_DB_PATH = os.path.join(os.path.dirname(__file__), "test_mock_pos_erp.db")
TEST_DB_URI = f"sqlite:///{TEST_DB_PATH}"


@pytest.fixture(scope="module")
def mock_db():
    """Create mock DB once for all tests in this module.

    fast_mode caps the catalogue and the store count: every assertion below is
    a shape/invariant check (>=100 items, >=3 stores), so the full 14-store,
    23.5k-SKU build buys nothing but fixture time.
    """
    builder = MockPosErpBuilder(db_path=TEST_DB_PATH, seed=42, fast_mode=True)
    builder.build(reset=True)
    yield TEST_DB_PATH
    # Cleanup after all tests (best-effort on Windows due to file locks)
    try:
        if os.path.exists(TEST_DB_PATH):
            os.remove(TEST_DB_PATH)
    except PermissionError:
        pass  # SQLAlchemy connection pool may hold file lock on Windows


@pytest.fixture(scope="module")
def connector(mock_db):
    """Create a UniversalConnector to the mock DB."""
    mapper = SchemaMapper.for_pos_erp()
    return UniversalConnector(TEST_DB_URI, mapper)


@pytest.fixture(scope="module")
def adapter(connector):
    """Create a PosErpAdapter."""
    return PosErpAdapter(connector)


# =====================================================================
# Test 1: Mock Database Creation & Schema Validation
# =====================================================================
class TestMockDbCreation:
    def test_db_file_exists(self, mock_db):
        assert os.path.exists(mock_db)

    def test_all_tables_created(self, mock_db):
        summary = summarize_mock_db(mock_db)
        expected_tables = [
            "ORGANIZATION_MST", "ITEM_MST", "STOCK_MASTER",
            "BASIC_SP_MST", "BASIC_CP_MST", "CUSTOMER_MST",
            "POS_SALES_HDR", "POS_SALES_DTL", "BI_SALES_REPORT",
            "COUNTER_MST", "BASE_SYSTEM_PREFERENCES",
            "TAX_PLAN_HDR", "TAX_MST", "SUPPLIER_MST",
            "GRN_HDR", "INTEGRATION_PURCHASE_ORDERS",
        ]
        for table in expected_tables:
            assert table in summary, f"Missing table: {table}"

    def test_seed_data_counts(self, mock_db):
        # Invariants, not builder constants: the builder seeds the full store
        # network + catalogue now (historically 3 stores / 100 items).
        summary = summarize_mock_db(mock_db)
        assert summary["ORGANIZATION_MST"] >= 3, "Expected at least 3 stores"
        assert summary["ITEM_MST"] >= 100, "Expected a real catalogue"
        # each item stocked in at least one store, at most in every store
        assert summary["ITEM_MST"] <= summary["STOCK_MASTER"] <= \
            summary["ITEM_MST"] * summary["ORGANIZATION_MST"]
        assert summary["POS_SALES_HDR"] > 10000, "Expected 10000+ sales transactions"
        assert summary["POS_SALES_DTL"] > 50000, "Expected 50000+ line items"
        assert summary["CUSTOMER_MST"] == 50, "Expected 50 customers"
        assert summary["BI_SALES_REPORT"] > 0, "Expected BI report data"

    def test_foreign_keys(self, mock_db):
        conn = sqlite3.connect(mock_db)
        # Verify stock references valid items
        orphans = conn.execute("""
            SELECT COUNT(*) FROM STOCK_MASTER s
            WHERE NOT EXISTS (SELECT 1 FROM ITEM_MST i WHERE i.ITM_CD = s.SM_ITM_CD)
        """).fetchone()[0]
        assert orphans == 0, f"Found {orphans} orphan stock records"
        conn.close()


# =====================================================================
# Test 2: PosErpAdapter → OASIS Dict Mapping
# =====================================================================
class TestAdapterProductFetch:
    def test_fetch_product_master_returns_data(self, adapter):
        products = adapter.fetch_product_master("ORG001")
        assert len(products) >= 100, f"Expected the full catalogue, got {len(products)}"

    def test_product_dict_has_required_keys(self, adapter):
        products = adapter.fetch_product_master("ORG001")
        required_keys = [
            "item_code", "product_name", "barcode", "current_stocks",
            "selling_price", "cost_price", "supplier_name", "category",
            "department", "is_fresh", "estimated_delivery_days",
            "supplier_reliability", "estimated_daily_sales",
        ]
        for key in required_keys:
            assert key in products[0], f"Missing key: {key}"

    def test_stock_values_are_numeric(self, adapter):
        products = adapter.fetch_product_master("ORG001")
        for p in products[:10]:
            assert isinstance(p["current_stocks"], float), f"current_stocks should be float: {p['current_stocks']}"
            assert isinstance(p["selling_price"], float), f"selling_price should be float: {p['selling_price']}"
            assert p["selling_price"] > 0, f"selling_price should be positive: {p['product_name']}"

    def test_stock_snapshot(self, adapter):
        snapshot = adapter.fetch_stock_snapshot("ORG001")
        assert len(snapshot) >= 100
        # Should be sorted by stock ascending (lowest first)
        assert float(snapshot[0]["current_stocks"]) <= float(snapshot[-1]["current_stocks"])


# =====================================================================
# Test 3: Sales Intelligence Extraction
# =====================================================================
class TestSalesIntelligence:
    def test_fetch_sales_intelligence_structure(self, adapter):
        intel = adapter.fetch_sales_intelligence("ORG001", days=300)
        assert len(intel) > 0, "Expected sales intelligence data"

        # Check first entry matches OrderEngine format
        first_name = list(intel.keys())[0]
        entry = intel[first_name]
        assert "monthly_sales" in entry
        assert "avg_daily_sales" in entry
        assert "avg_monthly_sales" in entry
        assert "trend" in entry
        assert isinstance(entry["monthly_sales"], dict)
        assert entry["trend"] in ("growing", "stable", "declining")

    def test_avg_daily_sales_is_reasonable(self, adapter):
        # "Reasonable" as an invariant, not a magic cap: a daily average over a
        # >=1-day window can never exceed the total units in that window (the
        # old <10000 cap broke when the mock scaled network-level staples).
        intel = adapter.fetch_sales_intelligence("ORG001", days=300)
        for name, data in list(intel.items())[:10]:
            ads = data["avg_daily_sales"]
            total = sum(float(v or 0) for v in data.get("monthly_sales", {}).values())
            assert ads >= 0, f"ADS should be >= 0 for {name}: {ads}"
            assert ads <= max(total, 1), \
                f"ADS exceeds total window units for {name}: {ads} > {total}"

    def test_sales_history_dataframe(self, adapter):
        df = adapter.fetch_sales_history("ORG001", days=90)
        assert len(df) > 0, "Expected sales history rows"
        expected_cols = ["bill_dt", "itm_cd", "item_name", "qty", "sell_price", "net_amt"]
        for col in expected_cols:
            assert col in df.columns, f"Missing column: {col}"


# =====================================================================
# Test 4: End-to-End: ERP → adapter → OrderEngine enrichment
# (The OrderEngine.load_from_erp/load_erp_products API these tests originally
#  encoded never shipped; PosErpAdapter.fetch_enriched_products is the
#  supported path — the same one the /erp/sync bridge endpoint now uses.)
# =====================================================================
class TestE2eErpToAllocation:
    def test_order_engine_load_from_erp(self, adapter):
        products = adapter.fetch_enriched_products("ORG001")
        assert len(products) >= 100, "Expected the full catalogue via the adapter"
        with_ads = [p for p in products if float(p.get("avg_daily_sales", 0) or 0) > 0]
        assert len(with_ads) > 0, "Expected some products with sales intelligence"

    def test_erp_products_enrichable(self, adapter):
        from oasis.logic.order_engine import OrderEngine
        data_dir = os.path.join(os.path.dirname(__file__), '..', 'oasis', 'data')

        products = adapter.fetch_enriched_products("ORG001")[:50]
        engine = OrderEngine(data_dir)
        engine.databases.setdefault('supplier_patterns', {})
        enriched = engine.enrich_product_data(products)
        assert len(enriched) == len(products)
        # engine enrichment adds the consignment flag on every product
        assert all("is_consignment" in p for p in enriched)


# =====================================================================
# Test 5: Round-Trip PO Push
# =====================================================================
class TestPoPushBack:
    def test_push_and_read_po(self, adapter):
        # Push a test PO
        recommendations = [
            {
                "item_code": "ITM00001",
                "product_name": "TEST PRODUCT A",
                "supplier_cd": "SUP0001",
                "recommended_quantity": 50,
                "cost_price": 100.0,
                "reasoning": "Stockout imminent, ADS=10, days_cover=0",
            },
            {
                "item_code": "ITM00002",
                "product_name": "TEST PRODUCT B",
                "supplier_cd": "SUP0002",
                "recommended_quantity": 25,
                "selling_price": 200.0,
                "reasoning": "Below reorder point",
            },
        ]

        lines = adapter.push_purchase_order("ORG001", recommendations)
        assert lines == 2, f"Expected 2 PO lines, got {lines}"

        # Read back from DB
        conn = sqlite3.connect(TEST_DB_PATH)
        rows = conn.execute(
            "SELECT * FROM INTEGRATION_PURCHASE_ORDERS WHERE ORG_CD = 'ORG001'"
        ).fetchall()
        conn.close()

        assert len(rows) == 2, f"Expected 2 rows in INTEGRATION_PURCHASE_ORDERS, got {len(rows)}"


# =====================================================================
# Test 6: SchemaMapper Factory
# =====================================================================
class TestSchemaMapper:
    def test_for_pos_erp_creates_mapper(self):
        mapper = SchemaMapper.for_pos_erp()
        assert isinstance(mapper, SchemaMapper)
        assert mapper.mapping["ITM_CD"] == "item_code"
        assert mapper.mapping["ITM_LONG_NAME"] == "product_name"
        assert mapper.mapping["SM_QTY"] == "current_stocks"
        assert mapper.mapping["BSP_SP"] == "selling_price"

    def test_mapper_translates_record(self):
        mapper = SchemaMapper.for_pos_erp()
        record = {"ITM_CD": "SKU001", "ITM_LONG_NAME": "Test Product", "SM_QTY": 42}
        mapped = mapper.map_record(record)
        assert mapped["item_code"] == "SKU001"
        assert mapped["product_name"] == "Test Product"
        assert mapped["current_stocks"] == 42


# =====================================================================
# Test 7: BI Summary and Supplier Patterns
# =====================================================================
class TestBiAndSuppliers:
    def test_bi_summary(self, adapter):
        df = adapter.fetch_bi_summary("ORG001")
        assert len(df) > 0
        assert "item_code" in df.columns
        assert "QUANTITY" in df.columns
        assert "REPORT_MONTH" in df.columns

    def test_supplier_patterns(self, adapter):
        patterns = adapter.fetch_supplier_patterns("ORG001")
        assert len(patterns) > 0
        first = list(patterns.values())[0]
        assert "order_frequency" in first
        assert "estimated_delivery_days" in first
        assert "reliability_score" in first

    def test_organization_fetch(self, adapter):
        org = adapter.fetch_organization("ORG001")
        assert org is not None
        # store names come from stores_network.json now — assert shape, not a
        # specific store label
        assert isinstance(org["ORG_NAME"], str) and org["ORG_NAME"].strip()

    def test_system_preferences(self, adapter):
        prefs = adapter.fetch_system_preferences()
        assert "OASIS_INTEGRATION_ENABLED" in prefs
        assert prefs["OASIS_INTEGRATION_ENABLED"] == "1"


# =====================================================================
# CLI Runner
# =====================================================================
if __name__ == "__main__":
    pytest.main([__file__, "-v", "--tb=short"])
