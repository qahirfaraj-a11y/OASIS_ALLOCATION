"""Tests for per-client schema adaptation (view generation + validation)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.client_schema import (
    identity_profile, validate_profile, generate_view_ddl, create_clause_for,
)
from oasis.logic.preflight import REQUIRED_POS_TABLES


class TestValidateProfile:
    def test_identity_profile_is_complete(self):
        assert validate_profile(identity_profile())["ok"] is True

    def test_missing_required_table(self):
        prof = identity_profile()
        del prof["POS_SALES_DTL"]
        v = validate_profile(prof)
        assert v["ok"] is False
        assert "POS_SALES_DTL" in v["missing_tables"]

    def test_missing_required_column(self):
        prof = identity_profile()
        prof["ITEM_MST"]["columns"].pop("ITM_CD")
        v = validate_profile(prof)
        assert v["ok"] is False
        assert "ITM_CD" in v["missing_columns"]["ITEM_MST"]

    def test_recommended_absent_still_ok(self):
        # dropping a recommended-only table doesn't fail validation
        prof = {t: identity_profile()[t] for t in REQUIRED_POS_TABLES}
        assert validate_profile(prof)["ok"] is True


class TestGenerateViewDDL:
    def test_aliases_client_columns_to_canonical(self):
        prof = {
            "ITEM_MST": {"source": "dbo.Products",
                         "columns": {"ITM_CD": "ProductCode", "ITM_LONG_NAME": "Name"}},
        }
        ddl = generate_view_ddl(prof)
        assert len(ddl) == 1
        s = ddl[0]
        assert s.startswith("CREATE VIEW ITEM_MST AS SELECT ")
        assert "ProductCode AS ITM_CD" in s
        assert "Name AS ITM_LONG_NAME" in s
        assert "FROM dbo.Products" in s

    def test_skips_unmapped(self):
        prof = {"ITEM_MST": {"source": "", "columns": {}}}
        assert generate_view_ddl(prof) == []

    def test_dialect_prefix(self):
        prof = {"ITEM_MST": {"source": "P", "columns": {"ITM_CD": "C"}}}
        assert generate_view_ddl(prof, create_clause_for("mssql"))[0].startswith("CREATE OR ALTER VIEW")
        assert generate_view_ddl(prof, create_clause_for("postgres"))[0].startswith("CREATE OR REPLACE VIEW")
        assert generate_view_ddl(prof, create_clause_for("sqlite"))[0].startswith("CREATE VIEW ")


class TestCreateClause:
    def test_known_and_default(self):
        assert create_clause_for("MSSQL") == "CREATE OR ALTER VIEW"
        assert create_clause_for("weird-db") == "CREATE VIEW"


class TestLiteralsAndWhere:
    def test_literal_satisfies_required_column(self):
        # SUPPLIER_CD mapped, LEAD-style absent column provided as a NULL literal
        prof = identity_profile()
        prof["STOCK_MASTER"]["columns"].pop("SM_QTY")
        prof["STOCK_MASTER"].setdefault("literals", {})["SM_QTY"] = "SM_CURR_STK_QTY"
        assert validate_profile(prof)["ok"] is True

    def test_ddl_emits_literals_and_where(self):
        prof = {
            "STOCK_MASTER": {
                "source": "STOCK_MASTER",
                "columns": {"SM_QTY": "SM_CURR_STK_QTY"},
                "literals": {"SM_LAST_RECV_DT": "NULL"},
                "where": "SM_LEVEL_NUMBER = 1",
            }
        }
        ddl = generate_view_ddl(prof)[0]
        assert "SM_CURR_STK_QTY AS SM_QTY" in ddl
        assert "NULL AS SM_LAST_RECV_DT" in ddl
        assert ddl.rstrip(";").endswith("WHERE SM_LEVEL_NUMBER = 1")


class TestRxlProfile:
    def test_rxl_profile_validates_and_generates(self):
        import json
        import os
        path = os.path.join(os.path.dirname(__file__), "..", "rxl_schema_profile.json")
        prof = {k: v for k, v in json.load(open(path, encoding="utf-8")).items()
                if not k.startswith("_")}
        assert validate_profile(prof)["ok"] is True
        ddl = generate_view_ddl(prof, create_clause_for("mssql"))
        joined = "\n".join(ddl)
        # the key real->canonical remaps are present. These were VERIFIED
        # against a real RXL database (TESTING11) on 2026-08-11 — the two
        # stock/price ones were already correct.
        assert "SM_CURR_STK_QTY AS SM_QTY" in joined
        assert "BSP_SELL_PRICE AS BSP_SP" in joined
        # SUPPLIER_CD is NOT a column on RXL's ITEM_MST. This test used to
        # assert "VENDOR_CD AS SUPPLIER_CD", which no real RXL could satisfy.
        # The item/vendor link is BASIC_CP_MST.BCP_VEND_CD, joined in.
        assert "BCP_VEND_CD AS SUPPLIER_CD" in joined
        assert "VENDOR_CD AS SUPPLIER_CD" not in joined
        # and the vendor master is keyed VAM_*, not VENDOR_*
        assert "VAM_CD AS SUPPLIER_CD" in joined
        assert "VAM_NAME AS SUPPLIER_NAME" in joined
        assert "VENDOR_ADDRESS_MST" in joined
        # synthetic attrs provided as literals so the adapter SELECT resolves
        assert "NULL AS LEAD_TIME_DAYS" in joined
