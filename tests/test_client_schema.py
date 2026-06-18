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
