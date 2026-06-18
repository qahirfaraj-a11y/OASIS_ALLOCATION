"""Tests for the install preflight pure logic."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.preflight import (
    evaluate_pos_schema, evaluate_sales_history, overall_status, _redact,
    REQUIRED_POS_TABLES,
)


def _full_schema():
    """A schema that satisfies every required + recommended table/column."""
    from oasis.logic.preflight import RECOMMENDED_POS_TABLES
    cols = {}
    for t, cs in {**REQUIRED_POS_TABLES, **RECOMMENDED_POS_TABLES}.items():
        cols[t] = set(cs)
    return set(cols), cols


class TestEvaluatePosSchema:
    def test_full_schema_all_pass(self):
        tables, cols = _full_schema()
        checks = evaluate_pos_schema(tables, cols)
        assert all(c["status"] == "PASS" for c in checks)

    def test_missing_required_table_fails(self):
        tables, cols = _full_schema()
        tables.discard("POS_SALES_DTL")
        cols.pop("POS_SALES_DTL")
        checks = evaluate_pos_schema(tables, cols)
        bad = [c for c in checks if "POS_SALES_DTL" in c["check"]]
        assert bad and bad[0]["status"] == "FAIL"

    def test_missing_recommended_table_warns(self):
        tables, cols = _full_schema()
        tables.discard("GRN_HDR")
        cols.pop("GRN_HDR")
        checks = evaluate_pos_schema(tables, cols)
        bad = [c for c in checks if "GRN_HDR" in c["check"]]
        assert bad and bad[0]["status"] == "WARN"

    def test_missing_required_column_fails(self):
        tables, cols = _full_schema()
        cols["ITEM_MST"].discard("ITM_CD")
        checks = evaluate_pos_schema(tables, cols)
        bad = [c for c in checks if "ITEM_MST" in c["check"]]
        assert bad and bad[0]["status"] == "FAIL"
        assert "ITM_CD" in bad[0]["detail"]

    def test_case_insensitive(self):
        # lower-case table/column names still satisfy the contract
        cols = {t.lower(): {c.lower() for c in cs} for t, cs in REQUIRED_POS_TABLES.items()}
        checks = evaluate_pos_schema(set(cols), cols)
        req = [c for c in checks if c["status"] == "FAIL"]
        assert req == []

    def test_present_but_uninspectable_columns_pass(self):
        # table present, no column info (e.g., some drivers) -> PASS not FAIL
        tables = set(REQUIRED_POS_TABLES)
        checks = evaluate_pos_schema(tables, {})
        assert all(c["status"] == "PASS" for c in checks if "table" in c["check"]
                   and c["check"].split()[1] in REQUIRED_POS_TABLES)


class TestSalesHistory:
    def test_enough(self):
        assert evaluate_sales_history(120, min_days=90)["status"] == "PASS"

    def test_too_little(self):
        assert evaluate_sales_history(30, min_days=90)["status"] == "WARN"

    def test_unknown(self):
        assert evaluate_sales_history(None)["status"] == "WARN"


class TestOverallStatus:
    def test_fail_dominates(self):
        assert overall_status([{"status": "PASS"}, {"status": "WARN"}, {"status": "FAIL"}]) == "FAIL"

    def test_warn_over_pass(self):
        assert overall_status([{"status": "PASS"}, {"status": "WARN"}]) == "WARN"

    def test_all_pass(self):
        assert overall_status([{"status": "PASS"}]) == "PASS"


class TestRedact:
    def test_hides_credentials(self):
        out = _redact("postgresql://user:secret@host:5432/db")
        assert "secret" not in out and "host:5432/db" in out

    def test_sqlite_unchanged(self):
        assert _redact("sqlite:///x.db") == "sqlite:///x.db"
