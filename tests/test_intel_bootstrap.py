"""Tests for the engine intelligence bootstrap (merge helpers + writer)."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.intel_bootstrap import (
    merge_sales_intelligence, merge_supplier_patterns, bootstrap_intelligence,
    SALES_FILE, SUPPLIER_FILE,
)


class TestMergeSales:
    def test_sums_monthly_across_stores(self):
        a = {"MILK": {"monthly_sales": {"January": 10, "February": 20}}}
        b = {"MILK": {"monthly_sales": {"January": 5, "March": 7}}}
        out = merge_sales_intelligence([a, b])
        ms = out["MILK"]["monthly_sales"]
        assert ms["January"] == 15 and ms["February"] == 20 and ms["March"] == 7
        assert out["MILK"]["total_10mo_sales"] == 42
        assert out["MILK"]["months_active"] == 3

    def test_growing_trend(self):
        a = {"X": {"monthly_sales": {"Jan": 1, "Feb": 1, "Mar": 10, "Apr": 10}}}
        out = merge_sales_intelligence([a])
        assert out["X"]["trend"] == "growing"

    def test_empty(self):
        assert merge_sales_intelligence([]) == {}


class TestMergeSuppliers:
    def test_sums_order_counts(self):
        a = {"ACME": {"total_orders_2025": 100, "reliability_score": 0.9}}
        b = {"ACME": {"total_orders_2025": 50, "reliability_score": 0.9}}
        out = merge_supplier_patterns([a, b])
        assert out["ACME"]["total_orders_2025"] == 150

    def test_union_of_suppliers(self):
        out = merge_supplier_patterns([{"A": {"total_orders_2025": 1}},
                                       {"B": {"total_orders_2025": 2}}])
        assert set(out) == {"A", "B"}


class _FakeAdapter:
    def fetch_all_organizations(self):
        return [{"ORG_CD": "ORG001"}, {"ORG_CD": "ORG002"}]

    def fetch_sales_intelligence(self, org, days=300):
        return {"MILK": {"monthly_sales": {"January": 10}}}

    def fetch_supplier_patterns(self, org):
        return {"ACME": {"total_orders_2025": 10, "reliability_score": 0.9}}


class TestBootstrapIntelligence:
    def test_writes_engine_preferred_files(self, tmp_path):
        d = str(tmp_path)
        summary = bootstrap_intelligence(_FakeAdapter(), d)
        assert summary["orgs"] == 2
        assert summary["sales_forecasting_products"] == 1
        assert summary["supplier_patterns_suppliers"] == 1
        # files written with the _updated.json names the engine prefers
        assert os.path.exists(os.path.join(d, SALES_FILE))
        assert os.path.exists(os.path.join(d, SUPPLIER_FILE))
        # MILK summed across 2 orgs -> 20
        sf = json.load(open(os.path.join(d, SALES_FILE), encoding="utf-8"))
        assert sf["MILK"]["monthly_sales"]["January"] == 20
        # ACME orders summed across 2 orgs -> 20
        sp = json.load(open(os.path.join(d, SUPPLIER_FILE), encoding="utf-8"))
        assert sp["ACME"]["total_orders_2025"] == 20

    def test_explicit_orgs_and_no_write(self, tmp_path):
        s = bootstrap_intelligence(_FakeAdapter(), str(tmp_path), orgs=["ORG001"], write=False)
        assert s["orgs"] == 1 and s["files"] == {}
        assert not os.listdir(str(tmp_path))
