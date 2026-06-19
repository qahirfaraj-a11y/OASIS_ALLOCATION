"""Tests for the native store-graph export."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.store_graph_export import build_stores_network, _stock_profile


class TestBuildStoresNetwork:
    def test_store_id_equals_org_cd(self):
        orgs = [{"ORG_CD": "ORG001", "ORG_NAME": "Westlands", "ORG_CITY": "Nairobi",
                 "ORG_STATE": "Nairobi"},
                {"ORG_CD": "ORG002", "ORG_NAME": "Mombasa Rd", "ORG_CITY": "Nairobi"}]
        net = build_stores_network(orgs)
        ids = [s["store_id"] for s in net["stores"]]
        assert ids == ["ORG001", "ORG002"]   # == ORG_CD, exact risk join
        assert net["store_count"] == 2
        assert net["reference_store"] == "ORG001"
        assert net["stores"][0]["is_reference_store"] is True

    def test_defaults_applied(self):
        net = build_stores_network([{"ORG_CD": "ORG001", "ORG_NAME": "X"}])
        s = net["stores"][0]
        assert s["floor_area_sqft"] == 10000
        assert s["supplier_diversity_score"] == 0.5
        assert s["region"] == "default"   # no city/state -> default

    def test_skips_blank_org(self):
        net = build_stores_network([{"ORG_CD": "", "ORG_NAME": "x"},
                                    {"ORG_CD": "ORG009", "ORG_NAME": "ok"}])
        assert [s["store_id"] for s in net["stores"]] == ["ORG009"]

    def test_stock_profile_rolls_up_by_department(self):
        stock = {"ORG001": [{"department": "DAIRY", "avg_daily_sales": 2.0},
                            {"department": "DAIRY", "avg_daily_sales": 3.0},
                            {"department": "BEER", "avg_daily_sales": 1.0}]}
        net = build_stores_network([{"ORG_CD": "ORG001", "ORG_NAME": "X"}], stock)
        prof = {d["department"]: d["ads_scaled"] for d in net["stores"][0]["stock_profile"]}
        assert prof["DAIRY"] == 5.0 and prof["BEER"] == 1.0

    def test_empty(self):
        net = build_stores_network([])
        assert net["store_count"] == 0 and net["reference_store"] is None


class TestStockProfile:
    def test_none_and_empty(self):
        assert _stock_profile(None) == []
        assert _stock_profile([]) == []
