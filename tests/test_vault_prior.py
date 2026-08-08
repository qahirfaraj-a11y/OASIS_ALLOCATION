"""Tests for the Rhapta catalog loader + vault coarse-prior extractor."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.catalog_snapshot import (
    dedupe, normalise_rows, vendor_departments,
)
from oasis.logic.vault_prior import (
    parse_supplier_complimentary, project_to_departments,
)


class TestCatalog:
    def test_normalise_and_dedupe_keeps_highest_stock(self):
        raw = [
            {"BARCODE": "X", "ITM_NAME": "Milk", "DEPARTMENT": "DAIRY",
             "VENDOR_NAME": "BROOKSIDE", "SellPrice": "60", "STOCK": "5"},
            {"BARCODE": "X", "ITM_NAME": "Milk", "DEPARTMENT": "DAIRY",
             "VENDOR_NAME": "BROOKSIDE", "SellPrice": "60", "STOCK": "20"},
            {"BARCODE": "", "ITM_NAME": "noise", "DEPARTMENT": "", "VENDOR_NAME": ""},
        ]
        rows = dedupe(normalise_rows(raw))
        assert len(rows) == 1                # blank barcode dropped, X deduped
        assert rows[0]["stock"] == 20.0 and rows[0]["price"] == 60.0

    def test_vendor_primary_department(self):
        rows = normalise_rows([
            {"BARCODE": "1", "VENDOR_NAME": "ACME", "DEPARTMENT": "BISCUITS"},
            {"BARCODE": "2", "VENDOR_NAME": "ACME", "DEPARTMENT": "BISCUITS"},
            {"BARCODE": "3", "VENDOR_NAME": "ACME", "DEPARTMENT": "SWEETS"},
        ])
        assert vendor_departments(rows)["ACME"] == "BISCUITS"


class TestVaultPrior:
    def test_parse_supplier_complimentary(self, tmp_path):
        d = tmp_path / "Nodes" / "Suppliers"
        d.mkdir(parents=True)
        (d / "KENYA SWEETS LTD.md").write_text(
            "## Complimentary Partners\n"
            "- [complimentary]:: [[TWIRL ENTERPRISES LTD]] (Weight: 171)\n"
            "- [complimentary]:: [[Unknown]] (Weight: 101)\n"
            "- [complimentary]:: [[MZURI SWEETS LIMITED]] (Weight: 85)\n",
            encoding="utf-8")
        edges = parse_supplier_complimentary(str(tmp_path))
        ks = edges["KENYA SWEETS LTD"]
        assert ks["TWIRL ENTERPRISES LTD"] == 171.0
        assert ks["MZURI SWEETS LIMITED"] == 85.0
        assert "UNKNOWN" not in ks            # 'Unknown' partner skipped

    def test_project_to_departments(self):
        sup_edges = {"ACME": {"GLOBEX": 100.0}, "GLOBEX": {"ACME": 100.0}}
        vendor_dept = {"ACME": "BISCUITS", "GLOBEX": "DAIRY"}
        prior = project_to_departments(sup_edges, vendor_dept)
        # symmetric dept halo, weights aggregated both ways
        assert prior["BISCUITS"]["DAIRY"] == 200.0
        assert prior["DAIRY"]["BISCUITS"] == 200.0

    def test_project_skips_same_dept_and_unknown(self):
        sup_edges = {"A": {"B": 5.0, "C": 9.0}}
        vendor_dept = {"A": "DAIRY", "B": "DAIRY"}   # C unknown, B same dept
        assert project_to_departments(sup_edges, vendor_dept) == {}
