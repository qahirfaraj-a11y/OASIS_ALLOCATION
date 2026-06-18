"""Unit tests for the ledger loader's pure helpers (Risk Redesign P1b).

The file-reading functions are integration-level (need the real data, not in
CI); only the pure join/normalisation helpers are unit tested here.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.ledger_loader import norm_barcode, org_matches, RHAPTA


class TestNormBarcode:
    def test_strips_float_artifact(self):
        assert norm_barcode("12345.0") == "12345"

    def test_plain_string_unchanged(self):
        assert norm_barcode("6164004734140") == "6164004734140"

    def test_scientific_notation(self):
        assert norm_barcode("2.027110e+12") == "2027110000000"

    def test_whitespace(self):
        assert norm_barcode("  95849008317 ") == "95849008317"


class TestOrgMatches:
    def test_matches_rhapta(self):
        assert org_matches("027 - Rhapta Road") is True
        assert org_matches("027 - Rhapta Road", RHAPTA) is True

    def test_rejects_other_org(self):
        assert org_matches("001 - HEAD OFFICE") is False
        assert org_matches("002 - Adlife Plaza") is False

    def test_custom_org(self):
        assert org_matches("001 - HEAD OFFICE", "001") is True
