"""Tests for whitelabel branding config loader."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.branding import DEFAULTS, Branding, load_branding, save_branding


class TestLoadBranding:
    def test_defaults_when_no_file(self, tmp_path):
        p = str(tmp_path / "nope.json")
        b = load_branding(p)
        assert b.tenant_name == DEFAULTS["tenant_name"]
        assert b.primary_color == DEFAULTS["primary_color"]
        assert b.source is None

    def test_partial_config_merges_with_defaults(self, tmp_path):
        p = tmp_path / "b.json"
        p.write_text('{"tenant_name": "ACME Retail"}')
        b = load_branding(str(p))
        assert b.tenant_name == "ACME Retail"
        assert b.primary_color == DEFAULTS["primary_color"]   # unchanged
        assert b.source == str(p)

    def test_invalid_hex_falls_back(self, tmp_path):
        p = tmp_path / "b.json"
        p.write_text('{"primary_color": "not-a-color", "accent_color": "#123"}')
        b = load_branding(str(p))
        assert b.primary_color == DEFAULTS["primary_color"]
        assert b.accent_color == "#123"                       # valid short hex kept

    def test_unknown_fields_ignored(self, tmp_path):
        p = tmp_path / "b.json"
        p.write_text('{"tenant_name": "X", "future_field": 42}')
        b = load_branding(str(p))
        assert b.tenant_name == "X"

    def test_malformed_json_falls_back(self, tmp_path):
        p = tmp_path / "b.json"
        p.write_text("{not-json")
        b = load_branding(str(p))
        assert b.tenant_name == DEFAULTS["tenant_name"]


class TestSaveBranding:
    def test_roundtrip(self, tmp_path):
        p = str(tmp_path / "b.json")
        save_branding(Branding(tenant_name="ACME", primary_color="#ff0000"), p)
        b = load_branding(p)
        assert b.tenant_name == "ACME" and b.primary_color == "#ff0000"


class TestDisplayTitle:
    def test_prepends_tenant_when_set(self):
        assert Branding(tenant_name="ACME", product_name="OASIS").display_title() == "OASIS — ACME"

    def test_bare_product_when_no_tenant(self):
        assert Branding().display_title() == DEFAULTS["product_name"]
