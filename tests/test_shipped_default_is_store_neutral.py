"""The shipped engine defaults name no one store; department roles are config.

Only oasis_engines_config.default.json ships in a release; each install tunes
its own oasis_engines_config.json. The bread review put one store's facts into
the default -- its reviewed departments, its four bakeries, a 5-day BREAD
label, sixteen of its milk lines -- so every new store would have inherited
them. The default is neutral now and the reference store keeps its values in
its own tier.

The department lists that decide freshness and transfers lived in three
modules, in that store's department names. They are one config block,
`departments`, with the three jobs kept apart:
  fresh             ordering freshness (exact names)
  fresh_raw         the adapter flag the transfer scan reads before enrichment
  no_auto_transfer  never auto-transferred (substrings)
"""
import json
import os

import pytest

from oasis.logic import department_constants as dc

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name):
    return json.load(open(os.path.join(ROOT, "oasis", "data", name), encoding="utf-8"))


STORE = _load("oasis_engines_config.json")
DEFAULT = _load("oasis_engines_config.default.json")


class TestTheShippedDefault:
    def test_the_fresh_cycle_names_no_store(self):
        fc = DEFAULT["fresh_cycle"]
        for k in ("reviewed_departments", "presence_departments", "bakery_suppliers",
                  "overnight_delivery_departments", "moq_exempt_departments"):
            assert fc[k] == [], k
        assert fc["sellable_life_days"] == {} and fc["sellable_life_per_sku"] == {}

    def test_no_store_product_or_supplier_ships(self):
        assert DEFAULT["long_life"]["products"] == []
        text = json.dumps(DEFAULT).upper()
        for name in STORE["fresh_cycle"]["bakery_suppliers"] + STORE["long_life"]["products"]:
            assert name.upper() not in text, name

    def test_the_generic_rules_still_ship(self):
        assert DEFAULT["long_life"]["name_tokens"] and DEFAULT["long_life"]["shelf_stable_pack_tokens"]
        assert DEFAULT["fresh_cycle"]["name_keywords_rule"] == "unknown_department"
        assert set(DEFAULT["departments"]) >= {"fresh", "fresh_raw", "no_auto_transfer"}


class TestThisStoresTier:
    def test_keeps_every_value_the_review_set(self):
        fc = STORE["fresh_cycle"]
        assert fc["reviewed_departments"] == ["BREAD", "CAKES"]
        assert fc["moq_exempt_departments"] == ["BREAD", "CAKES"]
        assert fc["overnight_delivery_departments"] == ["BREAD"]
        assert fc["sellable_life_days"] == {"BREAD": 5}
        assert len(fc["bakery_suppliers"]) == 4
        assert len(STORE["long_life"]["products"]) == 16

    def test_its_departments_are_the_lists_the_code_carried(self):
        # moving the lists into config must not move this store's orders
        d = STORE["departments"]
        # (its in-store bakery department is its own name, so it left the code)
        assert set(d["fresh"]) == set(dc.FRESH_DEPARTMENTS) | {"BAKERY FOODPLUS"}
        assert d["fresh_raw"] == dc.ADAPTER_FRESH_DEPARTMENTS
        assert d["no_auto_transfer"] == dc.NO_AUTO_TRANSFER_DEPARTMENTS


@pytest.fixture
def roles(monkeypatch):
    """Point the department roles at a config the test writes."""
    from oasis.logic import engines_config

    def use(departments):
        monkeypatch.setattr(engines_config, "load_engines_config",
                            lambda *a, **k: {"departments": departments})
        dc.reset_department_roles()
    yield use
    monkeypatch.undo()
    dc.reset_department_roles()


class TestTheRolesAreConfig:
    def test_another_stores_names_decide_freshness(self, roles):
        roles({"fresh": ["Boulangerie"], "fresh_raw": ["Laitier"], "no_auto_transfer": ["FRAIS"]})
        assert dc.fresh_departments() == ["BOULANGERIE"]
        assert dc.is_fresh_department(" laitier ") and not dc.is_fresh_department("BAKERY")

    def test_the_transfer_planner_reads_its_own_role(self, roles):
        from oasis.logic.fulfillment_decider import _is_fresh_department
        roles({"no_auto_transfer": ["FRAIS"]})
        assert _is_fresh_department("PRODUITS FRAIS") and not _is_fresh_department("BREAD")

    def test_a_missing_role_falls_back_to_the_generic_names(self, roles):
        roles({"fresh": ["BOULANGERIE"]})
        assert dc.department_role("fresh_raw") == dc.ADAPTER_FRESH_DEPARTMENTS
        assert "BREAD" in dc.no_auto_transfer_departments()

    def test_an_empty_role_means_none(self, roles):
        roles({"no_auto_transfer": []})
        assert dc.no_auto_transfer_departments() == []
