"""Inside a reviewed section, a product's department decides whether it is fresh.

The enrichment marked a line fresh if its NAME contained MILK, BUTTER, JUICE,
CHEESE... anywhere -- a substring test with no regard to department -- and the
long-life checks were substring tests too. Store-wide that marked 2,873 dry
lines fresh. Sections are being reviewed one at a time: bread first (the daily
fresh cycle), then milk, then meat, each with rules built from its own
movement. So the reviewed rules apply only to fresh_cycle.reviewed_departments
(BREAD, CAKES); every other department keeps the rules it had before, exactly,
until its own review adds it to the list.
"""
import json
import os

import pytest

from oasis.logic.intelligence_mixin import IntelligenceMixin

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Engine(IntelligenceMixin):
    def __init__(self, rule=None, reviewed=None):
        from oasis.logic.engines_config import load_engines_config
        self.engines_config = load_engines_config(None)
        fc = self.engines_config.setdefault("fresh_cycle", {})
        if rule is not None:
            fc["name_keywords_rule"] = rule
        if reviewed is not None:
            fc["reviewed_departments"] = reviewed


class TestTheBreadSection:
    @pytest.mark.parametrize("name", ["HUSEINI 700G FRUIT CAKE", "MARKET TOWN 400G RUM & BUTTER SLAB CAKE",
                                      "MILL BAKERS 200G DAZ MILK FLAVOUR"])
    def test_a_cake_is_not_made_fresh_by_its_name(self, name):
        assert not Engine()._fresh_by_name(name, "CAKES")

    def test_bread_and_cakes_are_the_reviewed_section(self):
        e = Engine()
        assert e._in_reviewed_section("BREAD") and e._in_reviewed_section(" cakes ")

    def test_long_life_uses_the_word_boundary_rule_there(self):
        # ESL inside MUESLI made this loaf "long life" under the old substring test
        e = Engine()
        assert not e._is_shelf_stable_pack("SIMPLIFINE 400G MUESLI BREAD")
        assert e._is_shelf_stable_pack("SUPA 200G BREADCRUMBS COARSE")


class TestEverySectionNotYetReviewed:
    """Deliberately unchanged: milk, meat, cheese and the rest keep the
    pre-review rules until their own breakdown adds them to the list."""

    @pytest.mark.parametrize("dept", ["FRESH MILK", "CHEESE", "MEAT", "DELI CHEESE", "FRESH GOURMET", "CHOCOLATES"])
    def test_is_not_in_the_reviewed_section(self, dept):
        assert not Engine()._in_reviewed_section(dept)

    @pytest.mark.parametrize("name,dept", [("CADBURY 180G DAIRY MILK FRUIT&NUT", "CHOCOLATES"),
                                           ("CH. GOUDA CHEESE PKG PLU20324", "DELI CHEESE"),
                                           ("CHOICE 500G MEATY BEEF SAUSAGES", "FRESH GOURMET")])
    def test_keeps_the_old_name_rule(self, name, dept):
        assert Engine()._fresh_by_name(name, dept)

    def test_keeps_the_old_substring_long_life_tests(self):
        e = Engine()
        assert e._legacy_shelf_stable("NATURALLI 500G ORIGINAL MUESLI")      # ESL inside MUESLI, as before
        assert e._legacy_cover_long_life("CAVIT 750ML RIESLING")            # ESL inside RIESLING, as before
        assert not e._legacy_shelf_stable("BROOKSIDE 500ML DAIRY BEST (POUCH)")   # listed, but never read here

    def test_a_section_is_switched_over_by_listing_it(self):
        e = Engine(reviewed=["BREAD", "CAKES", "CHOCOLATES"])
        assert not e._fresh_by_name("CADBURY 180G DAIRY MILK FRUIT&NUT", "CHOCOLATES")


def test_the_name_decides_when_there_is_no_department():
    e = Engine()
    assert e._fresh_by_name("FARM 1L FRESH MILK", "")
    assert not e._fresh_by_name("KITCHEN TOWEL 2S", "")


def test_the_old_rule_everywhere_is_one_switch_away():
    assert Engine("anywhere")._fresh_by_name("HUSEINI 700G FRUIT CAKE", "CAKES")


def test_both_config_tiers_ship_the_scope_and_the_rule():
    for tier in ("oasis_engines_config.json", "oasis_engines_config.default.json"):
        fc = json.load(open(os.path.join(ROOT, "oasis", "data", tier), encoding="utf-8"))["fresh_cycle"]
        assert fc["reviewed_departments"] == ["BREAD", "CAKES"], tier
        assert fc["name_keywords_rule"] == "unknown_department", tier


def test_the_code_defaults_match_the_config():
    e = Engine()
    e.engines_config["fresh_cycle"].pop("reviewed_departments", None)
    e.engines_config["fresh_cycle"].pop("name_keywords_rule", None)
    assert e._in_reviewed_section("CAKES") and not e._in_reviewed_section("FRESH MILK")
    assert not e._fresh_by_name("HUSEINI 700G FRUIT CAKE", "CAKES")
