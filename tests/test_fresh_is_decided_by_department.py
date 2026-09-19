"""A product's department decides whether it is fresh; its name only fills a gap.

The enrichment marked a line fresh if its NAME contained MILK, BUTTER, JUICE,
CHEESE... anywhere -- a substring test with no regard to department. 2,873 dry
lines in the store came out fresh: MILK CHOCOLATE, BUTTER COOKIES, PEANUT
BUTTER, pet food, body lotion. A fresh line is planned on a 1-day lead with a
7-day shelf life, which clamps the order-up-to level to its floor and strips
the safety stock (median 8.0 -> 21.7 days of demand once corrected, measured on
the shipped path). Now fresh_cycle.name_keywords_rule = unknown_department: the
name counts only when the line carries no department.
"""
import json
import os

import pytest

from oasis.logic.intelligence_mixin import IntelligenceMixin

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Engine(IntelligenceMixin):
    def __init__(self, rule=None):
        from oasis.logic.engines_config import load_engines_config
        self.engines_config = load_engines_config(None)
        if rule is not None:
            self.engines_config.setdefault("fresh_cycle", {})["name_keywords_rule"] = rule


@pytest.mark.parametrize("name,dept", [
    ("CADBURY 180G DAIRY MILK FRUIT&NUT", "CHOCOLATES"),
    ("ROYAL DANSK 454G BUTTER ROYAL COOKIES TIN", "BISCUITS"),
    ("ALPHAJIRI FARMERS 400G PEANUT BUTTER", "PEANUT BUTTER"),
    ("WANPY 375G DOG FOOD CHICKN&VEG", "PET DOG FOOD"),
    ("ST IVES 400ML OATMEAL&SHEA BUTTER LOTION", "WOMEN/UNISEX LOTION"),
])
def test_a_dry_department_is_not_made_fresh_by_the_name(name, dept):
    assert not Engine()._fresh_by_name(name, dept)


def test_the_name_still_decides_when_there_is_no_department():
    e = Engine()
    assert e._fresh_by_name("FARM 1L FRESH MILK", "")
    assert e._fresh_by_name("FARM 1L FRESH MILK", None)
    assert not e._fresh_by_name("KITCHEN TOWEL 2S", "")


def test_no_keyword_is_never_fresh_by_name():
    assert not Engine()._fresh_by_name("KITCHEN TOWEL 2S", "")


def test_the_old_rule_is_one_config_switch_away():
    e = Engine("anywhere")
    assert e._fresh_by_name("CADBURY 180G DAIRY MILK FRUIT&NUT", "CHOCOLATES")


def test_both_config_tiers_ship_the_department_rule():
    for tier in ("oasis_engines_config.json", "oasis_engines_config.default.json"):
        cfg = json.load(open(os.path.join(ROOT, "oasis", "data", tier), encoding="utf-8"))
        assert cfg["fresh_cycle"]["name_keywords_rule"] == "unknown_department", tier


def test_the_code_default_matches_the_config():
    e = Engine()
    e.engines_config.get("fresh_cycle", {}).pop("name_keywords_rule", None)
    assert not e._fresh_by_name("CADBURY 180G DAIRY MILK FRUIT&NUT", "CHOCOLATES")
