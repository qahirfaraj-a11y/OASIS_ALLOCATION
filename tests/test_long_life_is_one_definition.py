"""Long life is one definition, shared by the enrichment and the order-up-to engine.

The enrichment decided "long life" with `tok in name`, a substring test, and in
three more places with hardcoded token lists that bypassed the config. The
engine's is_long_life already matched on word boundaries because a substring
finds ESL inside MUESLI: 65 dry lines -- mueslis, cereal bars, Rieslings, smoked
salmon PRESLICED, DESLY noodles -- carried the long-life cover cap, which is
tighter than dry goods'.

Now both call order_up_to.long_life_match, fed from the config's long_life
block. The freshness override also reads long_life.shelf_stable_pack_tokens
(TETRA, LONGLIFE): a pack format switches freshness off without imposing the
long-life cap.
"""
import json
import os

import pytest

from oasis.logic import order_up_to as ou
from oasis.logic.intelligence_mixin import IntelligenceMixin

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class Engine(IntelligenceMixin):
    def __init__(self, cfg=None):
        if cfg is None:
            from oasis.logic.engines_config import load_engines_config
            cfg = load_engines_config(None)
        self.engines_config = cfg


@pytest.fixture
def eng():
    ou._LONG_LIFE = None
    return Engine()


FALSE_POSITIVES = [
    "NATURALLI 500G ORIGINAL MUESLI",           # ESL inside MUESLI
    "CAVIT 750ML RIESLING",                     # ESL inside RIESLING
    "PREMIUM SALMON SMOKED PRESLICED 200G",     # ESL inside PRESLICED
    "DESLY 400G EGG NOODLES",                   # ESL inside DESLY
    "DIABLO 30G APRICOT MUESLI BAR NAS",
]
GENUINE = [
    "RAVINE GOLD 500ML ESL UHT MILK",           # tokens as whole words
    "BIO 1L WHOLE LONGLIFE MILK",               # on the products list
    "KCC 200ML TCA CHOCOLATE MILK SHAKE",       # on the products list, no token
    "SUPA 200G BREADCRUMBS COARSE",
]


class TestOneRule:
    @pytest.mark.parametrize("name", FALSE_POSITIVES)
    def test_a_token_inside_another_word_is_not_long_life(self, eng, name):
        assert not eng._is_long_life(name)
        assert not ou.is_long_life(name)

    @pytest.mark.parametrize("name", GENUINE)
    def test_genuine_long_life_is_found_by_both(self, eng, name):
        assert eng._is_long_life(name)
        assert ou.is_long_life(name)

    def test_the_two_agree_on_every_line_in_the_store(self, eng):
        snap = json.load(open(os.path.join(ROOT, "oasis", "data", "stock_snapshot_dept.json"), encoding="utf-8"))
        names = [" ".join(k.upper().split()) for k in snap]
        disagree = [n for n in names if eng._is_long_life(n) != ou.is_long_life(n)]
        assert not disagree, disagree[:10]

    def test_the_enrichment_reads_its_own_config(self):
        # a store whose config lists a product the shipped file does not
        e = Engine({"long_life": {"products": ["HOUSE 1L OAT DRINK"], "name_tokens": ["UHT"]}})
        assert e._is_long_life("HOUSE 1L OAT DRINK")
        assert not e._is_long_life("NATURALLI 500G ORIGINAL MUESLI")


class TestShelfStablePacks:
    @pytest.mark.parametrize("name", ["MAAZA 250ML MANGO JUICE TETRA", "CAPRICE 1L SWEET RED TETRA",
                                      "VARTA LONGLIFE POWER BATT MN 1500 B2 AA 2S 1.5V"])
    def test_a_pack_format_switches_freshness_off(self, eng, name):
        assert eng._is_shelf_stable_pack(name)

    @pytest.mark.parametrize("name", ["MAAZA 250ML MANGO JUICE TETRA", "CAPRICE 1L SWEET RED TETRA",
                                      "VARTA LONGLIFE POWER BATT MN 1500 B2 AA 2S 1.5V"])
    def test_but_does_not_impose_the_long_life_cap(self, eng, name):
        # the long-life cap is tighter than dry goods'; a tetra wine keeps dry cover
        assert not eng._is_long_life(name)

    def test_tetra_is_a_word_not_a_prefix(self, eng):
        assert not eng._is_shelf_stable_pack("NER ELITE 2.0 TETRAD QS 4")

    def test_every_long_life_line_is_also_shelf_stable(self, eng):
        for name in GENUINE:
            assert eng._is_shelf_stable_pack(name)

    def test_the_pack_list_is_config(self):
        e = Engine({"long_life": {"name_tokens": ["UHT"], "shelf_stable_pack_tokens": ["POUCH"]}})
        assert e._is_shelf_stable_pack("X 500ML POUCH")
        assert not e._is_shelf_stable_pack("MAAZA 250ML MANGO JUICE TETRA")


def test_both_config_tiers_carry_the_pack_tokens():
    for tier in ("oasis_engines_config.json", "oasis_engines_config.default.json"):
        cfg = json.load(open(os.path.join(ROOT, "oasis", "data", tier), encoding="utf-8"))
        assert cfg["long_life"]["shelf_stable_pack_tokens"] == ["TETRA", "LONGLIFE"], tier
