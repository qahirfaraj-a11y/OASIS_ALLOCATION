"""Long-life lines must not be capped at the daily-fresh coverage target.

WHY THIS EXISTS
---------------
`calculate_replenishment_target_stock` has always carried a long-life rule:
a product whose name says UHT / ESL / LONG LIFE gets
`min(target, max(7, cycle + lead))`. Two things stopped it reaching the lines
that needed it most:

  1. The DAILY_FRESH_JIT branch runs first and pins any `is_fresh` product on a
     daily-cadence supplier to 1.2 days. The long-life rule below it is a
     `min()`, so it can only ever hold that down - never lift it.
  2. Detection was a product-NAME whitelist, and the single largest affected
     line, BROOKSIDE 500ML DAIRY BEST (POUCH), carries no marking at all.

Measured on the real engine, the fix takes Dairy Best from 873 units of cover
to 5,091 - on a product that keeps for months and whose supplier is currently
short-shipping.

The risk of the fix is loosening cover on something that genuinely spoils, so
most of what follows guards the fresh side, not the long-life side.

Everything runs against a temp data dir; nothing is written to oasis/data.
"""
import os
import shutil
import tempfile

import pytest

REPO = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))


@pytest.fixture(scope="module")
def engine():
    from oasis.logic.order_engine import OrderEngine
    tmp = tempfile.mkdtemp(prefix="oasis_ll_test_")
    parent = os.path.join(tmp, "parent")
    data_dir = os.path.join(parent, "data")
    os.makedirs(data_dir, exist_ok=True)
    for f in ("supplier_rhythm_analysis.json", "supplier_weekly_schedule.json"):
        src = os.path.join(REPO, f)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(parent, f))
    yield OrderEngine(data_dir)
    shutil.rmtree(tmp, ignore_errors=True)


def target(engine, name, supplier, is_fresh=True, ads=50.0, lead=2):
    return engine.calculate_replenishment_target_stock({
        "product_name": name,
        "avg_daily_sales": ads,
        "estimated_delivery_days": lead,
        "supplier_name": supplier,
        "is_fresh": is_fresh,
        "median_gap_days": 1.0,
    }, {})


# ── the fresh side must not move ────────────────────────────────────────────
@pytest.mark.parametrize("name,supplier", [
    ("FESTIVE 800G WHITE MILKY BREAD", "DPL FESTIVE  LIMITED"),
    ("BROOKSIDE 500ML TR FRESH MILK", "BROOKSIDE DAIRY LIMITED"),
    ("ILARA FRESH MILK 500ML SATCHET", "BROOKSIDE DAIRY LIMITED"),
    ("BIO 200ML GREEK STYLE NATURE PLAIN", "BIO FOOD PRODUCTS LTD"),
    ("ISINYA 30S EGGS", "BROOKSIDE DAIRY LIMITED"),
])
def test_genuine_fresh_still_capped_at_daily_jit(engine, name, supplier):
    """A real perishable on a daily supplier keeps its 1.2-day target."""
    assert not engine._is_long_life(name.upper())
    assert target(engine, name, supplier) == pytest.approx(1.2, abs=0.05)


# ── the long-life side must move ────────────────────────────────────────────
@pytest.mark.parametrize("name", [
    "BROOKSIDE 500ML DAIRY BEST (POUCH)",
    "TUZO 500ML WHOLE MILK (FINO PACK) 180DAYS",
])
def test_listed_long_life_escapes_the_fresh_cap(engine, name):
    """Named on the operator list, so it is exempt from DAILY_FRESH_JIT.

    These carry no UHT/ESL/LONG LIFE marking, which is exactly why the name
    whitelist alone could never catch them.
    """
    assert engine._is_long_life(name.upper())
    got = target(engine, name, "BROOKSIDE DAIRY LIMITED")
    assert got > 1.3, f"{name} still pinned to the daily-fresh target ({got})"
    # 6.0, not the 7.0 this asserted before. The velocity multiplier that used
    # to scale target_days by 1.4 on a >10/day line has been removed -- it had
    # the sign backwards and scaled cycle stock, which is not a risk quantity.
    # 8.4 fell to 6.0, and the long-life cap, min(target, max(7, R + L)), no
    # longer binds on these lines because 6.0 is already under it.
    #
    # The property under test is the escape from the 1.2-day daily-fresh cap,
    # which the assertion above states directly and which still holds. The
    # exact figure was always incidental to that.
    assert got == pytest.approx(6.0, abs=0.05)


def test_token_matched_long_life_unchanged(engine):
    """The pre-existing keyword path must behave exactly as before."""
    assert engine._is_long_life("RAVINE GOLD 500ML ESL UHT MILK")
    assert target(engine, "RAVINE GOLD 500ML ESL UHT MILK",
                  "RAVINE DAIRIES LIMITED") == pytest.approx(16.0, abs=0.05)


# ── the false positive that nearly shipped ──────────────────────────────────
def test_a_battery_is_not_long_life_dairy(engine):
    """Guards against broadening the name tokens.

    Adding ASEPTIC / TFA / LONGLIFE to the token list would catch
    'VARTA LONGLIFE POWER BATT'. The long-life cap is TIGHTER than the
    dry-goods cap, so a battery classed as long life quietly loses cover.
    That is why the awkward dairy cases go on an explicit list instead.
    """
    name = "VARTA LONGLIFE POWER BATT MN 1500 B2 AA 2S 1.5V"
    assert not engine._is_long_life(name.upper())
    # 6.0, not 8.4: the x1.4 velocity multiplier has been removed (8.4 was
    # exactly 6.0 x 1.4).
    #
    # WORTH KNOWING: this number no longer DISCRIMINATES. The long-life cap is
    # min(target, max(7, R + L)); with the dry-goods target now at 6.0 it sits
    # under that cap, so a battery wrongly classed as long life would score
    # 6.0 too. The classification assertion above is what actually guards
    # this case now, and it is the one that matters -- but if the cap is ever
    # tightened below 6 days, restore a numeric check that can tell them
    # apart.
    assert target(engine, name, "VARTA", is_fresh=False) == pytest.approx(6.0, abs=0.05)


def test_long_life_list_is_reachable_from_config(engine):
    """The list is operator-maintained data, not code."""
    names = engine._long_life_names()
    assert names, "long_life.products is empty - the config block is missing"
    assert "BROOKSIDE 500ML DAIRY BEST (POUCH)" in names
