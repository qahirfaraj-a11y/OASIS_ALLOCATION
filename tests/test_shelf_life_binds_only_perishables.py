"""An expiry may cap an order only where the expiry is real.

`shelf_life_for` fell back to the returns book whenever a department asserted
nothing, and the returns book does not measure a life: it measures how long a
write-off sat before somebody processed it. Applied as a ceiling that capped
dry goods which keep for months -- 18.9L bottled water at 7 days, basmati rice
at 8, toilet cleaner at 36 -- on 1,188 lines of the Rhapta book, cutting the
cover of stock that could safely hold it.

The operator's rule (2026-09-20): only fresh and frozen lines have an expiry
that matters to ordering; everything else is 90+ days and turns long before a
code date binds. So outside a perishable department the line is unclamped.
"""
import pytest

from oasis.logic import order_up_to as ou


@pytest.fixture(autouse=True)
def fresh_caches():
    ou.reset_fresh_cycle()
    ou.reset_perishable_departments()
    yield
    ou.reset_fresh_cycle()
    ou.reset_perishable_departments()


@pytest.fixture
def book(monkeypatch):
    """A measured life on one fresh and one ambient line, and one asserted table."""
    monkeypatch.setattr(ou, "load_shelf_life", lambda root=None: {"FRESH MILK": 1.2, "YOGHURT": 14.0})
    monkeypatch.setattr(ou, "load_shelf_life_per_sku", lambda root=None: {
        "TUZO 500ML FRESH MILK": {"shelf_life_days": 4.0, "department": "FRESH MILK", "provenance": "measured"},
        "PEARL 2KG BASMATI RICE": {"shelf_life_days": 8.0, "department": "RICE", "provenance": "measured"},
        "HARPIC 500ML TOILET CLEANER": {"shelf_life_days": 36.0, "department": "TOILET CLEANER",
                                        "provenance": "measured"},
    })
    monkeypatch.setattr(ou, "is_long_life", lambda sku=None, root=None: False)


class TestOutsideFreshAndFrozen:
    @pytest.mark.parametrize("dept,sku", [
        ("RICE", "PEARL 2KG BASMATI RICE"),
        ("TOILET CLEANER", "HARPIC 500ML TOILET CLEANER"),
    ])
    def test_a_write_off_age_is_not_a_shelf_life(self, book, dept, sku):
        assert ou.shelf_life_for(dept, sku=sku) == 0.0

    def test_a_line_with_no_department_is_unclamped(self, book):
        assert ou.shelf_life_for("", sku="SOMETHING UNFILED") == 0.0


class TestInsideThem:
    def test_the_department_still_caps(self, book):
        assert ou.shelf_life_for("YOGHURT") == 14.0

    def test_and_the_smaller_of_measured_and_asserted_wins(self, book):
        # the measured figure errs long; a ceiling that errs long is worse
        assert ou.shelf_life_for("FRESH MILK", sku="TUZO 500ML FRESH MILK") == 1.2


class TestWhichDepartmentsCount:
    def test_derived_from_what_the_install_already_declares(self, book):
        d = ou.perishable_departments()
        assert "FRESH MILK" in d and "YOGHURT" in d and "RICE" not in d

    def test_an_empty_config_list_means_derive_not_none(self, book, monkeypatch):
        # read the other way it silently unclamps the whole fresh chiller
        from oasis.logic import engines_config
        monkeypatch.setattr(engines_config, "load_engines_config",
                            lambda *a, **k: {"departments": {"perishable": []}})
        ou.reset_perishable_departments()
        assert ou.shelf_life_for("FRESH MILK") == 1.2

    def test_a_named_list_is_the_store_s_own_answer(self, book, monkeypatch):
        from oasis.logic import engines_config
        monkeypatch.setattr(engines_config, "load_engines_config",
                            lambda *a, **k: {"departments": {"perishable": ["RICE"]}})
        ou.reset_perishable_departments()
        assert ou.shelf_life_for("RICE", sku="PEARL 2KG BASMATI RICE") == 8.0
        assert ou.shelf_life_for("FRESH MILK") == 0.0          # not named, not clamped


class TestThisStore:
    """Bread and milk keep exactly the lives the store reviewed."""

    def test_bread_keeps_its_label_and_milk_its_department(self):
        assert ou.shelf_life_for("BREAD", sku="FESTIVE 400G WHITE SLICED") == 5.0
        assert ou.shelf_life_for("FRESH MILK") == pytest.approx(1.2)

    def test_dry_goods_are_no_longer_capped_by_the_returns_book(self):
        for dept in ("RICE", "MINERAL WATER", "FLOUR", "TOILET CLEANER", "BISCUITS", "CRISPS"):
            assert ou.shelf_life_for(dept) == 0.0, dept
