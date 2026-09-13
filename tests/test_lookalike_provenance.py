"""An inferred demand rate must not be reported as "no signal".

When a line has no measured sales and low GRN confidence, the engine
substitutes the MEDIAN daily rate of every SKU sharing the first whitespace
token of its product name, marks it a new item, and caps its aggression. That
substitution is defensible -- it is how you order something that has never
sold, and the median is the conservative choice against a mean the tail would
dominate.

What was not defensible: ads_source had already been fixed as 'none' when the
measured rate came back zero, and was never corrected. So the provenance log
reported "none 10,967 (27.6%)" on the live store while the planner used a
brand-median forecast for those lines and ordered KES 5,343,893 against it.
Every one of the 3,293 ordered lines labelled 'none' carried a non-zero rate
at order time.

"We inferred this" and "we have nothing" are different claims, and the
difference decides whether a buyer looks.
"""
import pytest

from oasis.logic.order_engine import OrderEngine


DB = {
    "ACME 500ML COLA": {"avg_daily_sales": 0.20},
    "ACME 1L COLA": {"avg_daily_sales": 0.40},
    "ACME 2L COLA": {"avg_daily_sales": 0.60},
    "ACME 5KG DOG FOOD": {"avg_daily_sales": 9.99},
    "ZETA 100G SOAP": {"avg_daily_sales": 5.00},
}

DEPTS = {
    "ACME 500ML COLA": "SODA",
    "ACME 1L COLA": "SODA",
    "ACME 2L COLA": "SODA",
    "ACME 5KG DOG FOOD": "PET FOOD",
    "ZETA 100G SOAP": "SOAP",
    "ACME 250ML COLA": "SODA",
}


class _Eng(OrderEngine):
    """Only the lookalike helper is under test; skip the heavy __init__."""

    def __init__(self, depts=None):
        self.databases = {"product_department_map": dict(depts or DEPTS)}


@pytest.fixture
def eng():
    return _Eng()


class TestTheLookalikeItself:
    def test_it_returns_the_brand_median_WITHIN_the_department(self, eng):
        """ACME sells colas AND dog food. Only the colas may inform a cola.

        Without the department the pool is ACME-anything and the median is
        pulled by the 9.99/day dog food -- which is the live-book defect in
        miniature: Red Paw 5Kg Dog Food planned at 4.23/day because three RED
        BULL cans shared its first word.
        """
        assert eng._find_lookalike_demand(
            "ACME 250ML COLA", DB, department="SODA") == pytest.approx(0.40)

    def test_a_different_department_does_not_borrow_across(self, eng):
        assert eng._find_lookalike_demand(
            "ACME 250ML COLA", DB, department="PET FOOD") == pytest.approx(9.99)

    def test_an_unknown_brand_infers_nothing(self, eng):
        """Returning 0 here is what leaves a line genuinely unplanned -- and
        those must never be ordered."""
        assert eng._find_lookalike_demand(
            "NOBRAND 1KG THING", DB, department="SODA") == 0.0

    def test_an_unknown_department_infers_nothing(self, eng):
        """The cross-category pool IS the bug, so it is not a fallback."""
        assert eng._find_lookalike_demand(
            "ACME 250ML COLA", DB, department="NOT A REAL AISLE") == 0.0

    def test_a_name_the_map_does_not_know_infers_nothing(self):
        """Red Paw's real failure: absent from product_department_map, so no
        department can be resolved and nothing may be inferred."""
        e = _Eng(depts={k: v for k, v in DEPTS.items() if "DOG" not in k})
        assert e._find_lookalike_demand("ACME 5KG DOG FOOD", DB) == 0.0

    def test_an_empty_name_is_survivable(self, eng):
        assert eng._find_lookalike_demand("", DB, department="SODA") == 0.0
        assert eng._find_lookalike_demand("   ", DB, department="SODA") == 0.0

    def test_the_key_is_case_insensitive(self, eng):
        assert eng._find_lookalike_demand(
            "acme 250ml cola", DB, department="soda") == pytest.approx(0.40)

    def test_the_department_is_looked_up_when_not_supplied(self, eng):
        """The caller does not always carry one; the catalogue map usually
        knows it."""
        assert eng._find_lookalike_demand(
            "ACME 250ML COLA", DB) == pytest.approx(0.40)

    def test_the_median_not_the_mean(self):
        """A pool spanning 140x on the live book would be dominated by its
        tail under a mean. NESTLE: 157 SKUs, 0.010 to 9.46/day."""
        skewed = {f"X {i}": {"avg_daily_sales": v}
                  for i, v in enumerate([0.01, 0.02, 0.03, 0.04, 100.0])}
        e = _Eng(depts={f"X {i}": "AISLE" for i in range(5)} | {"X NEW": "AISLE"})
        assert e._find_lookalike_demand("X NEW", skewed) == pytest.approx(0.03)


class TestTheProvenanceNamesIt:
    """Asserted on the source, because the substitution happens deep inside a
    23k-line enrichment that needs a live catalogue to run end to end."""

    def test_the_substitution_sets_its_own_source(self):
        import inspect
        from oasis.logic import intelligence_mixin as im
        src = inspect.getsource(im)
        i = src.find("_look = self._find_lookalike_demand(")
        assert i > 0, "the lookalike substitution moved; this guard needs rewriting"
        window = src[i:i + 1200]
        assert "lookalike_brand_median" in window, (
            "the substitution no longer records its own ads_source, so the "
            "provenance log will again report 'none' for an inferred rate")

    def test_the_source_is_only_claimed_when_something_was_inferred(self):
        """A lookalike that finds no pool returns 0 and must stay 'none': the
        line really does have nothing, and it must not be ordered."""
        import inspect
        from oasis.logic import intelligence_mixin as im
        src = inspect.getsource(im)
        i = src.find("_look = self._find_lookalike_demand")
        assert i > 0
        window = src[i:i + 800]
        assert "if _look > 0:" in window, (
            "ads_source must not claim an inference that returned nothing")

    def test_the_pool_key_is_recorded_for_audit(self):
        import inspect
        from oasis.logic import intelligence_mixin as im
        assert "lookalike_brand" in inspect.getsource(im), (
            "the inferred rate should name the pool it came from -- the key is "
            "the first token of the name, which yields 121 numeric or short "
            "keys on this catalogue")
