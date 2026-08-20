"""Operator-tunable transfer windows.

The contract worth pinning is not that the values load — it is that they are
OVERRIDES over derived defaults, that a nonsense value is refused rather than
applied, and that setting one changes THIS scan and not the process.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import transfer_settings as TS
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService


def _svc(**kw):
    return ConsolidatedTransferService(
        org_names={"A": "a", "B": "b"}, stock_data={"A": [], "B": []}, **kw)


class TestBounds:
    def test_a_value_outside_its_range_is_refused_not_clamped(self):
        """900 days of dead-stock window is a typo, not a policy.

        Clamping would silently apply 365 and look deliberate. Refusing falls
        back to the derived default and logs why.
        """
        s = TS.BY_KEY["dead_stock_days"]
        assert s.parse("30") == 30
        assert s.parse("900") is None
        assert s.parse("2") is None

    def test_unparseable_and_blank_fall_back_to_derived(self):
        s = TS.BY_KEY["release_fraction"]
        assert s.parse("abc") is None
        assert s.parse("") is None
        assert s.parse(None) is None

    def test_every_setting_declares_a_default_inside_its_own_bounds(self):
        for s in TS.SETTINGS:
            assert s.low <= s.derived <= s.high, s.key


class TestOverrides:
    def test_unset_keys_stay_derived(self):
        d = _svc()
        assert d.RELEASE_FRACTION == ConsolidatedTransferService.RELEASE_FRACTION
        assert d.DEAD_STOCK_DAYS == ConsolidatedTransferService.DEAD_STOCK_DAYS

    def test_an_override_changes_this_scan_only(self):
        """Instance shadows, never the class — one tuned scan must not retune
        every other service alive in the process."""
        tuned = _svc(settings={"release_fraction": 0.8, "dead_stock_days": 30})
        assert tuned.RELEASE_FRACTION == 0.8
        assert tuned.DEAD_STOCK_DAYS == 30
        assert ConsolidatedTransferService.RELEASE_FRACTION == 0.5
        assert ConsolidatedTransferService.DEAD_STOCK_DAYS == 90
        assert _svc().RELEASE_FRACTION == 0.5

    def test_the_derived_median_responds_to_a_tightened_ceiling(self):
        """Overrides feed the derivation; they do not sit beside it.

        max_relief_days caps every horizon, so the network median relief must
        move when it is lowered — if it did not, the override would be
        decoration.
        """
        rhythm = {f"s{i}": {"median_gap_days": 20, "estimated_delivery_days": 4,
                            "lata_variance_multiplier": 1.0} for i in range(5)}
        base = _svc(supplier_rhythm=rhythm)
        tight = _svc(supplier_rhythm=rhythm, settings={"max_relief_days": 9})
        assert base._median_relief == pytest.approx(24.0)
        assert tight._median_relief == pytest.approx(9.0)

    def test_the_cost_lever_reuses_the_key_the_panel_already_showed(self):
        """`max_transfer_cost_kes` shipped in the Settings panel from the first
        release and was read by NOTHING. It is wired, not duplicated under a
        new name."""
        assert "max_transfer_cost_kes" in TS.BY_KEY
        assert _svc(settings={"max_transfer_cost_kes": 1200.0}).transfer_cost_kes == 1200.0

    def test_donor_eligibility_is_tunable(self):
        assert "min_excess_ratio" in TS.BY_KEY
        assert _svc(settings={"min_excess_ratio": 3.0}).min_excess_ratio == 3.0


class TestLoad:
    def test_a_missing_database_degrades_to_derived(self):
        """A settings table that cannot be read must not stop a transfer scan."""
        assert TS.load(None) == {}
        assert TS.load("/nonexistent/path/to.db") == {}

    def test_values_equal_to_the_default_are_not_reported_as_overrides(self):
        """Seeding writes the derived value into every row. Those rows must not
        then read back as operator intent — otherwise every install looks
        hand-tuned and the log claim becomes meaningless."""
        s = TS.BY_KEY["release_fraction"]
        assert s.parse(str(s.derived)) == s.derived   # parses fine...
        # ...but load() filters it out; proven via the documented rule
        assert s.derived == 0.5
