"""The engine must be able to measure from a date other than today.

The ordering path read datetime.now() in four places that DECIDE something:
how stale a line's last delivery is, where the 30/60/90-day demand windows
fall, and which weekday the supplier calendar is checked against. Correct
against a live POS; wrong against an extract.

The Rhapta receipt history runs 2025-01-01 to 2025-12-09 -- 106,526 receipts.
Judged on a 2026 wall clock every SKU reads as 278+ days since delivery, so
the stale-fresh and dead-stock gates fire on everything; and once the builder
wrote one flat receipt date, on nothing. Measured as of the data's own horizon
the same field spreads properly and both gates fire on evidence: 5 and 23
lines, out of 3,479 SKUs older than 120 days -- the ones that ALSO sold
nothing.

Default is the wall clock, so an unset variable changes no behaviour at all.
"""
import os
from datetime import datetime

import pytest

from oasis.logic import clock


@pytest.fixture(autouse=True)
def _clean(monkeypatch):
    monkeypatch.delenv(clock.AS_OF_ENV, raising=False)
    clock._WARNED = False


class TestTheDefaultIsToday:
    def test_unset_is_the_wall_clock(self):
        before = datetime.now()
        got = clock.as_of()
        assert before <= got <= datetime.now()

    def test_unset_is_not_pinned(self):
        assert clock.is_pinned() is False

    def test_blank_and_whitespace_are_unset(self, monkeypatch):
        for raw in ("", "   ", "\t"):
            monkeypatch.setenv(clock.AS_OF_ENV, raw)
            assert clock.is_pinned() is False
            assert abs((clock.as_of() - datetime.now()).total_seconds()) < 5


class TestAPinnedDate:
    def test_a_plain_date(self, monkeypatch):
        monkeypatch.setenv(clock.AS_OF_ENV, "2025-12-09")
        assert clock.as_of().date().isoformat() == "2025-12-09"
        assert clock.is_pinned() is True

    @pytest.mark.parametrize("raw", ["2025-12-09 14:30:00", "2025-12-09T14:30:00"])
    def test_a_date_and_time(self, monkeypatch, raw):
        monkeypatch.setenv(clock.AS_OF_ENV, raw)
        got = clock.as_of()
        assert (got.date().isoformat(), got.hour, got.minute) == ("2025-12-09", 14, 30)

    def test_it_is_read_every_call_not_cached(self, monkeypatch):
        """A harness sweeping several dates in one process must get each one.
        A module-level constant here is the same class of bug this fixes."""
        monkeypatch.setenv(clock.AS_OF_ENV, "2025-01-01")
        assert clock.as_of().date().isoformat() == "2025-01-01"
        monkeypatch.setenv(clock.AS_OF_ENV, "2025-12-09")
        assert clock.as_of().date().isoformat() == "2025-12-09"


class TestItFailsLoudRatherThanOpen:
    """A mistyped date silently falling back to today would make a wrong run
    look like a working one whose numbers mean something else entirely."""

    @pytest.mark.parametrize("raw", ["not-a-date", "09-12-2025", "2025/12/09",
                                     "yesterday", "2025-13-45"])
    def test_an_unparseable_value_raises(self, monkeypatch, raw):
        monkeypatch.setenv(clock.AS_OF_ENV, raw)
        with pytest.raises(ValueError) as e:
            clock.as_of()
        assert clock.AS_OF_ENV in str(e.value)

    def test_the_error_says_how_to_fix_it(self, monkeypatch):
        monkeypatch.setenv(clock.AS_OF_ENV, "nope")
        with pytest.raises(ValueError) as e:
            clock.as_of()
        assert "YYYY-MM-DD" in str(e.value)


class TestTheOrderingPathUsesIt:
    """Asserted on the source: the alternative is a live DB in a unit test.

    These four are the clocks that decide something. The write timestamps
    (a PO's created_at) deliberately keep the wall clock -- a purchase order
    raised now IS raised now, whatever period is under analysis.
    """

    def _src(self, rel):
        import os
        root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        with open(os.path.join(root, rel), encoding="utf-8") as f:
            return f.read()

    def test_days_since_delivery_is_measured_from_as_of(self):
        assert "(as_of() - last_recv).days" in self._src("oasis/logic/pos_erp_adapter.py")

    def test_the_demand_windows_are_measured_from_as_of(self):
        src = self._src("oasis/logic/pos_erp_adapter.py")
        assert "cutoff = (as_of() - timedelta(days=days))" in src
        assert "now = as_of()" in src, "the 30/60/90 buckets still read the wall clock"

    def test_the_supplier_calendar_weekday_comes_from_as_of(self):
        src = self._src("oasis/logic/simulation_bridge.py")
        assert "_clock.as_of().timetuple().tm_yday" in src

    def test_the_demand_seeder_uses_the_same_clock(self):
        """Seeding off the wall clock while measuring as-of puts every bill
        outside its own window -- and the 30-day bucket has no upper bound, so
        future-dated bills all collapse into it at 60% weight. Measured when
        these disagreed: median ADS 0.052 -> 1.185, max 12,682/day."""
        assert "_clock.as_of().date()" in self._src("oasis/logic/real_demand.py")

    def test_write_timestamps_still_use_the_wall_clock(self):
        src = self._src("oasis/logic/pos_erp_adapter.py")
        assert 'now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")' in src, (
            "a PO's created_at is not an analysis clock and must not be pinned")
