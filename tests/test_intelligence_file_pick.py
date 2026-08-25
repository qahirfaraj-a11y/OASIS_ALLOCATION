"""Which file backs an intelligence database must never be an accident.

A stale duplicate beside a real data file silently replaced three weeks of
derived supplier rhythm: the engine loaded `supplier_patterns_2025 (3).json`
instead of `supplier_patterns_2025.json` because the old rule took whatever
os.listdir returned first, and a space sorts before a dot.

Nothing errored. The log said "Loaded supplier_patterns" and named a plausible
file. The only symptom was that lead times were wrong — 21 days against a
measured 3 — which fed gap + lead + safety and inflated every order for that
supplier. KES 3.5M of stock at one store, ordered against a number nobody had
checked because nobody knew there were two files.
"""

import os
import sys
import time

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.order_engine import pick_intelligence_file

TERM = "supplier_patterns_2025"


def _write(d, name, when=None):
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as f:
        f.write("{}")
    if when is not None:
        os.utime(p, (when, when))
    return p


class TestItPicksTheRightFile:

    def test_the_canonical_name_beats_a_windows_copy(self, tmp_path):
        """THE REGRESSION. ' (3)' sorts before '.json' — that is the whole bug."""
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025 (3).json")
        _write(d, "supplier_patterns_2025.json")
        assert pick_intelligence_file(d, TERM) == "supplier_patterns_2025.json"

    def test_it_holds_however_the_filesystem_orders_them(self, tmp_path):
        """The old code depended on listdir order, so the fix must not."""
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025 (3).json")
        _write(d, "supplier_patterns_2025.json")
        listing = ["supplier_patterns_2025 (3).json", "supplier_patterns_2025.json"]
        assert pick_intelligence_file(d, TERM, listing) == "supplier_patterns_2025.json"
        assert pick_intelligence_file(d, TERM, list(reversed(listing))) == \
            "supplier_patterns_2025.json"

    def test_the_bootstrap_output_wins_over_the_canonical_name(self, tmp_path):
        """_updated.json is what bootstrap-intel writes and it is the newest
        word on the subject — that precedence predates this fix and stays."""
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025.json")
        _write(d, "supplier_patterns_2025_updated.json")
        assert pick_intelligence_file(d, TERM) == "supplier_patterns_2025_updated.json"

    def test_with_only_odd_names_it_takes_the_newest(self, tmp_path):
        """No exact match at all: prefer recency, because a stale duplicate is
        the likelier accident."""
        d = str(tmp_path)
        old = time.time() - 90 * 86400
        _write(d, "supplier_patterns_2025 (1).json", when=old)
        _write(d, "supplier_patterns_2025 (7).json", when=time.time())
        assert pick_intelligence_file(d, TERM) == "supplier_patterns_2025 (7).json"

    def test_a_single_file_is_returned_unchanged(self, tmp_path):
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025.json")
        assert pick_intelligence_file(d, TERM) == "supplier_patterns_2025.json"

    def test_nothing_matching_returns_none_rather_than_guessing(self, tmp_path):
        d = str(tmp_path)
        _write(d, "something_else.json")
        assert pick_intelligence_file(d, TERM) is None

    def test_a_missing_directory_does_not_raise(self, tmp_path):
        assert pick_intelligence_file(str(tmp_path / "nope"), TERM) is None

    def test_non_json_neighbours_are_ignored(self, tmp_path):
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025.json")
        with open(os.path.join(d, "supplier_patterns_2025.bak"), "w") as f:
            f.write("{}")
        assert pick_intelligence_file(d, TERM) == "supplier_patterns_2025.json"


class TestItSaysSoWhenAmbiguous:

    def test_ambiguity_is_logged_loudly(self, tmp_path, caplog):
        """Silence is how this survived for weeks. If two files could have
        served, the operator has to be told which one did."""
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025 (3).json")
        _write(d, "supplier_patterns_2025.json")
        with caplog.at_level("WARNING"):
            pick_intelligence_file(d, TERM)
        msg = caplog.text
        assert "IGNORING" in msg
        assert "supplier_patterns_2025 (3).json" in msg

    def test_the_unambiguous_case_stays_quiet(self, tmp_path, caplog):
        d = str(tmp_path)
        _write(d, "supplier_patterns_2025.json")
        with caplog.at_level("WARNING"):
            pick_intelligence_file(d, TERM)
        assert "IGNORING" not in caplog.text


class TestTheRealDataDirectory:

    def test_the_shipped_data_dir_resolves_to_the_canonical_patterns(self):
        """Guards the actual install: whatever duplicates accumulate in
        oasis/data, the canonical name must still win."""
        data = os.path.join(os.path.dirname(__file__), "..", "oasis", "data")
        if not os.path.isdir(data):
            pytest.skip("no data directory in this checkout")
        chosen = pick_intelligence_file(data, TERM)
        if chosen is None:
            pytest.skip("no supplier patterns file in this checkout")
        assert chosen in ("supplier_patterns_2025.json",
                          "supplier_patterns_2025_updated.json"), (
            "the engine would load %r — a duplicate is shadowing the real "
            "supplier rhythm again" % chosen)
