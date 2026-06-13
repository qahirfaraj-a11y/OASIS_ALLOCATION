"""Tests for the journey state store (oasis/logic/journey_state.py)."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import journey_state as JS


class TestPureHelpers:
    def test_phase_to_mode_progression(self):
        assert JS.phase_to_mode(0) == "SETUP"
        assert JS.phase_to_mode(1) == "SHADOW"
        assert JS.phase_to_mode(2) == "ACTIVE"
        assert JS.phase_to_mode(4) == "ACTIVE"
        assert JS.phase_to_mode(5) == "AUTONOMOUS"
        assert JS.phase_to_mode(6) == "AUTONOMOUS"

    def test_phase_to_mode_clamps(self):
        assert JS.phase_to_mode(-3) == "SETUP"
        assert JS.phase_to_mode(99) == "AUTONOMOUS"

    def test_phase_name(self):
        assert JS.phase_name(0) == "Diagnose"
        assert JS.phase_name(6) == "Sustain"

    def test_next_phase(self):
        assert JS.next_phase(0) == 1
        assert JS.next_phase(5) == 6
        assert JS.next_phase(6) is None

    def test_default_state_shape(self):
        s = JS.default_state()
        assert s["phase"] == 0
        assert s["mode"] == "SETUP"
        assert s["value_recovered"] == 0.0


class TestPersistence:
    def test_load_missing_returns_default(self, tmp_path):
        assert JS.load_state(str(tmp_path / "nope.json"))["phase"] == 0

    def test_save_and_load_roundtrip(self, tmp_path):
        p = str(tmp_path / "j.json")
        JS.save_state({"phase": 3, "value_recovered": 1500.0}, p)
        s = JS.load_state(p)
        assert s["phase"] == 3
        assert s["mode"] == "ACTIVE"          # re-derived
        assert s["phase_name"] == "Fund"      # re-derived
        assert s["value_recovered"] == 1500.0

    def test_normalize_rederives_mode_from_phase(self, tmp_path):
        # A file with a stale/incorrect mode is corrected on load.
        p = str(tmp_path / "j.json")
        with open(p, "w", encoding="utf-8") as f:
            json.dump({"phase": 5, "mode": "SHADOW"}, f)
        assert JS.load_state(p)["mode"] == "AUTONOMOUS"

    def test_corrupt_file_returns_default(self, tmp_path):
        p = str(tmp_path / "j.json")
        with open(p, "w", encoding="utf-8") as f:
            f.write("{not json")
        assert JS.load_state(p)["phase"] == 0

    def test_set_value_recovered(self, tmp_path):
        p = str(tmp_path / "j.json")
        JS.set_value_recovered(250000, target=1000000, path=p)
        s = JS.load_state(p)
        assert s["value_recovered"] == 250000.0
        assert s["value_target"] == 1000000.0


class TestAdvance:
    def test_advance_moves_phase_and_records(self, tmp_path):
        p = str(tmp_path / "j.json")
        JS.save_state({"phase": 1}, p)
        s = JS.advance_phase("ops_admin", p)
        assert s["phase"] == 2
        assert s["mode"] == "ACTIVE"
        assert s["updated_by"] == "ops_admin"
        assert s["updated_dt"] is not None

    def test_advance_at_final_is_noop(self, tmp_path):
        p = str(tmp_path / "j.json")
        JS.save_state({"phase": 6}, p)
        s = JS.advance_phase("ops_admin", p)
        assert s["phase"] == 6  # cannot advance past Sustain

    def test_advance_never_skips(self, tmp_path):
        p = str(tmp_path / "j.json")
        JS.save_state({"phase": 0}, p)
        JS.advance_phase("u", p)
        assert JS.load_state(p)["phase"] == 1  # one step only
