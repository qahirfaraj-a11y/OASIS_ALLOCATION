"""Engine config two-tier resolution (deep-analysis finding S1).

The bug these lock down: on a client install `oasis_engines_config.json` does not
ship, every consumer fell back to {}, `is_engine_enabled()` answered False for
every engine, and the whole Chapter-11 layer sat dormant without a word.
"""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import engines_config as EC
from oasis.logic import release_packager as RP


# ── priority chain (pure, injected exists) ───────────────────────────────
def test_live_config_wins_over_default():
    seen = {os.path.join("d", EC.LIVE_FILE), os.path.join("d", EC.DEFAULT_FILE)}
    tier, path = EC.resolve_source("d", exists=lambda p: p in seen)
    assert tier == "live"
    assert path.endswith(EC.LIVE_FILE)


def test_default_answers_when_no_live_config():
    seen = {os.path.join("d", EC.DEFAULT_FILE)}
    tier, path = EC.resolve_source("d", exists=lambda p: p in seen)
    assert tier == "default", "a client install must fall back to shipped defaults"
    assert path.endswith(EC.DEFAULT_FILE)


def test_nothing_found_is_reported_not_guessed():
    assert EC.resolve_source("d", exists=lambda p: False) == (None, None)


def test_package_data_dir_is_searched_as_a_fallback():
    """A relocated OASIS_DATA_DIR must still find the shipped defaults."""
    paths = [p for _t, p in EC.candidate_paths("/somewhere/else")]
    assert any(os.path.join("oasis", "data", EC.DEFAULT_FILE) in p for p in paths)


# ── loading ──────────────────────────────────────────────────────────────
def _write(d, name, payload):
    p = os.path.join(d, name)
    with open(p, "w", encoding="utf-8") as f:
        json.dump(payload, f)
    return p


def test_engines_are_enabled_from_the_default_file(tmp_path):
    d = tmp_path.as_posix()
    _write(d, EC.DEFAULT_FILE, {"engines": {"lata": {"enabled": True,
                                                     "max_variance_multiplier": 2.0}}})
    assert EC.is_engine_enabled("lata", d) is True
    assert EC.engine_params("lata", d)["max_variance_multiplier"] == 2.0


def test_tuned_config_overrides_the_default(tmp_path):
    d = tmp_path.as_posix()
    _write(d, EC.DEFAULT_FILE, {"engines": {"amit": {"enabled": True}}})
    _write(d, EC.LIVE_FILE, {"engines": {"amit": {"enabled": False}}})
    assert EC.is_engine_enabled("amit", d) is False


def test_corrupt_tuned_config_falls_through_to_default(tmp_path):
    """A broken tuned file must not switch the engine layer off."""
    d = tmp_path.as_posix()
    with open(os.path.join(d, EC.LIVE_FILE), "w", encoding="utf-8") as f:
        f.write("{not json")
    _write(d, EC.DEFAULT_FILE, {"engines": {"mande": {"enabled": True}}})
    assert EC.is_engine_enabled("mande", d) is True


def test_missing_engine_is_false_not_an_error(tmp_path):
    d = tmp_path.as_posix()
    _write(d, EC.DEFAULT_FILE, {"engines": {}})
    assert EC.is_engine_enabled("nope", d) is False
    assert EC.engine_params("nope", d) == {}


# ── preflight ────────────────────────────────────────────────────────────
def test_preflight_fails_when_no_config_exists():
    row = EC.evaluate_engines_config(None)
    assert row["status"] == "FAIL"
    assert "disabled" in row["detail"]


def test_preflight_warns_when_config_enables_nothing():
    assert EC.evaluate_engines_config("live", [])["status"] == "WARN"


def test_preflight_passes_on_defaults_but_names_them():
    row = EC.evaluate_engines_config("default", ["amit", "lata"])
    assert row["status"] == "PASS"
    assert "default" in row["detail"] and "amit" in row["detail"]


def test_preflight_distinguishes_tuned_from_default():
    tuned = EC.evaluate_engines_config("live", ["amit"])["detail"]
    default = EC.evaluate_engines_config("default", ["amit"])["detail"]
    assert tuned != default, "the operator must be able to tell tuned from untuned"


# ── the shipped artifact ─────────────────────────────────────────────────
def test_default_config_ships_in_whitelist_mode():
    ok, _why = RP.should_ship_clean("oasis/data/" + EC.DEFAULT_FILE, 6000)
    assert ok, "the engine defaults must reach the client zip"


def test_default_config_ships_in_blacklist_mode():
    ok, _why = RP.should_ship("oasis/data/" + EC.DEFAULT_FILE, 6000)
    assert ok, "the oasis/data/*.json glob must not eat the shipped default"


def test_tuned_config_still_never_ships():
    """Machine state stays machine state."""
    for fn in (RP.should_ship_clean, RP.should_ship):
        ok, _why = fn("oasis/data/" + EC.LIVE_FILE, 6000)
        assert not ok, f"{fn.__name__} must not ship the tuned per-install config"


def test_real_default_file_exists_and_enables_engines():
    """The actual file in this repo — the thing clients will receive."""
    root = os.path.join(os.path.dirname(__file__), "..")
    p = os.path.join(root, "oasis", "data", EC.DEFAULT_FILE)
    assert os.path.exists(p), "the shipped default config is missing from the repo"
    with open(p, encoding="utf-8") as f:
        cfg = json.load(f)
    enabled = [k for k, v in cfg["engines"].items()
               if isinstance(v, dict) and v.get("enabled")]
    assert enabled, "shipped defaults must actually enable the engine layer"
    for name in ("amit", "lata", "dharam", "mande"):
        assert name in cfg["engines"], f"{name} missing from shipped defaults"


def test_shipped_default_has_no_mojibake():
    """The tuned file carried cp1252-mangled em-dashes into the UI."""
    root = os.path.join(os.path.dirname(__file__), "..")
    with open(os.path.join(root, "oasis", "data", EC.DEFAULT_FILE),
              encoding="utf-8") as f:
        raw = f.read()
    assert "â€" not in raw, "mojibake em-dash survived into the shipped default"
