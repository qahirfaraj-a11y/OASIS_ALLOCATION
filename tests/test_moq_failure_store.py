"""Tests for the MOQ failure store (dedup, per-org replace, expiry, legacy cleanup)."""

import json
import os
import sys
from datetime import datetime, timedelta

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.moq_failure_store import record_moq_failures, load_moq_failures


def _items(*codes, qty=5):
    return [{"item_code": c, "recommended_quantity": qty} for c in codes]


def test_record_and_load_roundtrip(tmp_path):
    path = str(tmp_path / "moq.json")
    n = record_moq_failures(path, "ORG001", _items("SKU1", "SKU2"))
    assert n == 2
    failures = load_moq_failures(path)
    assert failures == {"ORG001": {"SKU1": 5.0, "SKU2": 5.0}}


def test_rerecord_replaces_org_entries(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1", "SKU2"))
    record_moq_failures(path, "ORG001", _items("SKU3"))
    failures = load_moq_failures(path)
    # SKU1/SKU2 resolved in the latest run — must be gone
    assert failures == {"ORG001": {"SKU3": 5.0}}


def test_empty_run_clears_org(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1"))
    record_moq_failures(path, "ORG001", [])
    assert load_moq_failures(path) == {}


def test_other_orgs_preserved(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", _items("SKU1"))
    record_moq_failures(path, "ORG002", _items("SKU9"))
    failures = load_moq_failures(path)
    assert failures["ORG001"] == {"SKU1": 5.0}
    assert failures["ORG002"] == {"SKU9": 5.0}


def test_repeated_renders_do_not_grow_file(tmp_path):
    path = str(tmp_path / "moq.json")
    for _ in range(25):  # simulate 25 Streamlit rerenders
        record_moq_failures(path, "ORG001", _items("SKU1", "SKU2"))
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    assert len(raw) == 2


def test_expired_entries_dropped(tmp_path):
    path = str(tmp_path / "moq.json")
    old_ts = (datetime.now() - timedelta(days=10)).isoformat()
    with open(path, "w", encoding="utf-8") as f:
        json.dump([{"org_cd": "ORG001", "itm_cd": "OLD", "qty": 1, "ts": old_ts}], f)
    assert load_moq_failures(path, max_age_days=7) == {}


def test_legacy_untimestamped_entries_dropped(tmp_path):
    """Entries from the old append-only era have no ts — first load cleans them."""
    path = str(tmp_path / "moq.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump([
            {"org_cd": "ORG001", "itm_cd": "LEGACY1", "qty": 1},
            {"org_cd": "ORG001", "itm_cd": "LEGACY2", "qty": 2},
        ] * 500, f)  # bloated legacy file
    assert load_moq_failures(path) == {}
    # And a new record physically purges them from disk
    record_moq_failures(path, "ORG002", _items("NEW1"))
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    assert len(raw) == 1


def test_corrupt_file_handled(tmp_path):
    path = str(tmp_path / "moq.json")
    with open(path, "w", encoding="utf-8") as f:
        f.write("{not valid json")
    assert load_moq_failures(path) == {}
    assert record_moq_failures(path, "ORG001", _items("SKU1")) == 1
    assert load_moq_failures(path) == {"ORG001": {"SKU1": 5.0}}


def test_missing_file_returns_empty(tmp_path):
    assert load_moq_failures(str(tmp_path / "nope.json")) == {}


def test_itm_cd_key_fallback(tmp_path):
    path = str(tmp_path / "moq.json")
    record_moq_failures(path, "ORG001", [{"itm_cd": "VIA_ITM_CD", "recommended_quantity": 3}])
    assert load_moq_failures(path) == {"ORG001": {"VIA_ITM_CD": 3.0}}
