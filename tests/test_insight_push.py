"""On-prem insight push runner: card assembly, batching, and safe degradation."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import insight_push as IP


def _poster_ok(seen):
    def poster(url, headers, body):
        seen.append((url, headers, body))
        n = len(body["insights"])
        return 200, {"accepted": n, "duplicates": 0, "store_id": "s1"}
    return poster


def test_push_requires_hub_config(monkeypatch):
    monkeypatch.delenv("OASIS_HUB_URL", raising=False)
    monkeypatch.delenv("OASIS_HUB_INGEST_TOKEN", raising=False)
    with pytest.raises(IP.InsightPushError):
        IP.push_cards([{"supplier_code": "X", "kind": "velocity", "payload": {}}])


def test_push_empty_is_noop():
    assert IP.push_cards([], hub_url="http://h", token="t")["accepted"] == 0


def test_push_batches_and_sends_bearer():
    seen = []
    cards = [{"supplier_code": "C", "kind": "velocity", "payload": {}}] * 5
    res = IP.push_cards(cards, hub_url="http://h/", token="tok",
                        poster=_poster_ok(seen), batch_size=2)
    assert res["accepted"] == 5 and res["batches"] == 3
    url, headers, _ = seen[0]
    assert url == "http://h/ingest/insights"
    assert headers["Authorization"] == "Bearer tok"


def test_push_raises_on_rejection():
    def poster(url, headers, body):
        return 401, {"detail": "bad token"}
    with pytest.raises(IP.InsightPushError):
        IP.push_cards([{"supplier_code": "C", "kind": "velocity", "payload": {}}],
                      hub_url="http://h", token="t", poster=poster)


def test_build_cards_from_scorecard_and_mande(tmp_path, monkeypatch):
    data_dir = tmp_path.as_posix()
    # a MANDE report on disk → SEI cards
    with open(os.path.join(data_dir, "mande_purge_report.json"), "w") as f:
        json.dump({"suppliers": [
            {"supplier": "COKE", "sei": 388000, "sku_count": 36,
             "classification": "Elite Stabilizer",
             "trapped_capital_kes": 500000},          # store-private
        ]}, f)
    # a scorecard → reliability cards
    monkeypatch.setattr(IP, "IE", IP.IE)
    import oasis.logic.supplier_scorecard as SS
    monkeypatch.setattr(SS, "load_patterns", lambda d: {"x": 1})
    monkeypatch.setattr(SS, "scorecard_rows", lambda p: [
        {"supplier": "COKE", "classification": "RELIABLE", "avg_lead_time": 3.0,
         "total_spend": 9_000_000},                    # store-private
    ])

    cards = IP.build_cards(data_dir, period="2026-07-18")
    kinds = {c["kind"] for c in cards}
    assert "reliability" in kinds and "sei" in kinds
    # store-private figures never made it into any card
    blob = json.dumps(cards)
    assert "total_spend" not in blob and "trapped_capital" not in blob
    # period makes the run idempotent
    assert any(c.get("source_ref", "").endswith("2026-07-18") for c in cards)


def test_build_cards_degrades_when_nothing_available(tmp_path):
    # no scorecard patterns, no MANDE report → no crash, just no cards
    assert IP.build_cards(tmp_path.as_posix()) == []


def test_run_dry_run_builds_without_pushing(tmp_path):
    res = IP.run(tmp_path.as_posix(), dry_run=True)
    assert res["pushed"] is False and res["built"] == 0


def test_build_cards_all_10_kinds(tmp_path, monkeypatch):
    data_dir = tmp_path.as_posix()

    # 1. Scorecard -> Reliability
    import oasis.logic.supplier_scorecard as SS
    monkeypatch.setattr(SS, "load_patterns", lambda d: {"x": 1})
    monkeypatch.setattr(SS, "scorecard_rows", lambda p: [
        {"supplier": "SUP1", "classification": "RELIABLE", "avg_lead_time": 2.0}
    ])

    # 2. MANDE -> SEI
    with open(os.path.join(data_dir, "mande_purge_report.json"), "w") as f:
        json.dump({"suppliers": [{"supplier": "SUP1", "sei": 100}]}, f)

    # 3. Velocity
    with open(os.path.join(data_dir, "velocity_report.json"), "w") as f:
        json.dump({"SUP1": [{"sku_code": "SKU1", "units": 50, "ads": 2.5}]}, f)

    # 4. Halo
    with open(os.path.join(data_dir, "basket_affinity.json"), "w") as f:
        json.dump({"SUP1": [{"anchor_sku": "SKU1", "attachment_sku": "SKU2", "lift": 1.5}]}, f)

    # 5. Reorder
    with open(os.path.join(data_dir, "reorder_lines.json"), "w") as f:
        json.dump({"SUP1": [{"sku_code": "SKU1", "suggested_units": 10}]}, f)

    # 6. Broken Halo
    with open(os.path.join(data_dir, "dharam_demand_patch.json"), "w") as f:
        json.dump({"broken_halos": {"SUP1": [{"anchor_sku": "SKU1", "attachment_sku": "SKU3", "drop_pct": 0.4}]}}, f)

    # 7. Archetype
    with open(os.path.join(data_dir, "archetype_mix.json"), "w") as f:
        json.dump({"SUP1": [{"archetype": "staple", "sku_count": 5}]}, f)

    # 8. Capital Efficiency
    with open(os.path.join(data_dir, "capital_efficiency.json"), "w") as f:
        json.dump({"SUP1": {"efficiency_index": 1.2, "band": "top quartile"}}, f)

    # 9. NCP
    with open(os.path.join(data_dir, "ncp_report.json"), "w") as f:
        json.dump({"SUP1": {"ncp_days": 10, "credit_days": 30, "dio_days": 20, "position": "funding"}}, f)

    # 10. Cannibalization
    with open(os.path.join(data_dir, "cannibalization.json"), "w") as f:
        json.dump({"SUP1": [{"sku_code": "SKU1", "cannibalization_rate": 0.2}]}, f)

    cards = IP.build_cards(data_dir, period="2026-07-26")
    kinds = {c["kind"] for c in cards}
    expected = {
        "reliability", "sei", "velocity", "halo", "reorder",
        "broken_halo", "archetype", "capital_efficiency", "ncp", "cannibalization"
    }
    assert kinds == expected
    assert len(cards) == 10

