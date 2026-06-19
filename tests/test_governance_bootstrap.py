"""Tests for the governance bootstrap orchestrator (canonical engines mocked)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import governance_bootstrap as G


class TestRunGovernance:
    def test_runs_all_in_order_and_passes_args(self, monkeypatch):
        calls = []

        def fake_call(module, fn, *args):
            calls.append((module, fn, args))
            return {"stats": {module: "ok"}}

        monkeypatch.setattr(G, "_call", fake_call)
        out = G.run_governance("DATA", "NN")
        order = [m for m, _, _ in calls]
        assert order == ["lata_shield", "amit_gatekeeper", "mande_triage", "dharam_revenue"]
        assert out["overall"] == "OK"
        # LATA arg order is (data_dir, nn); graph engines are (nn, data_dir)
        lata_args = calls[0][2]
        amit_args = calls[1][2]
        assert lata_args[0] == "DATA"           # data_dir first for LATA
        assert amit_args == ("NN", "DATA")       # nn first for AMIT

    def test_lata_nn_none_when_not_a_dir(self, monkeypatch):
        calls = []
        monkeypatch.setattr(G, "_call", lambda m, f, *a: calls.append((m, a)) or {})
        # NN path that isn't a directory -> LATA receives None for nn
        G.run_governance("DATA", "/no/such/dir", steps=["lata"])
        assert calls[0] == ("lata_shield", ("DATA", None))

    def test_one_failure_is_partial(self, monkeypatch):
        def fake_call(module, fn, *args):
            if module == "amit_gatekeeper":
                raise RuntimeError("boom")
            return {"stats": {}}
        monkeypatch.setattr(G, "_call", fake_call)
        out = G.run_governance("D", "N")
        assert out["amit"]["status"] == "FAILED"
        assert "boom" in out["amit"]["error"]
        assert out["overall"] == "PARTIAL"

    def test_all_fail_is_failed(self, monkeypatch):
        def boom(*a, **k):
            raise RuntimeError("x")
        monkeypatch.setattr(G, "_call", boom)
        assert G.run_governance("D", "N")["overall"] == "FAILED"

    def test_steps_filter(self, monkeypatch):
        calls = []
        monkeypatch.setattr(G, "_call", lambda m, f, *a: calls.append(m) or {})
        G.run_governance("D", "N", steps=["mande", "dharam"])
        assert calls == ["mande_triage", "dharam_revenue"]
