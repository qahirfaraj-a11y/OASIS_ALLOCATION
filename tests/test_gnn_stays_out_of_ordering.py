"""The GNN may not move a purchase order until it has earned it.

gnn_service already proves the FUNCTION obeys the gate: with
OASIS_GNN_ORDERING_WEIGHT unset, a GNN score of 0.99 is ignored and
ordering_risk returns inventory_risk exactly (test_gnn_service.py).

Nothing asserted the other half -- that the switch is not quietly thrown
somewhere in shipped configuration. A single `set OASIS_GNN_ORDERING_WEIGHT=0.5`
in a launcher would satisfy every existing test and still put an unvalidated
model into live PO quantities.

The GNN is a static dead-feature prior that has not beaten a baseline on a
real-outcome backtest. Until it does, the weight stays 0 and this is what says
so out loud.
"""
import os
import re

import pytest

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
VAR = "OASIS_GNN_ORDERING_WEIGHT"

#: Where a value could be set for a real run. Tests and devkit are excluded on
#: purpose: they SHOULD be able to switch it on to measure what it would do.
SHIPPED = (".bat", ".env", ".json", ".yaml", ".yml", ".toml", ".cfg", ".ini")
SKIP_DIRS = {".git", "tests", "devkit", "__pycache__", ".oasis_venv",
             "dist_release", "oasis_checkpoint_before_refactor", "test_oasis",
             "Allocation_Engine_Release", "node_modules", "oasis_vault"}

#: `set VAR=`, `VAR=`, `"VAR": ` — anything that assigns a value.
ASSIGN = re.compile(
    r"(?:^|[\s;&|])(?:set\s+)?[\"']?" + VAR + r"[\"']?\s*[:=]\s*[\"']?([^\s\"',}]+)",
    re.IGNORECASE | re.MULTILINE)


def _shipped_files():
    for base, dirs, files in os.walk(ROOT):
        dirs[:] = [d for d in dirs if d not in SKIP_DIRS]
        for fn in files:
            if fn.endswith(SHIPPED) or fn.endswith(".py"):
                yield os.path.join(base, fn)


def test_the_ordering_weight_is_not_switched_on_anywhere_shipped():
    offenders = []
    for path in _shipped_files():
        try:
            with open(path, "r", encoding="utf-8", errors="ignore") as f:
                text = f.read()
        except OSError:
            continue
        if VAR not in text:
            continue
        for m in ASSIGN.finditer(text):
            val = m.group(1).strip().strip('"\'')
            # getenv(VAR, "0") is the DEFAULT, not a setting -- it is the gate.
            if val in ("0", "0.0", '"0"', ""):
                continue
            offenders.append((os.path.relpath(path, ROOT), val))
    assert not offenders, (
        f"{VAR} is assigned a non-zero value in shipped configuration: "
        f"{offenders}. That puts an unvalidated GNN into live purchase order "
        f"quantities. It stays 0 until the model beats a baseline on a "
        f"real-outcome backtest.")


def test_the_runtime_default_is_zero_with_a_clean_environment(monkeypatch):
    monkeypatch.delenv(VAR, raising=False)
    from oasis.logic import gnn_service as G
    assert G._ordering_gnn_weight() == 0.0


def test_a_confident_gnn_still_moves_nothing(monkeypatch):
    """The consequence, stated as a number rather than a policy."""
    monkeypatch.delenv(VAR, raising=False)
    from oasis.logic import gnn_service as G
    products = [{"avg_daily_sales": 5.0, "current_stocks": 0.0}]
    assert G.ordering_risk(products, gnn_risk_score=0.99) == pytest.approx(
        G.inventory_risk(products))


def test_a_malformed_weight_fails_closed(monkeypatch):
    """Fail-closed: an unparseable value must not become an open gate."""
    from oasis.logic import gnn_service as G
    for bad in ("", "  ", "abc", "None", "-1"):
        monkeypatch.setenv(VAR, bad)
        assert G._ordering_gnn_weight() == 0.0, f"{bad!r} opened the gate"
