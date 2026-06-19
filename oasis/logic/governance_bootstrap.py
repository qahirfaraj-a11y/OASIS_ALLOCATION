"""
Governance bootstrap (client onboarding, one command).

Orchestrates the four canonical governance engines in dependency order to
(re)generate the artifacts the OrderEngine reads:

    LATA  -> enriches supplier_patterns with lata_* (toxicity/variance)
    AMIT  -> amit_enforcement.json   (dead-stock blacklist + dept GMROI caps)
    MANDE -> mande_purge_report.json (suppliers flagged for delisting)
    DHARAM-> dharam_demand_patch.json(ghost-demand recovery patches)

LATA runs first because AMIT reads the lata variance multipliers it writes back
into supplier_patterns.

This is an **orchestrator** — it calls the real engines (run_lata / run_amit /
run_mande / run_dharam), so there is no risk of the rules diverging from the
canonical implementations. Inputs: ``data_dir`` (supplier_patterns + prts returns
+ engine config) and ``nn_path`` (the neutral_network_export graph dir). Each
step is guarded so one failure does not abort the rest.

    python entrypoint.py --mode bootstrap-governance

Pairs with ``--mode bootstrap-intel`` (demand + supplier intelligence) to make
the full intelligence layer regenerable from the client's data.
"""

from __future__ import annotations

import importlib
import os
from typing import List, Optional, Tuple

# (step name, module, fn, arg-builder(data_dir, nn_path) -> args tuple)
# Note LATA's signature is (data_dir, nn) while the graph engines are (nn, data_dir).
GOV_STEPS: List[Tuple[str, str, str, object]] = [
    ("lata", "lata_shield", "run_lata",
     lambda dd, nn: (dd, nn if (nn and os.path.isdir(nn)) else None)),
    ("amit", "amit_gatekeeper", "run_amit", lambda dd, nn: (nn, dd)),
    ("mande", "mande_triage", "run_mande", lambda dd, nn: (nn, dd)),
    ("dharam", "dharam_revenue", "run_dharam", lambda dd, nn: (nn, dd)),
]


def _call(module: str, fn: str, *args):
    """Import oasis.logic.<module> and call <fn>(*args). Isolated for testing."""
    mod = importlib.import_module(f"oasis.logic.{module}")
    return getattr(mod, fn)(*args)


def _summarize(result) -> object:
    """Keep the per-engine summary small for the report."""
    if isinstance(result, dict):
        return result.get("stats", result)
    return str(result)[:200]


def run_governance(data_dir: str, nn_path: Optional[str],
                   steps: Optional[List[str]] = None) -> dict:
    """Run LATA -> AMIT -> MANDE -> DHARAM in order (each guarded).

    Returns {<step>: {status, summary|error}, overall: OK|PARTIAL|FAILED}.
    """
    selected = set(steps) if steps else None
    results: dict = {}
    ran = 0
    for name, module, fn, argfn in GOV_STEPS:
        if selected is not None and name not in selected:
            continue
        ran += 1
        try:
            res = _call(module, fn, *argfn(data_dir, nn_path))
            results[name] = {"status": "OK", "summary": _summarize(res)}
        except Exception as e:
            results[name] = {"status": "FAILED", "error": str(e)[:200]}

    ok = sum(1 for v in results.values() if isinstance(v, dict) and v.get("status") == "OK")
    if ok == ran and ran > 0:
        overall = "OK"
    elif ok == 0:
        overall = "FAILED"
    else:
        overall = "PARTIAL"
    results["overall"] = overall
    return results
