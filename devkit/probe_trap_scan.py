"""Probe: does any live loader let the filesystem choose which file backs a number?

THE CLAIM UNDER TEST
    `claim.ordering.grn-cache-load-is-deterministic` — that the GRN intelligence
    cache, which is where lead times come from, is loaded deterministically.

WHY THIS PROBE EXISTS
    `oasis/logic/order_engine.pick_intelligence_file` was written specifically to
    kill this bug, and its docstring names the exact failure: a Windows Explorer
    copy leaves `supplier_patterns_2025 (3).json` beside the canonical file, the
    space sorts first, and the engine silently loads a three-week-old duplicate.
    KAMILI PACKERS carried 21 days against a measured 3 — a 7x inflation feeding
    straight into `gap + lead + safety`.

    Writing the hardened loader does not migrate the call sites. This probe
    checks whether any remain.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from devkit.methodology.traps import scan_unsorted_globs  # noqa: E402

ROOT = Path(__file__).resolve().parents[1]
CLAIM = "claim.ordering.grn-cache-load-is-deterministic"


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--path", default="oasis")
    a = ap.parse_args(argv)

    hits = scan_unsorted_globs(ROOT / a.path, include=("*.py",), only_high=True)
    for h in hits:
        print("  " + h.detail)

    sites = [{"file": h.evidence["file"], "line": h.evidence["line"],
              "finder": h.evidence["finder"]} for h in hits]
    grn = [s for s in sites if "order_engine" in s["file"]]

    print(json.dumps({
        "claim": CLAIM,
        "verdict": "contradicts" if hits else "supports",
        "metric": {"high_severity_sites": len(hits),
                   "in_order_engine": len(grn), "sites": sites[:20]},
        "held_out": False,
        "baseline": "pick_intelligence_file (the hardened loader)",
        "beat_baseline": None,
        "traps": ["T2"],
        "notes": (f"{len(hits)} live site(s) still let filesystem order choose "
                  "which file backs a number. The hardened loader exists; these "
                  "call sites were never migrated to it."
                  if hits else
                  "No site picks a single file by filesystem order."),
    }))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
