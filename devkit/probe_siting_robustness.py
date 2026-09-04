"""Probe: which unfitted siting constant actually moves the recommendation?

`devkit/siting_robustness.py` already answered this once and wrote one JSON that
nobody re-read. This wraps it so the answer becomes a SERIES: a recommendation
whose stability is falling is a signal even with no outcome label, and one run
cannot show a fall.

WHAT IT SETTLES
    Every siting constant is unfitted, and the graph treated all four as equally
    exposed. They are not. Perturbing each in turn against a real shortlist
    orders them by how far the ANSWER moves, which is the ordering of what to
    fix first — backed by numbers rather than by which defect is most
    embarrassing.

    This is a stability measure, not a validation. It says which constant the
    recommendation is sensitive to; it cannot say which value is right, because
    no observed store-performance label exists on this install. Verdicts here
    are therefore always provenance-limited.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import json
import statistics as st
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.methodology.traps import ratio, run_all      # noqa: E402

RESULT = ROOT / "devkit" / "siting_robustness.json"

#: condition prefix -> the claim whose constant it perturbs
#: condition prefix -> the SENSITIVITY claim it tests.
#:
#: Deliberately not the "unfitted" claims. Whether the answer moves when a
#: constant changes, and whether that constant was ever fitted to data, are two
#: different propositions. An earlier version of this probe tested the second
#: and reported the first, which falsified "SIZE_EXPONENT was never fitted" on
#: the evidence that the recommendation is insensitive to it — a non-sequitur.
FAMILY = {
    "beta": "claim.siting.recommendation-sensitive-to-distance-decay",
    "alpha": "claim.siting.recommendation-sensitive-to-size-exponent",
    "catchment": "claim.siting.recommendation-sensitive-to-catchment-km",
}


def summarise(conds: dict, prefix: str) -> dict:
    rows = {k: v for k, v in conds.items() if k.startswith(prefix + " ")}
    if not rows:
        return {}
    held = sum(1 for v in rows.values() if v.get("top1_held"))
    ov = [v.get("top10_overlap", 0) for v in rows.values()]
    dr = [v.get("rank_drift", 0) for v in rows.values()]
    return {"conditions": len(rows), "top1_held": held,
            "top1_held_rate": round(held / len(rows), 3),
            "min_top10_overlap": min(ov),
            "mean_top10_overlap": round(st.mean(ov), 2),
            "max_rank_drift": max(dr), "mean_rank_drift": round(st.mean(dr), 3)}


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--chain", default="Chandarana")
    ap.add_argument("--pool", type=int, default=60)
    ap.add_argument("--rerun", action="store_true")
    a = ap.parse_args(argv)

    if a.rerun or not RESULT.exists():
        subprocess.run([sys.executable, str(ROOT / "devkit" / "siting_robustness.py"),
                        "--chain", a.chain, "--pool", str(a.pool), "--workers", "2"],
                       cwd=str(ROOT), capture_output=True, text=True)
    if not RESULT.exists():
        print(json.dumps({"claim": "claim.siting.catchment-km-unfitted",
                          "verdict": "inconclusive", "metric": {},
                          "notes": "robustness run produced no result"}))
        return 0

    data = json.loads(RESULT.read_text(encoding="utf-8"))
    conds = data.get("conditions", {})
    fams = {p: summarise(conds, p) for p in FAMILY}
    rivals = {k: v for k, v in conds.items() if k.startswith("drop ")}
    rival_held = (sum(1 for v in rivals.values() if v.get("top1_held"))
                  / len(rivals)) if rivals else None

    ranked = sorted((p for p in fams if fams[p]),
                    key=lambda p: -fams[p]["mean_rank_drift"])
    print(f"  chain {data.get('chain')} · pool {data.get('pool')} · "
          f"{len(conds)} conditions")
    for p in ranked:
        f = fams[p]
        print(f"    {p:<10} top-1 held {f['top1_held']}/{f['conditions']} · "
              f"min top-10 {f['min_top10_overlap']}/10 · "
              f"mean drift {f['mean_rank_drift']}")
    if rival_held is not None:
        print(f"    competitor field: top-1 survived {rival_held:.0%} of "
              f"{len(rivals)} random rival drops")

    checks = []
    if len(ranked) >= 2:
        hi, lo = fams[ranked[0]], fams[ranked[-1]]
        checks.append(ratio(max(hi["mean_rank_drift"], 1e-9),
                            max(lo["mean_rank_drift"], 1e-9),
                            f"drift: {ranked[0]} vs {ranked[-1]}", lo=0.1, hi=50))
    run_all(checks)

    worst = ranked[0] if ranked else None
    for prefix, claim in FAMILY.items():
        f = fams.get(prefix) or {}
        if not f:
            continue
        # A constant the answer is INSENSITIVE to is not urgent, even unfitted.
        sensitive = f["top1_held_rate"] < 1.0 or f["mean_rank_drift"] >= 0.5
        print(json.dumps({
            "claim": claim,
            "verdict": "supports" if sensitive else "contradicts",
            "metric": {**f, "family": prefix, "is_worst": prefix == worst,
                       "ranked_by_drift": ranked,
                       "competitor_top1_survival": rival_held},
            "held_out": False, "provenance": "synthetic",
            "sources": ["source.stores-network-json", "source.pos-fixtures"],
            "baseline": "baseline assumptions, unperturbed",
            "beat_baseline": None, "traps": ["T3"],
            "notes": (
                f"Perturbing {prefix} moves the recommendation: top-1 held in "
                f"{f['top1_held']}/{f['conditions']} conditions, min top-10 "
                f"overlap {f['min_top10_overlap']}/10, mean rank drift "
                f"{f['mean_rank_drift']}."
                + (f" Largest mover of the three — fix this one first."
                   if prefix == worst else "")
                if sensitive else
                f"The recommendation is INSENSITIVE to {prefix} across the swept "
                f"range: top-1 held in every condition, mean rank drift "
                f"{f['mean_rank_drift']}. Still unfitted, but not urgent — an "
                "unfitted constant the answer does not depend on is a different "
                "problem from one it does.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
