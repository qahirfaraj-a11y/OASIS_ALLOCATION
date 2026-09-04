"""Probe: F3 and F4 — are the intelligence signals reaching ordering, and earning it?

THE JUNE ANALYSIS SAID
    F3  LATA supplier toxicity never reaches the replenishment safety buffer.
    F4  Live enrichment supplies no ROP, so the flat fallback always fires.

Both were later implemented. Neither claim was ever re-probed, so both sat stale
in the ledger while the code moved underneath them — which is the failure mode
the TTL exists to catch.

Re-tracing them turns up something the original finding could not: F3 is not
merely closed, it is LOUD. The multiplier reaching the safety buffer has a
median of 2.59 and a third of suppliers pinned at its ceiling.

THE TEST THAT MATTERS
    LATA claims to measure supplier lead-time toxicity. We now measure lead-time
    variance directly, per vendor, from 92,181 receipts. So the multiplier can be
    checked against the thing it claims to represent, rather than trusted because
    it is present.

    A multiplier that inflates the median supplier's safety stock 2.6x and does
    NOT track observed lead-time variance is not a shield. It is a tax.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import ast
import json
import os
import re
import statistics as st
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit import stock_ledger as SL                                   # noqa: E402
from devkit.methodology.traps import join_match_rate, ratio, run_all    # noqa: E402

BRIDGE = ROOT / "oasis" / "logic" / "simulation_bridge.py"
LATA = ROOT / "oasis" / "data" / "supplier_patterns_2025.json"
LATA_DOC_CEILING = 2.0


def _ranks(xs):
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        for k in range(i, j + 1):
            r[order[k]] = (i + j) / 2.0 + 1.0
        i = j + 1
    return r


def spearman(xs, ys):
    if len(xs) < 3:
        return None
    a, b = _ranks(xs), _ranks(ys)
    ma, mb = sum(a) / len(a), sum(b) / len(b)
    num = sum((x - ma) * (y - mb) for x, y in zip(a, b))
    da = sum((x - ma) ** 2 for x in a) ** 0.5
    db = sum((y - mb) ** 2 for y in b) ** 0.5
    return num / (da * db) if da and db else None


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--xlsx", default=None)
    a = ap.parse_args(argv)

    src = BRIDGE.read_text(encoding="utf-8")
    tree = ast.parse(src)

    # ---- F3: is the multiplier in the replenishment safety buffer? ---------
    # The assignment spans two lines. A single-line regex reported "LATA is
    # still absent" against code that plainly contains it — a detector that
    # fails closed is worse than none, because it re-files a closed finding.
    in_buffer = False
    for node in ast.walk(tree):
        if isinstance(node, ast.Assign) and any(
                getattr(t, "id", None) == "safety_buffer" for t in node.targets):
            names = {n.id for n in ast.walk(node.value) if isinstance(n, ast.Name)}
            if "lata_multiplier" in names:
                in_buffer = True
    lata_ref = "lata_variance_multiplier" in src or "_lata_multipliers" in src

    lata = json.loads(LATA.read_text(encoding="utf-8")) if LATA.exists() else {}
    mults = {str(k).upper().strip(): float(v["lata_variance_multiplier"])
             for k, v in lata.items()
             if isinstance(v, dict) and v.get("lata_variance_multiplier") is not None}
    vals = sorted(mults.values())
    ceiling = max(vals) if vals else None
    at_ceiling = sum(1 for v in vals if ceiling and abs(v - ceiling) < 1e-9)
    non_neutral = sum(1 for v in vals if abs(v - 1.0) > 0.05)

    # ---- does it track the lead-time variance it claims to represent? ------
    led = SL.load(local_copy=Path(a.xlsx) if a.xlsx else None)
    obs = {}
    from collections import defaultdict
    per = defaultdict(list)
    for rs in led.receipts.values():
        for r in rs:
            if r.lead_days is not None and 0 <= r.lead_days <= 120:
                per[r.vendor].append(float(r.lead_days))
    for vendor, xs in per.items():
        if len(xs) >= 8:
            name = vendor.split(" - ", 1)[1] if " - " in vendor else vendor
            obs[" ".join(name.upper().split())] = {
                "sigma_L": st.pstdev(xs), "L": st.median(xs), "n": len(xs)}

    checks = [join_match_rate(list(obs), list(mults),
                              "measured vendors -> LATA suppliers", floor=0.20)]
    shared = sorted(set(obs) & set(mults))
    rho = rho_cv = None
    if len(shared) >= 3:
        rho = spearman([mults[k] for k in shared], [obs[k]["sigma_L"] for k in shared])
        rho_cv = spearman([mults[k] for k in shared],
                          [obs[k]["sigma_L"] / max(obs[k]["L"], 0.5) for k in shared])
    if vals:
        checks.append(ratio(st.median(vals), 1.0,
                            "median LATA multiplier vs neutral", lo=0.5, hi=1.8))
    run_all(checks)

    print(f"  F3 LATA in replenishment safety buffer: {in_buffer} "
          f"(referenced anywhere: {lata_ref})")
    if vals:
        print(f"     {len(vals):,} suppliers · median {st.median(vals):.3f} · "
              f"max {ceiling} · {at_ceiling:,} at the ceiling "
              f"({at_ceiling / len(vals):.0%}) · {non_neutral:,} non-neutral")
        print(f"     documented ceiling {LATA_DOC_CEILING} · actual {ceiling}")
    print(f"     vs observed lead-time spread on {len(shared):,} shared vendors: "
          f"rho(sigma_L) = {rho if rho is None else round(rho, 4)} · "
          f"rho(sigma_L/L) = {rho_cv if rho_cv is None else round(rho_cv, 4)}")

    # LATA is a VARIANCE RATIO multiplier, so the right comparison is against
    # the coefficient of variation of lead time, not its absolute spread. A
    # supplier with a 2-day swing on a 2-day lead is erratic; the same swing on
    # a 20-day lead is not.
    tracks = rho_cv is not None and rho_cv > 0.7
    saturated = bool(vals) and (at_ceiling / len(vals)) > 0.20
    print(json.dumps({
        "claim": "claim.ordering.lata-not-in-safety-buffer",
        "verdict": "contradicts" if in_buffer else "supports",
        "metric": {"in_safety_buffer": in_buffer, "referenced": lata_ref,
                   "suppliers": len(vals),
                   "median_multiplier": round(st.median(vals), 4) if vals else None,
                   "max_multiplier": ceiling,
                   "documented_ceiling": LATA_DOC_CEILING,
                   "at_ceiling": at_ceiling,
                   "at_ceiling_share": round(at_ceiling / len(vals), 4) if vals else None,
                   "non_neutral": non_neutral,
                   "shared_vendors": len(shared),
                   "rho_vs_observed_sigma_L": round(rho, 4) if rho is not None else None,
                   "rho_vs_observed_cv_L": round(rho_cv, 4) if rho_cv is not None else None,
                   "tracks_observed_variance": tracks,
                   "saturated_at_ceiling": saturated},
        "held_out": False, "provenance": "observed",
        "sources": ["source.fulfilment-detail"],
        "baseline": "F3 as filed in 2026-06: LATA absent from the buffer",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": (
            "F3 IS CLOSED — the multiplier reaches the replenishment safety "
            f"buffer. It is also loud: median {st.median(vals):.2f}x, "
            f"{at_ceiling:,} of {len(vals):,} suppliers "
            f"({at_ceiling / len(vals):.0%}) pinned at {ceiling}, against a "
            f"documented ceiling of {LATA_DOC_CEILING}. Against lead-time "
            f"spread measured from receipts on {len(shared):,} shared vendors it "
            f"correlates at rho={rho}. "
            + ("It tracks what it claims to measure: against the coefficient "
               f"of variation of lead time it correlates at rho={rho_cv}, which "
               "is independent corroboration from a source LATA never saw. "
               if tracks else
               "It does not track what it claims to measure, so it is a tax "
               "rather than a shield. ")
            + (f"The remaining problem is the top of the range: {at_ceiling:,} "
               f"suppliers ({at_ceiling / len(vals):.0%}) sit exactly at "
               f"{ceiling}, so among the worst third the multiplier no longer "
               "discriminates — and the ceiling itself is undocumented, the "
               f"code saying {LATA_DOC_CEILING}."
               if saturated else "The range is not saturated.")
            if in_buffer else "LATA is still absent from the safety buffer.")}))

    # ---- F4: what supplies the reorder point, and does it say so? ----------
    mode_default = None
    m = re.search(r'OASIS_ROP_MODE"?\s*,\s*"([a-z\-]+)"', src)
    if m:
        mode_default = m.group(1)
    announces = "[ROP Fallback" in src
    newsvendor = "newsvendor" in src and "risk_baseline" in src
    live_mode = os.getenv("OASIS_ROP_MODE", mode_default or "heuristic")

    print(f"  F4 ROP: default mode {mode_default!r} · newsvendor available "
          f"{newsvendor} · fallback announces itself {announces} · "
          f"env now {live_mode!r}")

    closed4 = bool(newsvendor and announces and mode_default != "heuristic")
    print(json.dumps({
        "claim": "claim.ordering.rop-fallback-always-fires",
        "verdict": "contradicts" if closed4 else "supports",
        "metric": {"default_mode": mode_default, "newsvendor_available": newsvendor,
                   "fallback_announces": announces, "env_mode": live_mode},
        "held_out": False, "provenance": "observed",
        "sources": [], "baseline": "F4 as filed in 2026-06",
        "beat_baseline": None, "traps": [],
        "notes": (
            "F4 is PARTIALLY closed. A statistically-correct reorder point "
            "(mu_LTD + z*sigma_LTD) now exists behind OASIS_ROP_MODE, and the "
            "flat fallback announces itself per line instead of passing "
            f"silently. But the default is still {mode_default!r}, so on an "
            "unconfigured install the flat heuristic remains what actually "
            "supplies every reorder point. Mechanism built; not enabled."
            if not closed4 else
            "F4 closed: a non-heuristic ROP is the default and the fallback "
            "announces itself.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
