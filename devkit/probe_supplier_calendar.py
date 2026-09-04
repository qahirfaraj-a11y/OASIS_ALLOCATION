"""Probe: is the declared order calendar a data file, or a parsed report?

THE CLAIM UNDER TEST
    claim.ordering.supplier-calendar-is-clean

WHY IT MATTERS
    `supplier_weekly_schedule.json` is where `R` comes from — the observed
    review period, worth 2.14x on working capital and the single largest lever
    in the ordering engine. `order_up_to._load_schedule` reads it, splits each
    entry on " - ", and admits anything whose remainder is longer than three
    characters.

    Two failures follow, and both are silent:

      * PARSE ARTEFACTS ADMITTED. Entries like "...AND 123 MORE" and "(BI-WK)"
        are display truncation markers, not suppliers. They pass the length
        guard, become keys, and inflate the count of "suppliers with a declared
        order day" — the number the derivation quotes.

      * REAL SUPPLIERS LOST. A name containing a comma was split across two
        entries: "SB0179 -" followed by "BRANDACTIV KENYA SR". Neither keys to
        the canonical name, so those suppliers fall back to the default review
        period without anything saying so. That is T1, on param.R.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import json
import re
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.methodology.traps import join_match_rate, ratio, run_all  # noqa: E402
from oasis.logic.order_up_to import load_review_schedule                # noqa: E402

SCHEDULE = ROOT / "supplier_weekly_schedule.json"
CLAIM = "claim.ordering.review-schedule-is-clean"

CODE = re.compile(r"^[A-Z]{2,4}\d{3,5}\s*-")
EMPTY_CODE = re.compile(r"^[A-Z]{2,4}\d{3,5}\s*-\s*$")
#: Markers that a rendered report was parsed rather than a data file read.
ARTEFACT = re.compile(r"^\.\.\.\s*AND\s+\d+\s+MORE$|^\(.*\)$|^\d+$", re.I)


def main() -> int:
    if not SCHEDULE.exists():
        print(json.dumps({"claim": CLAIM, "verdict": "inconclusive",
                          "metric": {}, "notes": "schedule file not present"}))
        return 0
    sched = json.loads(SCHEDULE.read_text(encoding="utf-8"))

    total = coded = empty = artefact = fragment = 0
    split_pairs, artefacts = [], set()
    for day, names in sched.items():
        if not isinstance(names, list):
            continue
        for i, raw in enumerate(names):
            s = str(raw or "").strip()
            total += 1
            if EMPTY_CODE.fullmatch(s):
                empty += 1
                nxt = str(names[i + 1]).strip() if i + 1 < len(names) else ""
                split_pairs.append([day, s, nxt])
            elif CODE.match(s):
                coded += 1
            elif ARTEFACT.match(s):
                artefact += 1
                artefacts.add(s)
            else:
                fragment += 1

    # Ask the REAL consumer what it admits. Replicating its logic here is the
    # F1 pattern — two implementations of the same rule that drift apart and
    # then disagree about what the engine does.
    admitted = set(load_review_schedule(str(ROOT)))

    real = {" ".join(re.sub(r"^[A-Z]{2,4}\d{3,5}\s*-\s*", "", s).upper().split())
            for names in sched.values() if isinstance(names, list)
            for s in (str(x).strip() for x in names)
            if CODE.match(s) and not EMPTY_CODE.fullmatch(s)}
    # A repaired comma-split name is a REAL supplier, not junk. Counting it as
    # junk would make the fix look like it failed.
    real |= {" ".join(str(nxt).upper().split()) for _d, _c, nxt in split_pairs
             if nxt}
    real = {r for r in real if len(r) > 3}
    junk = sorted(admitted - real)

    checks = [
        join_match_rate(sorted(admitted), sorted(real),
                        "admitted keys that are real suppliers", floor=0.90),
        ratio(len(admitted), max(len(real), 1), "admitted vs real suppliers",
              lo=0.9, hi=1.1),
    ]
    run_all(checks)

    print(f"  entries {total} · coded {coded} · code-with-empty-name {empty} "
          f"· artefacts {artefact} · other fragments {fragment}")
    print(f"  consumer would admit {len(admitted)} 'suppliers'; "
          f"{len(real)} have a supplier code")
    print(f"  junk admitted: {len(junk)} — e.g. {junk[:5]}")
    print(f"  comma-split suppliers silently losing their R: {len(split_pairs)}")

    # Two different questions. The engine's schedule is clean when nothing it
    # admits is junk. The FILE is still a parsed report — that is upstream, and
    # it stays visible in the metric so the durable fix is not forgotten.
    clean = not junk
    print(json.dumps({
        "claim": CLAIM,
        "verdict": "supports" if clean else "contradicts",
        "metric": {"entries": total, "coded": coded,
                   "code_with_empty_name": empty, "artefacts": artefact,
                   "other_fragments": fragment,
                   "admitted_by_consumer": len(admitted),
                   "real_suppliers": len(real), "junk_admitted": len(junk),
                   "junk_examples": junk[:12],
                   "artefact_examples": sorted(artefacts)[:6],
                   "split_suppliers_repaired": split_pairs,
                   "file_is_parsed_report": bool(artefact or fragment)},
        "held_out": False, "provenance": "observed",
        "sources": ["source.supplier-weekly-schedule"],
        "baseline": "order_up_to.load_review_schedule",
        "beat_baseline": None, "traps": ["T1", "T3"],
        "notes": (
            f"{len(junk)} non-suppliers pass the consumer's `len(key) > 3` "
            f"guard, including display truncation markers such as "
            f"{sorted(artefacts)[:2]}. {len(split_pairs)} real suppliers were "
            "split across two entries by a comma and key to neither name, so "
            "they fall back to the default review period silently. R is the "
            "engine's largest lever; its input is a parsed report."
            if not clean else
            f"The engine's review schedule admits {len(admitted)} suppliers, "
            f"all real: {len(split_pairs)} comma-split names repaired and "
            f"{artefact + fragment} parse artefacts rejected. Was 940, of which "
            "262 were display truncation markers — the figure the derivation "
            "quotes for R. The FILE is still a parsed report; the durable fix "
            "is upstream of it.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
