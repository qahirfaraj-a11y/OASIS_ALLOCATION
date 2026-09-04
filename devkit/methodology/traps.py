"""The trap register — seven detectors for seven failures this repo already paid for.

Every one of these was found by a human noticing. Every one is mechanisable.
The register is Loop A's floor: it runs BEFORE a cycle is allowed to report a
finding, so an analysis cannot be wrong in a way we have already been wrong.

    T1 silent join failure      0 of 486 suppliers matched on case
    T2 stale duplicate shadow   "…(3).json" sorted first; lead times 3-7x
    T3 denominator sanity       43,405% overrun was 434x of KES 103
    T4 like-for-like            "40% deeper" was two different denominators
    T5 circularity              the ordering habit contaminating its own measure
    T6 hierarchy inversion      a clean-measuring fix that inverted the design
    T7 category error           an initial-load allocator judged as replenishment

Use them as guards inside probes:

    from devkit.methodology.traps import join_match_rate, ratio
    join_match_rate(derived.keys(), consumers.keys(), "supplier lookup").raise_if_violated()
"""
from __future__ import annotations

import ast
import os
import re
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Dict, Iterable, List, Optional, Sequence, Tuple

TRAPS = {
    "T1": "silent-join-failure",
    "T2": "stale-duplicate-shadowing",
    "T3": "denominator-sanity",
    "T4": "like-for-like",
    "T5": "circularity",
    "T6": "hierarchy-inversion",
    "T7": "category-error",
}


class TrapViolation(Exception):
    pass


@dataclass
class TrapResult:
    trap: str
    ok: bool
    detail: str
    evidence: Dict[str, Any] = field(default_factory=dict)

    def raise_if_violated(self) -> "TrapResult":
        if not self.ok:
            raise TrapViolation(f"[{self.trap}] {self.detail}")
        return self

    def __bool__(self) -> bool:
        return self.ok

    def __str__(self) -> str:
        return f"{'PASS' if self.ok else 'FAIL'} [{self.trap}] {self.detail}"


# ---------------------------------------------------------------- T1
def join_match_rate(left: Iterable[Any], right: Iterable[Any],
                    name: str = "join", floor: float = 0.60) -> TrapResult:
    """A join that matches almost nothing is a bug, not a finding.

    Also reports what the rate WOULD be under case- and whitespace-normalisation,
    because that is the shape the real failure took: the derivation wrote
    lower-case supplier names where every consumer looks up upper-case, and
    zero of 486 matched. Invisible, because nothing asserted the rate.
    """
    L = [str(x) for x in left]
    R = set(str(x) for x in right)
    if not L:
        return TrapResult("T1", False, f"{name}: left side is empty")
    hits = sum(1 for x in L if x in R)
    rate = hits / len(L)

    def norm(s: str) -> str:
        return re.sub(r"\s+", " ", s).strip().upper()

    Rn = {norm(x) for x in R}
    nhits = sum(1 for x in L if norm(x) in Rn)
    nrate = nhits / len(L)

    detail = f"{name}: {hits}/{len(L)} matched ({rate:.1%}), floor {floor:.0%}"
    if nrate - rate > 0.10:
        detail += (f" — but {nrate:.1%} would match case/whitespace-normalised. "
                   "This is a key-formatting bug, not a data gap.")
    return TrapResult("T1", rate >= floor, detail,
                      {"matched": hits, "total": len(L), "rate": rate,
                       "normalised_rate": nrate})


# ---------------------------------------------------------------- T2
_DUP_SUFFIX = re.compile(r"[ _\-]*\(\d+\)|[ _\-]copy(?: \d+)?$", re.I)


def newest_wins(paths: Sequence[os.PathLike | str],
                name: str = "load") -> TrapResult:
    """Pick a file explicitly, never by whatever the filesystem returned first.

    `"…_2025 (3).json"` sorts before `"…_2025.json"` because a space precedes a
    dot. A stale duplicate shadowed three weeks of derived data and inflated
    lead times 3-7x. The fix is not "sort better" — it is to notice that more
    than one candidate exists at all.
    """
    ps = [Path(p) for p in paths]
    if not ps:
        return TrapResult("T2", False, f"{name}: no candidate files")
    if len(ps) == 1:
        return TrapResult("T2", True, f"{name}: single candidate {ps[0].name}",
                          {"chosen": str(ps[0])})
    ranked = sorted(ps, key=lambda p: (p.stat().st_mtime if p.exists() else 0),
                    reverse=True)
    chosen, shadowed = ranked[0], ranked[1:]
    dupes = [p.name for p in shadowed if _DUP_SUFFIX.search(p.stem)]
    detail = (f"{name}: {len(ps)} candidates; chose {chosen.name} by mtime; "
              f"shadowed {', '.join(p.name for p in shadowed[:4])}")
    if dupes:
        detail += f" — {len(dupes)} look like accidental duplicates: {', '.join(dupes[:3])}"
    return TrapResult("T2", not dupes, detail,
                      {"chosen": str(chosen),
                       "shadowed": [str(p) for p in shadowed]})


def scan_unsorted_globs(root: os.PathLike | str,
                        include: Sequence[str] = ("*.py",),
                        only_high: bool = True) -> List[TrapResult]:
    """Static form of T2: a listdir/glob whose result is used to PICK ONE FILE.

    The distinction matters, and getting it wrong is how a register starts
    crying wolf. Iterating every match is safe — aggregating all GRN workbooks
    in a directory does not care what order they arrive in. Taking ``[0]``, or
    ``next(...)``, or ``max()`` without a key, means the filesystem chose which
    file backs a number, and that is exactly the bug that made KAMILI PACKERS
    carry 21 days against a measured 3.

    ``oasis/logic/order_engine.pick_intelligence_file`` is the hardened pattern:
    explicit precedence, newest-by-mtime as the fallback, ambiguity logged
    rather than resolved quietly. Sites flagged HIGH should route through it.
    """
    out: List[TrapResult] = []
    FINDERS = ("listdir", "glob", "iglob", "rglob", "scandir")

    for pattern in include:
        for path in sorted(Path(root).rglob(pattern)):
            if any(part in ("__pycache__", ".git", "build", "dist", "node_modules",
                            ".oasis_venv", "oasis_checkpoint_before_refactor")
                   for part in path.parts):
                continue
            try:
                src = path.read_text(encoding="utf-8", errors="ignore")
                tree = ast.parse(src)
            except (SyntaxError, ValueError):
                continue

            for fn in [n for n in ast.walk(tree)
                       if isinstance(n, (ast.FunctionDef, ast.AsyncFunctionDef,
                                         ast.Module))]:
                body = list(ast.walk(fn))

                # Names that hold the result of a finder call.
                held: Dict[str, int] = {}
                for node in body:
                    if isinstance(node, ast.Assign) and isinstance(node.value, ast.Call):
                        nm = (getattr(node.value.func, "attr", None)
                              or getattr(node.value.func, "id", None))
                        if nm in FINDERS:
                            for t in node.targets:
                                if isinstance(t, ast.Name):
                                    held[t.id] = node.lineno

                for node in body:
                    if not isinstance(node, ast.Call):
                        continue
                    nm = getattr(node.func, "attr", None) or getattr(node.func, "id", None)
                    if nm not in FINDERS:
                        continue

                    guarded = any(
                        isinstance(a, ast.Call)
                        and getattr(a.func, "id", None) in ("sorted", "max", "min")
                        and any(x is node for x in a.args)
                        for a in body)
                    if guarded:
                        continue

                    # Is the result used to pick ONE element?
                    picks = any(
                        (isinstance(a, ast.Subscript) and a.value is node)
                        or (isinstance(a, ast.Call)
                            and getattr(a.func, "id", None) == "next"
                            and a.args and _contains(a.args[0], node))
                        for a in body)

                    sev = "HIGH" if picks else "LOW"
                    if not picks:
                        for name, ln in held.items():
                            if ln != node.lineno:
                                continue
                            for a in body:
                                if (isinstance(a, ast.Subscript)
                                        and isinstance(a.value, ast.Name)
                                        and a.value.id == name):
                                    sev = "HIGH"
                                if (isinstance(a, ast.Call)
                                        and getattr(a.func, "id", None) == "next"
                                        and a.args and _contains(a.args[0], ast.Name(id=name))):
                                    sev = "HIGH"
                    if only_high and sev != "HIGH":
                        continue
                    verb = ("picks one file" if sev == "HIGH"
                            else "iterates all matches")
                    out.append(TrapResult(
                        "T2", False,
                        f"[{sev}] {path.as_posix()}:{node.lineno} — {nm}() "
                        f"{verb} without sorted()/mtime; filesystem order is "
                        "not a policy",
                        {"file": path.as_posix(), "line": node.lineno,
                         "severity": sev, "finder": nm}))
    # de-duplicate: a Module walk re-sees function bodies
    seen, uniq = set(), []
    for r in out:
        key = (r.evidence["file"], r.evidence["line"])
        if key not in seen:
            seen.add(key)
            uniq.append(r)
    return uniq


def _contains(haystack: ast.AST, needle: ast.AST) -> bool:
    if isinstance(needle, ast.Name):
        return any(isinstance(n, ast.Name) and n.id == needle.id
                   for n in ast.walk(haystack))
    return any(n is needle for n in ast.walk(haystack))


# ---------------------------------------------------------------- T3
def ratio(numerator: float, denominator: float, label: str,
          lo: float = 0.1, hi: float = 10.0,
          min_denominator: float = 1e-9) -> TrapResult:
    """Check the denominator before raising an alarm.

    A 43,405% budget overrun was 434x of one hundred and three shillings. Any
    ratio outside [lo, hi] must print both terms, so the reader can see whether
    the number is large or the base is small.
    """
    if abs(denominator) < min_denominator:
        return TrapResult("T3", False,
                          f"{label}: denominator is ~0 ({denominator!r}) — "
                          f"ratio undefined, numerator {numerator!r}",
                          {"numerator": numerator, "denominator": denominator})
    r = numerator / denominator
    inside = lo <= abs(r) <= hi
    detail = (f"{label}: {r:.4g}x  (numerator {numerator:,.4g} / "
              f"denominator {denominator:,.4g})")
    if not inside:
        detail += "  ← OUT OF BAND: state the base before calling this a finding"
    return TrapResult("T3", inside, detail,
                      {"ratio": r, "numerator": numerator,
                       "denominator": denominator})


# ---------------------------------------------------------------- T4
def like_for_like(a: Tuple[str, float], b: Tuple[str, float],
                  label: str = "comparison", tol: float = 1e-6) -> TrapResult:
    """Two ratios are only comparable if they share a denominator.

    "The engine is 40% deeper than the human book" was two ratios over
    different bases. Like-for-like they were the same.
    """
    (an, ad), (bn, bd) = a, b
    same = abs(ad - bd) <= tol * max(1.0, abs(ad), abs(bd))
    detail = (f"{label}: denominators {an}={ad:,.6g} vs {bn}={bd:,.6g}"
              + ("" if same else "  ← DIFFERENT BASES: not comparable as stated"))
    return TrapResult("T4", same, detail,
                      {"a": {"name": an, "denominator": ad},
                       "b": {"name": bn, "denominator": bd}})


# ---------------------------------------------------------------- T5
def non_circular(measure: str, inputs: Iterable[str],
                 downstream_of_behaviour: Iterable[str]) -> TrapResult:
    """A measure fed by the behaviour it measures proves only that it exists.

    The pattern that works is `--mode residual-cover`: cover carried against the
    gap the delivery actually had to span, taken AFTERWARDS, so the ordering
    habit cannot contaminate the score. This caught us twice before it was named.
    """
    ins, bad_set = set(inputs), set(downstream_of_behaviour)
    overlap = sorted(ins & bad_set)
    detail = (f"{measure}: inputs {sorted(ins)} vs behaviour-derived "
              f"{sorted(bad_set)}")
    if overlap:
        detail += (f"  ← CIRCULAR on {overlap}: take the measure afterwards, "
                   "against the gap that actually had to be spanned")
    return TrapResult("T5", not overlap, detail, {"overlap": overlap})


# ---------------------------------------------------------------- T6
def _inversions(order: Sequence[int]) -> int:
    n, c = len(order), 0
    for i in range(n):
        for j in range(i + 1, n):
            if order[i] > order[j]:
                c += 1
    return c


def rank_diff(before: Sequence[str], after: Sequence[str],
              hierarchy: Optional[Sequence[str]] = None,
              max_inversion_rate: float = 0.10,
              label: str = "weights") -> TrapResult:
    """A fix that measures clean can still invert the design.

    The rebuilt department weights scored well and halved the staple share
    (60.7% → 31.3%). A working guard read as a symptom. So: diff the resulting
    RANK ORDER against the hierarchy the operating docs encode, not just the loss.
    """
    pos = {k: i for i, k in enumerate(before)}
    common = [k for k in after if k in pos]
    if len(common) < 2:
        return TrapResult("T6", True, f"{label}: too few common items to rank")
    inv = _inversions([pos[k] for k in common])
    worst = len(common) * (len(common) - 1) / 2
    rate = inv / worst if worst else 0.0

    moved = [(k, pos[k], i) for i, k in enumerate(common)
             if abs(pos[k] - i) >= max(3, 0.2 * len(common))]
    detail = (f"{label}: {inv}/{int(worst)} inversions ({rate:.1%}); "
              f"{len(moved)} items moved materially")
    ok = rate <= max_inversion_rate

    if hierarchy:
        hp = {k: i for i, k in enumerate(hierarchy)}
        ranked = [k for k in after if k in hp]
        broken = [(ranked[i], ranked[i + 1]) for i in range(len(ranked) - 1)
                  if hp[ranked[i]] > hp[ranked[i + 1]]]
        if broken:
            ok = False
            detail += ("  ← HIERARCHY INVERTED: "
                       + ", ".join(f"{a} now above {b}" for a, b in broken[:3]))
    if not ok and "HIERARCHY" not in detail:
        detail += "  ← rank order moved more than a refinement should"
    return TrapResult("T6", ok, detail,
                      {"inversions": inv, "rate": rate,
                       "moved": [{"item": k, "from": f, "to": t} for k, f, t in moved[:10]]})


# ---------------------------------------------------------------- T7
REGIMES = ("initial-load", "replenishment", "transfer", "siting")


def regime(metric: str, declared: str, expected: str) -> TrapResult:
    """Declare which regime a metric belongs to, and refuse cross-regime judgement.

    Milk's wallet looked under-used at 11.8% — but fresh items are JIT-capped by
    design, so that wallet was never meant to be drained. The allocator was
    initial-load; the metric was replenishment. A working guard read as a symptom.
    """
    if declared not in REGIMES:
        return TrapResult("T7", False,
                          f"{metric}: undeclared regime {declared!r}; "
                          f"must be one of {REGIMES}")
    ok = declared == expected
    detail = f"{metric}: regime {declared!r} vs context {expected!r}"
    if not ok:
        detail += ("  ← CATEGORY ERROR: this metric's assumptions do not hold "
                   "in the regime it is being applied to")
    return TrapResult("T7", ok, detail,
                      {"declared": declared, "expected": expected})


# ---------------------------------------------------------------- runner
def run_all(results: Iterable[TrapResult], strict: bool = False) -> List[TrapResult]:
    rs = list(results)
    for r in rs:
        print("  " + str(r))
    failed = [r for r in rs if not r.ok]
    if failed and strict:
        raise TrapViolation(f"{len(failed)} trap(s) violated: "
                            + "; ".join(r.trap for r in failed))
    return rs
