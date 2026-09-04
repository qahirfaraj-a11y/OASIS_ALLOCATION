---
name: trap-register
description: The seven mechanical detectors for failures this repo has already paid for — silent joins, stale duplicates, denominators, like-for-like, circularity, hierarchy inversion, category errors. Use before reporting any finding, and as guards inside probes.
---

# The trap register

Seven failures, each found once by a human noticing, each now mechanisable.
`devkit/methodology/traps.py`. Run them; do not remember them.

## T1 · silent join failure

The derivation wrote lower-case supplier names where every consumer looks up
upper-case. **Zero of 486 matched.** Invisible, because nothing asserted the rate.

```python
join_match_rate(derived_keys, consumer_keys, "supplier lookup", floor=0.60)
```

Also reports what the rate would be case/whitespace-normalised — the diagnostic
that names the bug rather than the symptom.

## T2 · stale duplicate shadowing

The loader took whatever `os.listdir` returned first, and `"…_2025 (3).json"`
sorts before `"…_2025.json"` because a space precedes a dot. Lead times inflated
**3–7x**.

```python
newest_wins(candidate_paths, "lead time cache")     # at load time
scan_unsorted_globs("oasis")                        # static sweep
```

The fix is not "sort better" — it is noticing that more than one candidate exists.

## T3 · denominator sanity

A **43,405% budget overrun** was 434x of one hundred and three shillings.

```python
ratio(numerator, denominator, "budget overrun", lo=0.1, hi=10.0)
```

Anything outside the band must print both terms.

## T4 · like-for-like

"The engine is 40% deeper than the human book" was two ratios over different
denominators. Like-for-like they were the same.

```python
like_for_like(("engine", engine_denom), ("human", human_denom), "depth")
```

## T5 · circularity

A measure fed by the behaviour it measures proves only that the behaviour exists.
This caught us **twice** before it was named.

```python
non_circular("residual cover", inputs, downstream_of_behaviour=["order_gaps"])
```

The pattern that works is `--mode residual-cover`: cover carried against the gap
the delivery actually had to span, taken **afterwards**.

## T6 · hierarchy inversion

A fix that measures clean can still invert the design. The rebuilt department
weights scored well and halved the staple share (60.7% → 31.3%).

```python
rank_diff(before, after, hierarchy=["staples", "fast_five", "general"])
```

Read the operating docs before changing weights that encode a hierarchy.

## T7 · category error

Greenfield allocation is **initial load**, not replenishment. Milk's wallet looked
under-used at 11.8% — fresh is JIT-capped by design, so it was never meant to be
drained. A working guard read as a symptom.

```python
regime("wallet_utilisation", declared="replenishment", expected="initial-load")
```

Every metric declares its regime: `initial-load`, `replenishment`, `transfer`, `siting`.

## Using them

```python
from devkit.methodology.traps import run_all, join_match_rate, ratio
run_all([...], strict=True)          # raises TrapViolation on any failure
result.raise_if_violated()           # or guard individually
```

Record the codes of the traps that actually ran in the probe's verdict. The gate
checks for `T6` before it will pass a rank change.
