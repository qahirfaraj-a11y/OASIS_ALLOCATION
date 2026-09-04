---
description: Run one outcome-fed correction pass (Loop B) over a window of realised decisions
argument-hint: "[window, e.g. 90d]"
allowed-tools: Read, Write, Edit, Bash, Task
---

Run one **Loop B** pass over window `$1` (default 90 days).

## 1 · Is the loop open at the observation step?

```bash
python oasis/logic/decision_ledger.py --unresolved
```

Decisions with no outcome are the loop's real constraint. A growing number here
means the problem is observation, not analysis — say so and stop.

## 2 · Score

Primary scorer is **residual cover**: for each delivery, the cover carried against
the gap it actually had to span, taken *afterwards* so the ordering habit cannot
contaminate it. The book scores **1.9x**.

Guard it: `traps.non_circular("residual cover", inputs, downstream_of_behaviour=[...])`.

## 3 · Attribute

Decompose the residual-cover error across the formula's own terms:

```
error ≈ ∂/∂R·ΔR  +  ∂/∂L·ΔL  +  ∂/∂σ_L·Δσ_L  +  ∂/∂z·Δz
```

**Sanity check the attribution before trusting it:** it must reproduce the known
ranking — `R` 2.14x, `σ_L` the larger variance term, `z` only 1.52x. An
attribution that cannot recover what is already established cannot rank anything new.

## 4 · Propose

Ranked by measured worth. Each proposal names the parameter, the change, the
expected effect, and the evidence.

## 5 · Gate

`/gate` each proposal. Nothing ships without all four checks. Anything touching
`param.R` is `REQUIRES_HUMAN_COMMITMENT` — propose, never apply.

## Siting closes differently

Siting has **no outcome labels**. Do not attribute siting error against realised
revenue that does not exist. Use the surrogates:

1. **Comparable-store correlation** — score the existing estate, correlate
   predicted capture against realised store revenue. This fits `SIZE_EXPONENT`,
   `DISTANCE_DECAY` and `CATCHMENT_KM` at once, and is the highest-value unbuilt
   probe in the system.
2. **Competitor openings as natural experiments** — the model predicts a
   cannibalisation pattern; realised POS movement in nearby own-stores tests it.
3. **Robustness as a series** — `devkit/siting_robustness.py` every cycle; track
   top-1 held, top-10 overlap, rank drift. Falling stability is a signal even with
   no outcome label.

Until (1) clears a stated bar, the permitted claim stays *catchment analysis and
comparable-store inference*.
