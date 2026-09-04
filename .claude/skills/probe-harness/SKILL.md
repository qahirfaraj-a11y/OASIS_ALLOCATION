---
name: probe-harness
description: Write and run probes that return verdicts on methodology claims, and record them as append-only evidence that moves claim status. Use when building a new probe, wrapping an existing devkit script, or recording a measurement.
---

# The probe harness

devkit already holds the right probes. Each wrote a one-off JSON that nobody
re-read. A probe that produces a **series** makes degradation visible; a probe that
produces a file does not.

## The contract

A probe emits **one JSON object per line on stdout** for each claim it judges.
Anything else on stdout is ignored, so log freely.

```json
{"claim": "claim.ordering.sigma-L-missing", "verdict": "supports",
 "metric": {"delta_residual_cover": 0.31}, "held_out": true,
 "baseline": "current safety buffer, no sigma_L term", "beat_baseline": true,
 "traps": ["T3", "T5"], "notes": "one sentence, what was measured"}
```

`verdict` is `supports` | `contradicts` | `inconclusive`.

## How status moves

| Verdict | Result |
|---|---|
| `contradicts` | → `falsified`, and everything downstream goes `stale` |
| `supports`, `held_out: false` | → `measured` |
| `supports`, `held_out: true`, `beat_baseline: false` | → `measured` (honest, not a licence) |
| `supports`, `held_out: true`, `beat_baseline: true` | → `validated` |
| three consecutive such runs | → `trusted` |
| `inconclusive` | no move, and the TTL clock does **not** reset |

**`held_out` is the difference between a number and a licence to move money.** If
you are unsure, it is false.

## Running

```bash
python devkit/methodology/cli.py probe probe.siting-robustness
python devkit/methodology/cli.py probe probe.residual-cover -- --window 90d
```

## Recording by hand

```python
from devkit.methodology.harness import Verdict, record
record(Verdict(claim=..., probe=..., verdict=..., metric={...},
               held_out=True, baseline="...", beat_baseline=True, traps=["T5"]))
```

## The rules that matter

- **Append-only.** Never edit or delete anything under `oasis_vault/Evidence/`.
  A wrong verdict is corrected by a new verdict.
- **Date evidence to when the measurement was taken**, never to today. A June
  measurement is not September evidence, and the TTL must be able to say so.
- **Guard with traps** and record their codes. The gate checks for `T6` before it
  will pass a rank change.
- **A probe that judges nothing is not a probe.** The harness says so.
- Read from the decision ledger (`oasis/logic/decision_ledger.py`) when the
  question is "what did the engine decide, and what actually happened".

## Probes still to build

| Probe | Why it matters |
|---|---|
| `probe.comparable-store-correlation` | the highest-value unbuilt probe — fits `SIZE_EXPONENT`, `DISTANCE_DECAY` and `CATCHMENT_KM` at once, against realised revenue of stores that already exist |
| `probe.residual-cover` | Loop B's objective function; non-circular by construction |
| `probe.term-attribution` | must reproduce the known ranking (R 2.14x, z 1.52x) before it ranks anything new |
