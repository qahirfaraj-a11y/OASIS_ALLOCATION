# O.A.S.I.S. — working agreement

Retail intelligence for FMCG: smart ordering (replenishment), transfers, initial-load
allocation, and site selection. This file is the standing contract for any session
working on the methodology. Read it before changing a weight.

## The one rule

**Nothing drives money until it has beaten a baseline on data it has not seen.**

`OASIS_GNN_ORDERING_WEIGHT = 0` is this rule, already enforced, for one model. The
methodology graph generalises it to every parameter. A claim's status decides
whether the parameter it supports may move a purchase order:

| Status | Meaning | Drives money |
|---|---|---|
| `asserted` | stated, no probe attached | no |
| `measured` | a probe returned a number, once | no |
| `validated` | beat a stated baseline on held-out data | yes, gated |
| `trusted` | validated on 3 consecutive cycles | yes |
| `stale` | a dependency moved, or evidence outlived its TTL | **no** — revert to last good |
| `falsified` | a probe returned a verdict against it | **no** — everything downstream goes stale |

## The second rule: provenance

**Synthetic data can exercise the machinery. It can never validate it.**

One store on this book is real — Rhapta. The estate around it is extrapolated
from that one store *on purpose*, so the methodology can be tested before real
POS exists. That is good practice. It is also exactly why the distinction has to
be mechanical rather than remembered: synthetic data is most dangerous when it
is working well.

Every verdict carries a `provenance`, and the harness enforces it — a verdict
computed on anything but `observed` data cannot promote a claim past `measured`,
whatever else it clears. `/gate` reports `blocked_on_data` when that is the only
thing standing in the way, which is an honest and useful place to be.

| Provenance | Meaning | Can validate |
|---|---|---|
| `observed` | measured from the real book | yes |
| `extrapolated` | derived from observed data by a model | no |
| `synthetic` | generated to exercise the machinery | no |

Sources are nodes: `oasis_vault/Sources/`. Ordering rests on observed data (the
GRN cache, the declared supplier calendar). Siting does not, and that is the
whole of why its constants sit unfitted.

## Before you touch anything

```bash
python devkit/methodology/cli.py build --snapshot   # integrity + gate, and snapshot
python devkit/methodology/cli.py frontier           # what to interrogate next
python devkit/methodology/cli.py blast <id>         # what falls if this is wrong
python devkit/methodology/cli.py motifs             # structural anti-patterns
python devkit/methodology/cli.py drift              # is the shape still moving?
```

`build` currently reports GATE issues on ten parameters. That is not a bug list —
it is the true state of the system, stated out loud for the first time.

## Vocabulary — use these words exactly

- **`P = R + L`** — the protection interval, the only horizon in the engine with a
  derivation. Safety grows with its **square root** and enters **additively**:
  `S = d(R+L) + z·sqrt((R+L)·σ_d² + d²·σ_L²)`
- **`R`** — a **commitment, not a coefficient**. Halving working capital means
  ordering twice as often: an operations decision about buyer workload. Any
  proposal touching `R` is `REQUIRES_HUMAN_COMMITMENT` and goes to a human.
- **residual cover** — the non-circular scorer. Cover carried against the gap the
  delivery actually had to span, taken *afterwards*. The book scores 1.9x.
- **initial load ≠ replenishment** — greenfield allocation is width-first, then
  depth, then consolidate. Fresh bypasses depth at `Cycle + 0.5 days`. Never judge
  it with replenishment metrics.
- **catchment analysis and comparable-store inference** — the permitted claim for
  siting output. Never "AI predicts store success"; neither ML artefact has seen
  an outcome.

## The seven traps — run them, do not remember them

Each one was paid for once already. `devkit/methodology/traps.py`.

| | Trap | What it cost |
|---|---|---|
| T1 | silent join failure | 0 of 486 suppliers matched, on letter case |
| T2 | stale duplicate shadowing | `"…(3).json"` sorted first; lead times 3–7x |
| T3 | denominator sanity | a 43,405% overrun was 434x of KES 103 |
| T4 | like-for-like | "40% deeper" was two different denominators |
| T5 | circularity | the ordering habit contaminating its own measure — twice |
| T6 | hierarchy inversion | a clean-measuring fix that inverted the allocator |
| T7 | category error | an initial-load allocator judged as replenishment |

Import them into probes as guards:

```python
from devkit.methodology.traps import join_match_rate, ratio, non_circular
join_match_rate(derived, consumers, "supplier lookup").raise_if_violated()
```

## Naming claims

**Name a claim for the state you want to be true.** The graph propagates
alarm from anything `falsified`, so a claim phrased as a defect inverts the
meaning of its own verdict: falsifying "LATA is missing from the safety
buffer" is good news, and the graph raised an alarm for it anyway.

Write `lata-reaches-safety-buffer`, not `lata-not-in-safety-buffer`. Write
`review-schedule-is-clean`, not `schedule-is-full-of-artefacts`. When a
negatively-named claim is resolved, mark it `mitigated` and move the live
finding to a positively-named one.

## Standing invariants

1. **Never load a file by whatever `os.listdir` returned first.** Use
   `traps.newest_wins(paths)` or an explicit sort. Filesystem order is not a policy.
2. **Never report a ratio without its denominator** when it falls outside [0.1, 10].
3. **Never compare two ratios over different bases.** Assert the denominators match.
4. **Never score a measure with an input downstream of the behaviour it measures.**
5. **Never change a weight vector without diffing the resulting rank order** against
   the hierarchy the operating docs encode. A fix that measures clean can still
   invert the design.
6. **Every metric declares its regime** — `initial-load`, `replenishment`,
   `transfer`, or `siting`.
7. **Every decision that costs money is written to the decision ledger** with the
   inputs it saw and the parameters in force (`oasis/logic/decision_ledger.py`).
   Attribution after the fact is impossible without it.

## Where things live

```
devkit/methodology/     the graph toolkit — vault, graph, traps, harness, cli, seed
oasis_vault/Claims/     falsifiable statements, one file each
oasis_vault/Parameters/ the numbers the engine actually reads
oasis_vault/Surfaces/   where decisions cost money
oasis_vault/Probes/     executables that return verdicts
oasis_vault/Traps/      the seven, with their detectors
oasis_vault/Evidence/   append-only dated runs — never edit by hand
oasis_vault/_graph/     derived; rebuilt by `cli.py build`, never hand-edited
oasis/logic/decision_ledger.py   every PO, transfer and site recommendation
```

`oasis_vault/Decisions/` is the existing **narrative** record and keeps that role.
DECISION nodes live in `Surfaces/` so the two never collide.

## Conventions

- **Stdlib only** in `devkit/methodology/`. A loop that fails to import is a loop
  that silently stops running. No PyYAML, no NetworkX, no pandas.
- Evidence is append-only and dated to when the measurement was taken, never to today.
- A claim with no probe is an opinion. Log it as one.
- A cycle that produces no falsifiable objection is a valid outcome. Record it.
