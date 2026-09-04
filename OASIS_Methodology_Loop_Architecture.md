# O.A.S.I.S. — Methodology Interrogation Loops: Architecture

> **What this is.** A design for turning the interrogation of OASIS's smart-ordering
> and site-selection methodology from *episodic prose* into a *standing, self-correcting
> machine* that runs inside Claude Code.
>
> **Status:** design for review. No code written yet.
> Generated 2026-09-03 against the repo at `.gemini/antigravity/scratch` @ `2d37b2d1`.

---

## 1. The honest starting position

OASIS already interrogates itself better than most systems ever do. The material
is all here:

| Asset | What it is | Where |
|---|---|---|
| Methodology reviews | `OASIS_GNN_Methodology_Review.md`, `OASIS_Intelligence_Ordering_Pipeline_Analysis.md`, `OASIS_Risk_Scoring_Methodology_Redesign.md` | repo root |
| The derived ordering formula | `S = d(R+L) + z·sqrt((R+L)·σ_d² + d²·σ_L²)`, with three measured defects ranked by worth | `oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md` |
| Sensitivity probes | `devkit/siting_robustness.py`, `devkit/measure_order_sensitivity.py`, `devkit/compare_transfer_methodologies.py` | `devkit/` |
| Sweeps | `devkit/whitespace.py`, `devkit/pov_sweep.py`, `devkit/matrix_sweep.py`, `devkit/grid_simulation.py` | `devkit/` |
| A working feedback harness | `backtest_allocation.py` — allocate → simulate → measure fill rate → repeat | repo root |
| A live-vs-model comparator | `devkit/shadow_monitor.py`, `shadow_logs/shadow_comparison_*.csv` | `devkit/`, `shadow_logs/` |
| A working governance gate | `OASIS_GNN_ORDERING_WEIGHT` defaults to 0 — the unvalidated GNN moves no purchase orders | `oasis/logic/gnn_service.py:104` |
| A knowledge graph substrate | Obsidian vault with `Decisions/`, `Nodes/`, `GNN_Insights/`, wikilinks already in use | `oasis_vault/` |

**So the gap is not analysis capability. The gap is persistence and re-execution.**

Three specific failure modes follow from that:

1. **Findings decay into prose.** "R is the observed order gap, worth 2.14x on
   working capital" is one of the most valuable sentences in the repo. It lives in
   a markdown paragraph. Nothing re-checks it, nothing knows what depends on it,
   and nothing notices when a code change invalidates it.

2. **Probes run once and their verdicts evaporate.** `siting_robustness.py`
   produces exactly the right output — "what moves the answer, ranked, with
   numbers." It wrote `siting_robustness.json` once. There is no series, so
   there is no way to see a robustness result *degrade*.

3. **The same traps recur, and are only caught by memory.** The stale-duplicate
   loader bug, the upper/lower-case join that matched 0 of 486, the 43,405%
   overrun that was 434x of KES 103, the "fix" that measured clean and inverted
   the allocator's own hierarchy. Each was found by a human noticing. Each is
   fully mechanizable as a check.

---

## 2. What the loops must produce

Not "more analysis." Three concrete outputs, on a schedule:

- **A claim ledger whose entries have a status that can go down as well as up.**
- **A dependency graph that marks everything downstream of a fallen claim stale**, so
  a broken assumption cannot quietly keep driving purchase orders.
- **A promotion gate**: nothing moves money until it has beaten a baseline on data
  it has not seen.

---

## 3. The graph — and what kind of graph it is

**Important distinction up front.** OASIS already has a GNN (`models/store_gnn.py`),
and the review found it to be a static attribute prior trained on synthetic labels
with its dynamic feature block frozen at zero. *The graph engineering proposed here
is not more of that.* It is a **provenance and dependency DAG over the methodology
itself** — plain, inspectable, no learned weights. Deterministic edges, because the
whole point is auditability.

### 3.1 Node types

```
CLAIM        A falsifiable statement about the methodology or the world.
             "sigma_L is absent from the safety term and materially matters."
             "The 60% staple/general split is encoded in department_scaling_ratios.csv."
             "GNN risk is insensitive to live stockouts."

PARAMETER    A number the engine actually reads.
             R, L, z, base_safety, SIZE_EXPONENT (=1.0), DISTANCE_DECAY (=2.0),
             CATCHMENT_KM (=10.0), gnn_risk_blend_ratio, OASIS_GNN_ORDERING_WEIGHT,
             CANNIBALISATION_KM (=3.0), the department scaling ratios.

DECISION     An output that costs or earns money.
             A purchase-order quantity. A transfer. A site recommendation.
             A store-format call. An initial-load allocation.

PROBE        An executable that returns a verdict on one or more CLAIMs.
             devkit/siting_robustness.py, devkit/measure_order_sensitivity.py,
             backtest_allocation.py --mode residual-cover, a new probe.

EVIDENCE     One dated run of a PROBE: inputs, config hash, numeric result, verdict.
             Immutable. Append-only. This is the series that makes drift visible.

TRAP         A known failure mode with a mechanical detector.
             "Silent join failure", "circular measure", "denominator sanity",
             "stale duplicate shadowing", "hierarchy inversion".
```

### 3.2 Edge types

```
CLAIM      --supports-->      PARAMETER      why this number is what it is
PARAMETER  --feeds-->         DECISION       where the number reaches money
PROBE      --tests-->         CLAIM          how the claim is re-checked
EVIDENCE   --instantiates-->  PROBE          one dated run
CLAIM      --depends_on-->    CLAIM          a claim resting on another claim
TRAP       --guards-->        PROBE|CLAIM    the check that must pass first
CLAIM      --contradicts-->   CLAIM          held tension, not silently resolved
DECISION   --realised_as-->   OUTCOME        the link Loop B needs and siting lacks
```

### 3.3 Claim status — a lattice, not a flag

```
    ASSERTED  ──►  MEASURED  ──►  VALIDATED  ──►  TRUSTED
       │              │              │              │
       └──────────────┴──────────────┴──────────────┘
                          ▼
                       STALE  ──►  FALSIFIED
```

| Status | Means | Allowed to drive money? |
|---|---|---|
| `ASSERTED` | Stated, no probe attached | **No** |
| `MEASURED` | A probe returned a number, once | **No** |
| `VALIDATED` | Beat a stated baseline on data it had not seen | Yes, gated |
| `TRUSTED` | Validated, and re-validated on N consecutive cycles | Yes |
| `STALE` | A dependency changed, or the last evidence is older than the claim's TTL | **No** — reverts to last-good parameter |
| `FALSIFIED` | A probe returned a verdict against it | **No** — and everything downstream goes `STALE` |

The GNN risk claim sits at `FALSIFIED` for the ordering path today and `MEASURED`
for the dashboard path. `OASIS_GNN_ORDERING_WEIGHT = 0` is already the enforcement
of exactly this rule — the architecture just generalises what that one line does.

### 3.4 Storage

Two representations of one graph, and the choice matters less than the discipline:

- **Canonical, human-editable:** one markdown file per node in `oasis_vault/`,
  YAML frontmatter for status/type/edges, body for the argument. This continues
  the vault convention already in use, keeps every claim readable and diffable in
  git, and means a human can amend a claim without a tool.
- **Derived, queryable:** a NetworkX graph built from the vault on demand,
  serialised to `oasis_vault/_graph/methodology.json`. Rebuilt by a probe, never
  hand-edited. Reachability queries ("what does this parameter touch?"), staleness
  propagation, and cycle detection all run here.

`oasis/logic/graph_export.py` and `generate_obsidian_network.py` already do the
vault↔graph round trip for SKUs and stores. This is the same machinery pointed
at the methodology instead of the inventory.

---

## 4. Loop A — Adversarial critique (runs from day one)

**Closes on:** surviving objections. No outcome data required.

```
   ┌─────────────────────────────────────────────────────┐
   │                                                     │
   │   SELECT ──► ATTACK ──► TRIAGE ──► PROBE ──► RULE   │
   │      ▲                                        │     │
   │      └────────────────────────────────────────┘     │
   └─────────────────────────────────────────────────────┘
```

**SELECT.** Pick the cycle's target by expected value, not by rotation:
`(money touched) × (staleness) × (unexamined-ness)`. A parameter feeding live PO
quantities whose supporting claim is `ASSERTED` and 90 days old outranks a
well-probed sweep constant.

**ATTACK.** Three subagents, run in parallel, each with a distinct brief and each
required to produce *falsifiable* objections — an objection that cannot be checked
by a probe is discarded:

| Agent | Brief |
|---|---|
| `methodology-challenger` | Attack the derivation. Is the horizon right? Is the term additive where it should be? Is the estimator biased? Does the model answer the question actually asked? |
| `data-adversary` | Attack the inputs. Joins, casing, duplicates, denominators, units, coverage, survivorship, what the missing rows have in common. Runs the TRAP register mechanically. |
| `ops-realist` | Attack the consequence. What does this ask a buyer to do? Does halving working capital mean doubling order frequency, and has anyone agreed to that? Does the "fix" invert a hierarchy the operating docs encode? |

**TRIAGE.** Objections are deduplicated against the ledger. A repeat of an
already-`FALSIFIED` objection is closed with a pointer. A new objection becomes a
`CLAIM` at `ASSERTED` with a required probe.

**PROBE.** Each surviving objection must name an executable test. Existing devkit
scripts satisfy most; new ones get written. **An objection with no probe is not a
finding — it is an opinion, and it is logged as one.**

**RULE.** Verdicts are written back as `EVIDENCE`, statuses move, staleness
propagates through the DAG, and the cycle's diff is committed.

### The TRAP register — Loop A's mechanical floor

Every trap below was actually paid for in this repo. Each becomes an automatic
check that runs before any cycle can report a finding:

| Trap | Detector |
|---|---|
| **Silent join failure** | Any join reporting < 60% match rate raises. The derivation once matched **0 of 486** suppliers on lower-vs-upper-case keys. |
| **Stale duplicate shadowing** | Any `os.listdir`/glob load without explicit sort + newest-wins raises. `"…_2025 (3).json"` sorts before `"…_2025.json"` because space precedes dot; lead times inflated 3–7x. |
| **Denominator sanity** | Any ratio > 10x or < 0.1x must print numerator and denominator. A 43,405% budget overrun was 434x of KES 103. |
| **Like-for-like** | Any two ratios compared must assert identical denominators. "40% deeper than the human book" was two different denominators. |
| **Circularity** | A measure whose input is downstream of the behaviour it measures is rejected. `--mode residual-cover` is the pattern: take the measure *afterwards*, against the gap that actually had to be spanned. |
| **Hierarchy inversion** | Any change to a weight vector must diff the resulting *rank order* against the operating docs' stated hierarchy, not just the loss. The rebuilt department weights measured clean and halved the staple share — a working guard read as a symptom. |
| **Category error** | A metric must declare which regime it belongs to. Initial-load allocation is not replenishment: milk's 11.8% "under-used wallet" was a JIT cap doing its job. |

### Cadence

Weekly. One target per cycle. A cycle that produces no falsifiable objection is a
valid outcome and is recorded as such — that is how a claim earns its way toward
`TRUSTED`.

---

## 5. Loop B — Outcome-fed correction (staged in as outcomes accumulate)

**Closes on:** realised outcomes attributed back to specific parameters.

```
   DECIDE ──► LOG ──► WAIT ──► OBSERVE ──► ATTRIBUTE ──► PROPOSE ──► GATE
      ▲                                                                │
      └────────────────────────────────────────────────────────────────┘
```

**The prerequisite that does not exist yet: a decision ledger.** Every PO quantity,
transfer, and site recommendation must be written at decision time with the full
input vector, the parameter values in force, and the config hash. Without it,
attribution later is guesswork. This is the single highest-value thing to build,
and it is cheap.

### 5.1 Ordering — real substrate, ready now

The outcome data genuinely exists:

| Signal | Source |
|---|---|
| Realised lead time & variance | `oasis/data/grn_intelligence_cache.json`, `sku_grn_frequency.json` |
| Actual order cadence (`R`) | `supplier_weekly_schedule.json` (940 suppliers) vs observed GRN gaps |
| Realised demand | POS DBs (`rhapta_pos.db`, `rhapta_multi_store.db`, `mock_pos_erp.db`) |
| Model-vs-live divergence | `shadow_logs/shadow_comparison_*.csv` |
| Closed-loop fill rate | `backtest_allocation.py` |

**Primary scorer: residual cover.** For each delivery, the cover carried against
the gap it actually had to span — taken afterwards, so the ordering habit cannot
contaminate it. The book currently scores **1.9x**. That single number is the
loop's objective function, and it is already non-circular by construction.

**Attribution.** Decompose the residual-cover error across the formula's own terms:

```
error  ≈  ∂/∂R  · ΔR      (cadence assumption vs observed gap)
       +  ∂/∂L  · ΔL      (lead-time point estimate)
       +  ∂/∂σ_L · Δσ_L   (the missing variance term — measured 2.22d spread on a 2.29d mean)
       +  ∂/∂z  · Δz      (service level — worth only 1.52x across 50%→99%)
```

This ranks *which term to fix* by measured worth on the client's book, which is
exactly how the three defects were ranked in the first place. The loop makes that
ranking continuous instead of one-off.

**The honest constraint.** `R` is a commitment, not a coefficient. The loop may
*propose* "halve working capital by ordering twice as often" — it must never
*apply* it, because that is an operations decision about buyer workload. Proposals
touching `R` are flagged `REQUIRES_HUMAN_COMMITMENT` and routed to a human.

### 5.2 Siting — no outcome labels, and the design must say so

There are no opened-store-with-realised-revenue labels. Neither ML artefact has
seen an outcome: the RandomForest trained on 10,000 rows generated from a
hand-written formula, the GCN on a weighted sum of its own input features, neither
with a validation split. Loop B therefore **cannot close on siting yet**, and
pretending otherwise is the exact anomaly the GNN gate exists to prevent.

Three surrogate closures that are honest in the meantime:

1. **Comparable-store inference.** Score the *existing* estate with the same
   pipeline and correlate predicted capture against realised store revenue. This
   is a real held-out test using stores that already exist. `score_band` /
   `rank_band` already sweep the exponents (`ALPHA_RANGE`, `BETA_RANGE`) — fit
   `SIZE_EXPONENT` and `DISTANCE_DECAY` against that correlation instead of
   leaving them at 1.0 and 2.0 by convention.
2. **Competitor openings as natural experiments.** When a rival opens, the model
   predicts a cannibalisation and trade-diversion pattern. Realised POS movement
   in nearby own-stores tests it. `devkit/COMPETITOR_OPENINGS_REQUEST.md` is
   already asking for this data.
3. **Robustness as a standing series.** Re-run `siting_robustness.py` every cycle
   and track top-1 held, top-10 overlap, and rank drift over time. A recommendation
   whose stability is *falling* is a signal even with no outcome label.

**Until (1) clears a stated bar, siting output stays "catchment analysis and
comparable-store inference" — a claim that survives a technical buyer — and never
"AI predicts store success."** That is a graph-enforced constraint, not a
marketing preference: the claim node carries the permitted phrasing.

### 5.3 The promotion gate

A parameter change moves from proposal to production only by clearing all four:

1. **Held-out.** Measured on a window the proposal did not touch.
2. **Baseline.** Beats the incumbent — including the trivial incumbent. The GNN
   must beat `inventory_risk`; if it does not, it stays a monitoring aid.
3. **Rank check.** The resulting hierarchy diffs clean against the operating docs.
4. **Shadow period.** Runs in `shadow_mode` for N cycles with divergence logged
   before touching a live PO.

Failing any gate is not a rejection of the finding — it is a status of `MEASURED`,
which is honest and useful. Only `VALIDATED` and above may change a live weight.

---

## 6. Staging — Loop A now, Loop B as decisions accumulate

| Phase | Loop A | Loop B | Exit criterion |
|---|---|---|---|
| **0 · Seed** | Extract claims from existing docs into the vault; wire the TRAP register | — | ~25 claims with status and probes named |
| **1 · Critique** | Weekly cycles, real probes, statuses moving | Decision ledger writing on every run | 8 weeks of cycles; ledger non-empty |
| **2 · Attribute** | Continues | Residual-cover scorer + term attribution on ordering | Attribution reproduces the known 2.14x / 1.52x ranking |
| **3 · Propose** | Continues | Gated proposals on ordering params; siting on surrogate closures | One parameter promoted `ASSERTED`→`VALIDATED` end-to-end |
| **4 · Sustain** | Cycles narrow to the frontier | Continuous; TTL-driven re-validation | Claims start reaching `TRUSTED` |

---

## 7. The Claude Code surface

```
scratch/
├── CLAUDE.md                          # invariants, vocabulary, the gate rules
├── .claude/
│   ├── agents/
│   │   ├── methodology-challenger.md  # attacks the derivation
│   │   ├── data-adversary.md          # attacks the inputs; runs the TRAP register
│   │   ├── ops-realist.md             # attacks the consequence
│   │   └── evidence-scribe.md         # writes verdicts back to the vault; no opinions
│   ├── commands/
│   │   ├── interrogate.md             # /interrogate <node>  — one Loop A cycle
│   │   ├── probe.md                   # /probe <claim>       — run this claim's probes
│   │   ├── attribute.md               # /attribute <window>  — one Loop B pass
│   │   ├── graph.md                   # /graph <node>        — blast radius, staleness
│   │   └── gate.md                    # /gate <proposal>     — run the four checks
│   ├── skills/
│   │   ├── methodology-graph/         # vault↔NetworkX, status lattice, propagation
│   │   ├── trap-register/             # the seven mechanical detectors
│   │   └── probe-harness/             # uniform verdict schema over devkit scripts
│   └── settings.json                  # hooks
└── oasis_vault/
    ├── Claims/                        # one .md per claim, YAML frontmatter
    ├── Probes/                        # probe definitions → devkit entrypoints
    ├── Evidence/                      # append-only dated runs
    ├── Traps/                         # the seven, each with its detector
    ├── Decisions/                     # existing — keeps its role
    └── _graph/methodology.json        # derived, never hand-edited
```

**Hooks** carry most of the automatic discipline:

- `PostToolUse` on `Edit`/`Write` → if the touched file is referenced by any
  `PARAMETER` node, mark dependent claims `STALE` and print the blast radius.
  *This is the mechanism that stops a code change silently invalidating a claim.*
- `PreToolUse` on `Bash` running a probe → assert the TRAP register's preconditions.
- `Stop` → if the session changed a weight vector, require a rank diff.

**Scheduled tasks** (not cron — the scheduled-task tools, so they survive the
session): weekly `/interrogate` on the highest-EV node; monthly `/attribute` over
the trailing window; a TTL sweep that marks expired claims `STALE`.

**Model split.** Loop A's three attackers want a strong model and genuinely
benefit from being separate contexts — an agent that wrote the derivation is a poor
critic of it. `evidence-scribe` and the TRAP detectors are mechanical and cheap.

---

## 8. Seed claims — extracted from what already exists

The ledger starts populated, not empty. A representative slice:

| Claim | Status | Probe | Worth |
|---|---|---|---|
| `R` should be the observed order gap, not a policy review period | MEASURED | residual-cover, held-out | **2.14x** working capital |
| `σ_L` belongs in the safety term and is the larger variance contributor | MEASURED | term attribution | 2.22d spread on 2.29d mean |
| `z` as five multipliers × eleven constants is over-engineered | MEASURED | z-sweep 50%→99% | only **1.52x** |
| Cadence is a distribution, not a point estimate | MEASURED | gap CV; median 1.27, 4% with real rhythm | — |
| The engine buys ~p75 service, implicitly | MEASURED | service-level backsolve | untunable today |
| The 60% staple/general split is real and encoded | MEASURED | `department_scaling_ratios.csv` = 60.7% | reverting the rebuild was correct |
| GNN risk is insensitive to live stockouts | **FALSIFIED** (for ordering) | stockout injection; risk Δ = −0.00001 | gate holds at weight 0 |
| GNN dynamic feature block trained on constant zero | **FALSIFIED** | payday toggle; Δ = 0.00000 | cols 24–29 inert |
| Site-selection chain is sound; the ML artefacts are not | MEASURED | four Nairobi sites; Westlands correctly downgraded | sell as catchment analysis |
| `SIZE_EXPONENT = 1.0` is a convention, never fitted | ASSERTED | comparable-store correlation | **unmeasured** |
| `DISTANCE_DECAY = 2.0` is a convention, never fitted | ASSERTED | `BETA_RANGE` sweep vs realised revenue | **unmeasured** |
| `CATCHMENT_KM = 10.0` is a convention | ASSERTED | robustness perturbation | **unmeasured** |
| Trade available at a point is a market property, not a chain property | MEASURED | `pov_sweep`: all six chains → identical 0.95% capture | falls out of the model being right |
| LATA toxicity never reaches the replenishment safety buffer | MEASURED (open) | grep + term trace | F3, still open |
| Live enrichment supplies no ROP; the fallback always fires | MEASURED (open) | pipeline trace | F4, still open |

Two things that table makes visible immediately:

- **The three siting conventions are the largest unmeasured exposure in the system.**
  `SIZE_EXPONENT`, `DISTANCE_DECAY` and `CATCHMENT_KM` sit at `ASSERTED`, are never
  fitted, and every site recommendation depends on all three. The comparable-store
  correlation in §5.2 tests all three at once and is buildable now.
- **F3 and F4 are open findings with no owner and no TTL.** In the graph they
  become claims that will go `STALE` on schedule and resurface, instead of
  remaining a paragraph at the end of a document.

---

## 9. What this design deliberately does not do

- **It does not add a neural model.** The graph is a deterministic DAG. OASIS's
  existing GNN is a cautionary tale about unvalidated learned structure driving
  decisions; the corrective is not a second one.
- **It does not auto-apply parameter changes.** Loop B proposes; the gate and a
  human dispose. Anything touching `R` is explicitly an operations commitment.
- **It does not treat siting and ordering as one loop.** Ordering has outcomes and
  can close properly. Siting has none and closes on surrogates. Collapsing them
  would launder the siting model's unvalidated status through the ordering loop's
  credibility.
- **It does not replace the existing docs.** `OASIS_*.md` stays the narrative
  record. The vault holds the machine-checkable skeleton, and links back.

---

## 10. Build order

1. **Decision ledger** — write every PO, transfer, and site recommendation with
   its full input vector and config hash. Cheap, and Loop B is impossible without
   it. *Do this first regardless of everything else.*
2. **Vault schema + seed** — the ~25 claims above as nodes, with statuses and
   probes named.
3. **TRAP register** — the seven detectors as a skill, wired to hooks.
4. **Probe harness** — a uniform verdict schema; wrap the existing devkit scripts
   so their output is a claim verdict rather than a one-off JSON.
5. **`/interrogate`** — the three attackers and the scribe. Loop A runs.
6. **Staleness propagation** — the `PostToolUse` hook and the blast-radius query.
7. **Residual-cover scorer + term attribution** — Loop B on ordering.
8. **Comparable-store correlation** — fits the three siting conventions and gives
   siting its first honest held-out measurement.

Steps 1–5 are the minimum viable loop. Step 6 is what makes it self-correcting
rather than merely recurring.

---

## 11. Open questions for you

1. **Cadence.** Weekly `/interrogate` — right rhythm, or does that outpace your
   ability to act on findings?
2. **Where does the ledger live?** A new SQLite alongside `oasis/data/*.db`, or a
   table in an existing one?
3. **Client data boundary.** Loop B needs realised POS and GRN outcomes. Is that
   always available in the environment where these loops run, or does the loop
   need to degrade gracefully to shadow logs and simulation?
4. **Who is the human at the gate?** Proposals touching `R`, and any promotion to
   `VALIDATED`, need a named approver.
5. **Does the vault stay Obsidian-shaped?** It works and you already use it. The
   alternative is a plain `claims/` directory with no Obsidian coupling.
