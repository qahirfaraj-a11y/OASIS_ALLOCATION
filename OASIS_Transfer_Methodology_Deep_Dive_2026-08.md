# OASIS Transfer Methodology — Deep Dive

**Date:** 2026-08-20
**Measured on:** the 14-outlet Chandarana network in the Odoo test depot
(`connectors/odoo/`), 2,971 ranged SKUs, 26,881 store-SKU pairs, read through
`OdooAdapter` over XML-RPC.
**Instrument:** `devkit/analyse_transfer_funnel.py` (read-only; walks the same
gates the engine walks, in the same order).

---

## 0. The two jobs, stated

The transfer system exists to do two things:

1. **Plug gaps** — a store is about to run out; move stock from somewhere that
   can spare it rather than waiting for the supplier.
2. **Eliminate dead stock** — a line is frozen at a node that does not sell it;
   move it to a node that does.

Everything below is judged against those two, separately, because **the engine
currently blends them and the blend favours job 1 at the direct expense of job 2.**

---

## 1. The formulae as they stand

### Shared primitives

```
safety      = ADS x 14                          (fixed, all stores, all SKUs)
gate        = 14d cover if fresh else 30d
excess      = stock - safety   if cover > gate AND (stock - safety) > ADS x 7
            = stock            if ADS == 0 and stock > 0
            = 0                otherwise

T_sustain   = min(14, max(1, next_order_window + supplier_lead))
target      = ADS x T_sustain                   (RECIPIENT fill level)

donor score = excess / (haversine_km + 0.1)
              x 3.0  if warehouse hub
              x 2.0  if days_since_delivery > 45 AND velocity_ratio < 0.05
eligibility : excess > 0 AND stock >= safety x r,  r = 1.5 / 2.0 / 2.5 by ADS

pool        = excess x RELEASE_FRACTION(0.5) - already_booked
```

### PULL (`scan_network_opportunities`, pass 2)

```
trigger   : ADS>0 and cover < 7d, or stock <= ROP, or ADS=0 and stock<1, or MOQ-failed
shortfall : max(0, target - effective_stock, moq_qty)
weight    : risk_kes = shortfall x margin
share     : min(remaining_need, pool x weight_i / sum(weights))
```

Three rounds, then a remainder sweep. Order-independent by construction —
**verified at 0.00% divergence** across reversed and sorted store orderings on
this depot.

### PUSH (pass 3)

```
donor     : cover > 60d (cold) and excess > 0 and pool >= 1
recipient : cover < 14d (hot) and target > effective_stock
weight    : need x unit_margin
```

---

## 2. Job 1 — plugging gaps: the funnel

| gate | survivors | of pairs |
|---|---:|---:|
| store-SKU pairs | 27,121 | 100% |
| below the 7d deficit trigger | 7,359 | 27.1% |
| real shortfall vs the 14d target | 7,151 | 26.4% |
| SKU exists at another store | 6,856 | 25.3% |
| **another store has excess > 0** | **3,624** | **13.4%** |
| donor also clears the eligibility ratio | 2,931 | 10.8% |
| donor can release a whole unit | 1,978 | 7.3% |
| **PULL lines emitted** | **3,110** | 11.5% |

The engine works. The losses are concentrated in one place: **half of all genuine
deficits die because no other store is allowed to be a donor**, not because no
other store has stock.

### 2.1 The donor bar sits above the plan — structurally

Five constants govern who may give, and they only make sense relative to each
other:

```
deficit trigger    < 7d      "this store is short"
fill target        = 14d     "restore it to here"
safety floor       = 14d     "excess only counts above this"
overstock gate     > 30d     "and the donor must also be above this"
eligibility ratio  x 1.5-2.5 "stock >= safety x ratio, i.e. 21-35d cover"
```

**A store must hold roughly 30 days of cover to give anything away, while the
plan only ever buys 14 and the alarm only rings at 7.** Everything between 7d
and 30d neither asks nor gives. On the uniform network that dead band held **56%
of all selling store-SKU pairs**.

The consequence is not subtle: *a network stocked to its own plan can never
produce a donor.* Only a network stocked to more than twice its plan can. That
is why the undepleted seed produced literally zero opportunities, and it will be
true of any well-run client whose stores sit near their targets.

### 2.2 Donor protection ignores what the donor actually needs

`T_sustain` — cover until the supplier's next delivery actually lands — is
already computed and already used to size the **recipient's** fill. The
**donor's** floor is a flat `ADS x 14`, and its release cap a flat 0.5.

So a donor two days from a delivery is protected exactly as heavily as one
sixteen days out. The information to do better is in the same function.

Per-store `safety_days` exists in the store profile (10 for most outlets) and is
**never read** — the 14 is hardcoded.

### 2.3 The sub-unit asymmetry

The two passes give opposite answers to the same question:

- **PULL** calls `_round_transfer_qty`, which **ceils**. Given a pool of 0.44
  units it ships **1**, breaching `RELEASE_FRACTION` by up to a full unit per line.
- **PUSH** gates on `if pool < 1: continue` and ships **nothing**.

On the uniform network *every one* of the 218 PULL lines was a sub-unit pool
rounded up, and PUSH emitted zero. Both behaviours are defensible; holding both
at once is not.

### 2.4 Two donor-protection constants, and three private ledgers — fixed

`RELEASE_FRACTION` (scan path) and `max_donor_drain` (`decide()`) were two
declarations of one idea. **Now one**: `DONOR_RELEASE_FRACTION` lives in
`fulfillment_decider` and the service imports it, so the two cannot drift.

Underneath that sat a worse problem. Donor excess was drawn down by **three**
mechanisms that could not see each other:

| claimant | how it tracked its takings | visible to others |
|---|---|---|
| `scan_network_opportunities` | a local `booked` dict off `stock_data` | no |
| `decide_batch` | mutated `StoreSkuState` on the availability map | no |
| `ProactiveRebalancer` | mutated `current_stock` privately | no |

Each was correct alone. Together they promised the same units repeatedly. On a
fixture with one donor holding **600 units (530 spare)**:

```
three private ledgers : 1,568 units promised   -- 2.6x the stock that exists
one shared ledger     :   460 units promised   -- within its spare capacity
```

`DonorLedger` is now the single book, consulted by all three, and by
`find_donors` so an exhausted donor stops being offered and stops out-ranking
free ones. Pinned by `TestCrossPathLedger`, whose assertions were each checked
to FAIL with the ledger disabled.

**One trap this surfaced.** `NetworkAvailabilityMap` indexes every state under
several aliases — item code, product name, barcode — and `ProactiveRebalancer`
walks that index directly, so it visits the same physical stock once per alias.
The old private mutation hid this (the second visit saw depleted stock); a
ledger keyed on the loop variable gave each alias its own fresh allowance and
promised the pile twice (460 + 342 from a 600-unit store). The ledger is
therefore keyed on the **donor's own canonical code**, never on the caller's
alias, and `FulfillmentDecision` now carries `donor_itm_cd` to make that
identity explicit.

### 2.6 Two engines for one job — consolidated

`ProactiveRebalancer` did the same work as the PUSH pass, from a different
entry point, holding its own version of every rule:

| | ProactiveRebalancer | PUSH pass |
|---|---|---|
| donor | cover > 60d | dead, or past its category's tier |
| protection | `safety_stock x 2` | full for dead, release fraction otherwise |
| recipient | cover < 14d | any **active** store |
| fill to | 30 days | category tier, or the relief horizon |
| bookkeeping | mutated `current_stock` | the shared ledger |

Four constants and a private ledger, none of them derived, all disagreeing with
the pass doing the same job. An operator running Smart Ordering and then the
Transfer Intelligence tab got two different answers about the same stock.

The class is **deleted**. `_identify_proactive_transfers` now calls
`_push_opportunities`, so both entry points run one implementation that takes
its thresholds from AMIT and its horizons from LATA. Supporting extractions:
`_coverage_entry` / `_coverage_index` (one view of the network, instead of the
scan's inline build plus a hand-rolled second view) and `_donor_pool`
(previously a closure, so unreachable from anywhere but the scan).

Pinned by `TestProactiveTransferRelaxedThresholds`, which now asserts that
changing AMIT's category tier changes which donors qualify — something a
hardcoded window cannot do — that both entry points route through
`_push_opportunities`, and that the deleted engine does not reappear. The
previous version of that test asserted the literals `"60"` and `"14"` appeared
in the method's SOURCE TEXT, which is why it never noticed a whole second
engine with different numbers.

### 2.5 Dead constants — fixed

- `MIN_SAVINGS_RATIO = 0.3` was declared and referenced nowhere; the live gate
  was an unexplained inline `x 0.4`. The dead constant is deleted and the real
  one is named `MAX_TRANSFER_COST_RATIO = 0.4` — the value that always ran,
  not the aspirational one that never did.
- Transfer cost still defaults to 200 in the decider and 500 from the service.
  500 wins. Untouched.

---

## 3. Job 2 — eliminating dead stock: this is where it breaks

The depot carries **240 dead lines — 6,831 units, KES 1.48M at cost** — every one
of which is a SKU that is **active at another store**. That is the entire PUSH
case, on a plate.

**PUSH emits 26 lines.** Read back through `OdooAdapter` against the live depot,
the whole plan is **3,486 opportunities: 3,460 PULL and 26 PUSH.** Gap-plugging
does 99.3% of the work; dead-stock clearance does 0.7%.

> Figures in the funnel table above come from the offline reconstruction
> (`--source seed`), which is why they differ by a few lines from the Odoo-read
> totals — Odoo holds quantities rounded to whole units. The gap proportions are
> the same either way.

### 3.1 Half the dead stock is withheld by design

```
dead stock on hand:            6,831 units
moved by the plan:             3,398 units  (49.7%)
```

That 49.7% is not a coincidence. `excess` for a zero-ADS line is its whole stock,
and `pool = excess x RELEASE_FRACTION` caps release at **exactly half**.

**The engine holds back 50% of the dead stock it exists to eliminate.** For a SKU
selling zero units a day, a 14-day safety floor is 0 units and a 50% release cap
protects nothing — there is no service risk to hedge against. Both rules are
correct for live stock and meaningless here.

### 3.2 PUSH's two halves serve different goals

| half | shape | serves |
|---|---|---|
| donor: cover > 60d, excess > 0 | dead-stock-shaped | job 2 |
| recipient: cover < 14d, fill to 14d | **gap-shaped** | job 1 |

The stated goal is "move dead stock to nodes that are **active**". The code
requires the recipient to be **short** — under 14 days of cover — and will only
fill it to 14 days. A store that sells the line briskly and is adequately stocked
cannot receive it, even though that is precisely where the frozen capital would
turn over.

Measured: 148 of 150 movable dead lines *did* have a hot recipient, so this is
not the binding gate **today** — but it is the wrong criterion, and it binds the
moment the network is healthier.

### 3.3 PULL runs first and eats the recipients

**202 of 216 dead SKUs had their hot recipient already served by PULL** before
PUSH ran. Dead-stock clearance gets the leftovers of gap-plugging.

### 3.4 Donor ranking does not prefer dead stock

Of PULL lines for a SKU that is dead somewhere in the network:

```
sourced FROM the dead node:   408   dead capital cleared
sourced from a healthy node:  347   gap plugged, dead stock left frozen
```

**347 times the engine had a choice between clearing frozen capital and drawing
down a healthy store, and chose the healthy store.**

The only dead-stock preference in the ranking is a `x2` score bonus gated on
`days_since_delivery > 45 AND velocity_ratio < 0.05` — an *age proxy* for
deadness, not deadness itself. `ADS == 0 with stock on hand` is the direct
signal and is already computed.

---

## 4. What I would change, in order of value

1. **Exempt dead stock from donor protection.** If `ADS == 0`, release 100%, not
   50%. Recovers ~3,400 units / ~KES 740k of frozen capital in this depot alone.
2. **Prefer dead stock as a donor when plugging a gap.** Score on `ADS == 0`
   directly rather than the `days_since_delivery > 45` proxy. Both jobs get done
   in one move; up to 347 lines here.
3. **Reconcile the sub-unit rule.** Pick ceil-and-account or refuse — not one
   each.
4. **Derive the donor floor from `T_sustain`,** the way the recipient target
   already is, and read the store's own `safety_days`. Closes the 7d–30d dead
   band without weakening genuine protection.
5. **Loosen PUSH's recipient test** from "short" to "active", filling beyond 14d
   when the source is dead — that is what the stated goal asks for.
6. **One donor-protection constant and one ledger** across the scan and
   `decide()` paths. Delete `MIN_SAVINGS_RATIO`; settle the transfer cost default.

Items 1–2 are the highest value per line changed and directly serve the stated
goal. Item 4 is the largest structural change and should be measured before and
after, not reasoned about.

---

## 5. What was changed, and what it did

Implemented against the corrected specification: the horizon is whatever it
takes to reach the next delivery, transfers plug the gap and nothing more, and
dead stock is zero demand sustained for 90 days.

**Nothing is a constant any more where a real number exists.**

| was | is now | derived from |
|---|---|---|
| deficit trigger 7d | `stock < ADS x relief` | LATA supplier rhythm |
| fill target 14d | restore to relief, no further | LATA supplier rhythm |
| relief = `min(14, next+lead)` | `median_gap + lead x variance` | LATA GRN history |
| horizon cap 45d | the category's shelf life | AMIT perishability tiers |
| unknown supplier -> 7d | the network's median relief | the rhythm table itself |
| PUSH donor: cover > 60d | zero demand, silent 90d+ | the stated definition |
| PUSH recipient: short (<14d) | **active**, any velocity | the stated goal |
| PUSH sizing: fill to 14d | what it can sell before dying | AMIT tier x local ADS |
| dead-stock release: 50% | 100% — no demand, no risk | the definition |
| capital floor 500 | must exceed the transfer cost | `transfer_cost_kes` |
| PUSH overstock gate 60d | the category's own threshold | AMIT perishability tiers |
| `MIN_SAVINGS_RATIO` (dead code) | deleted; the live gate is now named `MAX_TRANSFER_COST_RATIO` | the behaviour that actually ran |
| `max_donor_drain` 0.5 + `RELEASE_FRACTION` 0.5 | one `DONOR_RELEASE_FRACTION`, imported by both | single definition |

Measured on the live depot, read through `OdooAdapter`:

| | before | after |
|---|---:|---:|
| PULL lines | 3,110 | **4,001** |
| PULL units | 29,883 | 19,546 |
| PUSH lines | 29 | **827** |
| — from dead donors | — | 2,907 units (42.6% of all dead) |
| — from overstock donors | — | 665 units |
| dead stock cleared | ~0% | **42.6%** |
| order sensitivity | 0.00% | **0.00%** |

PULL finds *more* stores genuinely short (measured against their own supplier's
rhythm rather than a flat 7 days) while moving *fewer* units, because each
transfer is now sized to the real gap instead of topped up to an arbitrary 14
days. That is the "transfers only fill gap" rule doing exactly what it says.

The 57% of dead stock left behind is not a failure: 47 lines are silent for
under 90 days and so are not yet dead, and the remainder has no store that can
absorb it without it going dead again there. Stock that nowhere can sell needs a
markdown, not a lorry — and the engine now says so instead of shuffling it.

### PUSH has TWO donor kinds — the difference is the release rule

A revision of this work briefly narrowed the PUSH donor gate to dead stock
only, which silently deleted overstock rebalancing: a store on 100 days of
cover beside one on 10 moved nothing at all. That was wrong. The thing that
actually needed fixing was the RELEASE rule, not eligibility.

| donor | qualifies when | releases | receiver filled to |
|---|---|---|---|
| **dead** | ADS 0, silent 90d+ | **all of it** — no demand means no service to protect | its category's dead threshold (the alternative is it stays dead) |
| **overstock** | still selling, cover past its category threshold | the protected fraction, through the shared ledger | its relief horizon (beyond that just relocates surplus) |

Both are eligible; both are sized by what the receiver can actually trade out.
Filling every receiver to the dead threshold — which the first attempt did —
over-ships from donors that are still trading perfectly well.

The overstock gate is now AMIT's per-category tier rather than a flat
`cold_node_days = 60`, so it is derived and lower: 50 days of cover on a
general line is surplus, not merely "cold".

## 6. Standing caveat on the data

This is measured on a **synthetic multi-store network**. The Rhapta snapshot
underneath it is real — real SKU velocities, prices, costs, departments,
suppliers — but the cross-store variation is modelled, in both the original
profile (one scalar per store) and the differentiated one used here (trait-driven
affinity from each outlet's real floor area, catchment affluence and store
category, with stock allocated on network-average velocity).

Neither is observed multi-store behaviour. **The findings above are about the
engine's structure — thresholds that contradict each other, two passes answering
one question differently, a release cap that halves its own objective — and those
hold regardless of the data.** Any *numeric recalibration* (the 30d gate, the 0.5
fraction, the 60d cold threshold) must be re-derived against genuine multi-store
data before it is trusted.
