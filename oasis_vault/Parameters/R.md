---
id: param.R
type: parameter
status: measured
domain: ordering
title: Review period / order gap
value: declared calendar 53% · measured cadence 38% · flat default 9%
defined_in: `oasis/logic/order_up_to.py` (`review_period`, `_r_source`)
feeds: [surface.purchase-order-quantity]
---

**Value:** declared calendar 53% · measured cadence 38% · flat default 9%
**Defined in:** `oasis/logic/order_up_to.py` — `review_period()` resolves it, `_r_source()` labels which rule fired, and the label is carried on every line in `order_up_to_terms`

The protection interval is `P = R + L`. `R` is no longer a flat policy number.

Measured on the live book, 3,474 lines reaching `order_up_to` (mode `scheduled`):

| where R came from | lines | |
|---|---:|---|
| `calendar` — a declared order day | 1,849 | 53.2% |
| `cadence` — measured from receipt history | 1,120 | 32.2% |
| `cadence_overrides_calendar` | 208 | 6.0% |
| `default` — the flat 7d | **297** | **8.5%** |

So **91.5%** of lines take `R` from something the chain either committed to or was observed doing. `R` spans 1 to 30 days; the median is 7d, but that is mostly *declared* weekly rather than *assumed* weekly. Median `P = R + L` is 14d.

## What this note used to say, and why it was wrong

> `R` is currently a policy review period; the books say it should be the OBSERVED order gap.

That gap is largely closed. A declared calendar day wins because it is a commitment somebody made; a cadence built from ≥10 receipts is used where no day is declared; and where measurement strongly contradicts a declaration, `cadence_overrides_calendar` lets the evidence win (208 lines). The flat default survives only where there is neither a declaration nor enough receipts — 297 lines.

The absent-supplier question that used to sit inside this one is now explicit rather than accidental: `ordering_mode` chooses whether a supplier with no declared day is reviewed on the chain default (`scheduled`) or can be ordered any working day (`on_demand`). Worth a week of cover on roughly half the book, and it is a stated choice with a name.

## The 2.14x claim, re-derived

The old figure is not reproducible as stated because it named no range. Re-derived by recomputing `S` across a span of `R` on the same 3,474 lines, with working capital approximated as **average cycle stock + safety** (`d·P/2 + (S − d·P)`):

| R multiplier | implied stock | vs today |
|---|---:|---:|
| 0.25x | KES 3,445,705 | 0.71x |
| 0.5x (order twice as often) | KES 3,978,958 | **0.82x** |
| 1.0x (today) | KES 4,853,473 | 1.00x |
| 2.0x (order half as often) | KES 6,532,809 | **1.35x** |
| 4.0x | KES 9,794,411 | 2.02x |

A **16-fold** span of `R` moves capital **2.84x** — which is the shape the old 2.14x was pointing at. But the marginal sensitivity near today's `R` is far smaller than that headline implies: doubling the review period costs 35%, and halving it saves only 18%. Safety stock scales with `sqrt(P)` while only the cycle half scales linearly, so the lever flattens as you pull it.

It is still comfortably the largest lever in the engine. For comparison, on the same book the exact discrete quantile moved capital -4.9% and the cv Poisson floor +2.7%.

The capital model above is this note's own approximation, not something the engine computes — the engine sizes `S`, not a balance sheet. Treat the ratios as sound and the absolute shillings as indicative.

## The commitment

`R` is a commitment, not a coefficient: halving working capital requires ordering twice as often, which is an operations decision about buyer workload, not a parameter change. Proposals touching it remain `REQUIRES_HUMAN_COMMITMENT`. That the engine now *reads* a declared cadence does not make the cadence the engine's to set.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.ordering.no-absurd-orders

- 2026-09-04 — `stale` → `measured` — upstream recovered

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.ordering.receipt-history-is-complete

- 2026-09-13 — `stale` → `measured` — re-derived against the derived model: R now resolves from a declared calendar or a measured cadence on 91.5% of lines, and the working-capital sensitivity re-measured across a 16-fold span
