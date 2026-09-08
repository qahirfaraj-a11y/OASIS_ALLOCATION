# Statement of position: where the ordering engine really is

**Store 027 Rhapta Road · 2026-09-06 · branch `methodology-loops`**

## Bottom line

The ordering engine is **not** at its optimal state, and today we found the
reason it was further from optimal than anyone thought.

Every supplier lookup in `order_up_to.py` used the vendor string as it
arrived. The GRN book and the live product records carry
`SB0009 - BROOKSIDE DAIRY LIMITED`; the order calendar, the cadence file and
the lead patterns are keyed on the name alone.

| vendor strings in the GRN book | 616 |
|---|---|
| resolving to a calendar or measured cadence, as the string appears | **0** |
| resolving after the code prefix is stripped | 432 (70.1%) |

**Zero.** 15,739 of 15,739 order lines — 100% of lines, 100% of daily demand —
missed and took `R = 7.0` instead of the cadence measured from the receipt
book. The review-cadence half of LATA did nothing in production while every
offline probe reported it working, because devkit strips the code before
looking up and the engine did not.

That fully explains the production series:

| engine state | service | GMROI | DIO |
|---|---|---|---|
| pre-LATA | 71.72% | 18.16 | 11.4 d |
| after LATA `229185ef` | 45.83% | 9.65 | 17.0 d |
| after cv fix `11eea434` | 64.26% | 22.15 | 6.5 d |

The `sigma_L` half of LATA *did* land, because the patterns file stores both
spellings — so safety stock fell to its measured level while the protection
interval stayed at 8.28 days. Less safety over the same exposure window is
exactly a service collapse. The cv fix then restored safety on the slow tail,
which is the partial recovery to 64.26%.

And the milk. BROOKSIDE 500ML DAIRY BEST at 190 units/day, with R stuck at 7,
hit the protection-interval floor: `190 x (7.0 + 1.28) = 1,573` units against
**1,572 observed**. The floor was doing its job on a wrong R and amplified the
error into eight days of cover on a product that lives one. Ajab flour and
Kabras sugar moved the other way for the same reason: which vendor names
happened to match decided which lines gained and which lost.

Fixed in `c7388b6`. With the key resolved: R = 1.0, S = 433 units, 2.28 days.

## What is confirmed working

* **Margin** — VAT-corrected, 25.00% ex-VAT chain median, natural experiment
  closes at 0.00 points between zero-rated and vatable.
* **Lead time and spread** — measured PO-to-GRN over 107,165 receipts, with a
  30-receipt floor before a measured sigma is believed.
* **Shelf life** — measured per SKU from expiry returns joined to the
  delivering GRN, after stripping three separate contaminations (late
  paperwork, damage-on-arrival, stocktake cadence), bounded by the asserted
  department figure.
* **Demand** — real. `corrected_ads_from_pos.json` reconciles to the unit
  against six monthly extracts for org 027.
* **Stock** — real. The `dept_*.xlsx` snapshot, 769 negatives and 20,760
  zeros, is a genuine ledger.
* **The clamp floor design** — a ceiling below the protection interval does
  not avoid waste, it converts it into a certain stockout. Independently
  simulated: fill 48% -> 78% on the structurally-short fresh lines for
  negligible real waste.

## Per-stratum optimality

| stratum | verdict |
|---|---|
| fast dry, unclamped | modestly suboptimal, S ~15-24% understated |
| slow dry / intermittent | clearly suboptimal, S at 40-53% of the true-law optimum |
| fresh, clamped | z is inert; correctness rests on the shelf-life number |
| fresh, unclamped | as dry, same Gaussian-tail gap |
| structurally short (770 lines) | floor design correct and validated |

A continuous Gaussian order-up-to formula is the wrong tool for near-zero-mean
discrete demand. Many of the 13,553 SKUs selling one a day or less want a
"hold at least one unit" answer that a z-score cannot produce.

## Residual gaps, sized and owned

| gap | KES/yr | owner |
|---|---|---|
| Gaussian-tail understatement on slow movers | 6 - 10 M | ordering |
| Lead-time shadow price on structurally-short lines | 2.1 - 3.9 M per 0.5-1.0 d | procurement |
| ~~Flat z = 1.28 against a derived z* of 2.20~~ **MEASURED AND REJECTED** — raising z *costs* ~68 M/yr | — | closed |
| `forced_waste_units_per_cycle` overstated ~400x | reporting only | ordering |
| `order_book.csv` built without the clamp floor | audit artefact | devkit |
| Overdispersion's functional form (i.i.d. vs persistent) | ~20% of fast-mover S | needs daily till timestamps |
| Slow-mover stockouts on generous cover | out of scope | **transfer engine** |
| Ghost demand / halo | zero today | needs till baskets with timestamps |

## What would make this configuration wrong

* Overdispersion is persistent-shock rather than i.i.d. per day. Both the
  held-out real data and the demand-family stress test lean that way, and it
  would worsen fast movers materially.
* The +8.1% demand growth found between the train and test months is a real
  ongoing trend the model has no term for and never re-fits.
* Shelf life is mismeasured for a category beyond the three fresh SKUs
  actually simulated.
* Measured L and sigma_L drift from today's values.
* Every KES figure here describes **one branch of 29**. Nothing has been
  validated on another store.

## The next change, in order

1. **Verify the supplier-key fix in the live engine**, not in a probe. The
   whole point of today is that the harness said yes while production said no.
   Re-run the 12-seed x 45-week production harness on `c7388b6` and confirm
   service recovers past the 71.72% pre-LATA baseline.
2. **Discrete critical fractile below a velocity threshold** — the 6-10 M
   item, and the largest substantive lever remaining in ordering.
3. **Wire the derived z*** — 0.55-1.0 M, one line, no risk.
4. Fix the two reporting defects so audits stop reading stale numbers.

## The habit that found it

Nothing in this list came from the engine agreeing with itself. The VAT error,
the pooled-fill profit bias, the circular cv test, the degenerate substitution
gate, the 400x waste diagnostic and today's supplier-key miss were all found
by making one measurement disagree with another and refusing to explain the
gap away. The supplier-key defect in particular was invisible to every probe
in devkit and visible immediately in a production run — which is the argument
for keeping both, and for trusting the one that touches real orders.

---

# Addendum: state after the full-pipeline sweep

**2026-09-06, commits `c7388b6` .. `ecfb16c`**

## The engine at six real stock positions

Every quantity from `ou.recommend()` with the raw vendor string, so the same
code and the same joins production uses. Stock anchored on the real snapshot.

| stock | lines ordering | order KES | suppressed | infeasible | below P | stranded above S | median cover after |
|---|---|---|---|---|---|---|---|
| 0.00x | 13,188 | 23,407,644 | 1,849 | 666 | 15,037 | 0 | 21.6 d |
| 0.25x | 8,765 | 12,377,298 | 758 | 666 | 7,083 | 3,709,548 | 25.1 d |
| 0.50x | 5,675 | 7,664,799 | 650 | 666 | 4,865 | 13,919,504 | 30.7 d |
| **1.00x (today)** | **3,774** | **4,665,115** | **591** | **666** | **3,807** | **40,067,053** | **47.7 d** |
| 1.50x | 3,269 | 3,842,782 | 564 | 666 | 3,492 | 68,084,792 | 69.1 d |
| 2.00x | 3,035 | 3,461,946 | 564 | 666 | 3,353 | 96,492,740 | 92.2 d |

Three things this establishes.

**The engine behaves correctly across the whole cycle.** The buy falls
monotonically as the shelf fills -- 23.4m at a cold start, 4.67m today (19.9%
of it), 3.46m when overbought -- and floors rather than chasing a target it has
met. `infeasible` is flat at 666 across every position, which is right: it is a
property of shelf life against R + L, not of how much is on the shelf.

**Today's book carries KES 40,067,053 above its own order-up-to level.** That
is inherited, not generated: the engine cannot order it down, only wait it out.
Median cover after ordering is 47.7 days against a policy position of 21.6.
WINES 2.93m, SPIRITS 2.97m, CHOCOLATES 1.67m, COOKING OIL 1.30m.

**Monday's order is 3,774 lines and KES 4,665,115**, concentrated in WINES
270k, COOKING OIL 247k, SPIRITS 187k, BISCUITS 155k, EGGS 141k.

## R now resolves on 89.8% of lines

`{calendar 10,184 · cadence 6,574 · cadence_overrides_calendar 744 · default 1,984}`

It was 0% three commits ago and nothing said so. The assertion in the sweep was
rewritten to test the thing that matters -- the share of LINES that end up with
a measured review period, not whether two dictionaries share spellings -- so
this specific failure cannot recur silently.

## Gap register

| gap | status | size | owner |
|---|---|---|---|
| Supplier key missed on every line | **CLOSED** `c7388b6` | was 100% of lines | ordering |
| Long-life rule unreachable | **CLOSED** `724799e` | 25 lines | ordering |
| Substring token false positives (DESLY, MUESLI) | **CLOSED** `724799e` | 27 lines | ordering |
| Fresh floor overruling the 2.0-day cap | **CLOSED** `ecfb16c` | 269 fresh lines | ordering |
| Dead stock auto-ordering | **CLOSED** `ecfb16c` | 591 lines, 969k GP/yr | ordering |
| Suppression reason discarded at q=0 | **CLOSED** `ecfb16c` | 591 lines | ordering |
| Harness reimplemented the engine | **CLOSED** `84a35b0`, `724799e` | hid the above | devkit |
| Flat z = 1.28 vs derived 2.20 | **CLOSED — measured, and the register had the sign wrong** | raising z costs ~68 M/yr | ordering |
| Structurally short against the 2.0 cap | **OPEN, not ours — RESIZED to 147** | the 666 was a harness artefact | procurement |
| Discreteness on sub-1/day lines | **OPEN, not ours** | 12.0m capital | transfer |
| Overdispersion functional form | **BLOCKED** | ~20% of fast-mover S | needs daily till timestamps |
| Ghost demand / halo | **BLOCKED** | zero today | needs till baskets with timestamps |
| One branch of 29 | **OPEN** | every KES figure | needs another store's data |

Five gaps closed today, one open in ordering and it is a single line, three
owned elsewhere, three blocked on data that does not exist.

### Later corrections, from measuring what this table asserted

**`z` is closed, and the register had the sign wrong.** It read as a thing to
fix by RAISING z toward 2.20. Swept across the tabulated service levels on the
full book, five stores, 180 days: measured cycle service tracks Phi(z) at the
0.90 setting almost exactly (89.25% against 0.900), so z is calibrated and was
never the defect. Above 1.28 service SATURATES — 89.25% to 90.07% to 90.91%
while stock goes 177M to 186M to 203M — because the shelf-life clamp, pack
rounding and dead-stock suppression bind before the safety term does. Raising z
to 2.33 costs about 68 M/yr of EP to buy 1.66pp of cycle service. Keep 1.28.

**The 666 structurally-short lines are 147.** The devkit sweeps build their
inputs with `default_patterns` and a 2-day lead-time fallback; the production
path uses the measured lead-time cache. L differs on 797 lines, and since
feasibility is `shelf_life < R + L` that alone moves the verdict. Any supplier
conversation started from the 666 needs re-deriving.

**Fill rate and cycle service are not the same number.** Every service figure
quoted above and in the runs behind it is a FILL RATE (units served / units
demanded). z targets a CYCLE SERVICE LEVEL (probability of surviving a cycle).
Fill is structurally the higher of the two, because a cycle that runs out on
its last day still served nearly all its units. Reading 93.64% fill against a
90% target and concluding the model over-serves compares a rate to a
probability — a mistake made in this project once already, and the reason
`devkit/z_calibration.py` now reports both.

## What "optimal" honestly means here

The engine is now correct on every term it computes, and every input it uses is
measured rather than asserted except three: `phi = 0.40` (shape confirmed on
real data, magnitude not), the department shelf-life ceilings, and `h = 14.38%`
which is really a service-level choice wearing a cost's clothing. The one
substantive tuning item left inside ordering is `z`.

**Superseded.** `z` was swept and is calibrated; see the corrections above. And
two of the three "measured, not asserted" inputs were not reaching the engine
at all when this was written: `avg_daily_sales` was being overwritten from a
static February file (29.1% below the POS-derived series), and `sigma_d` was
being built from a cv of MONTHLY totals, leaving the safety term at 0.334x of
what the formula asks for on 96.6% of lines. Both are fixed. That second one is
also why sweeping `phi` moved nothing: `phi` only enters `demand_cv(d)`, which
was unreachable on all but 512 lines — so "magnitude not confirmed" understated
it. It was not being used.

It is not optimal in the sense of nothing left to do. It is optimal in the sense
that what remains is either a business decision, another module's, or waiting on
data -- and each of those is named above with a number against it.
