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
| Flat z = 1.28 against a derived z* of 2.20 | 0.55 - 1.0 M | ordering, one line |
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
