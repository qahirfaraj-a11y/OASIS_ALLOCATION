---
id: param.sigma_L
type: parameter
status: measured
domain: ordering
title: Lead time standard deviation
value: 1.93d chain p75 measured — IN the term, and small
defined_in: `oasis/logic/order_up_to.py` (`sigma_lead`, `demand_sigma_over`)
feeds: [surface.purchase-order-quantity]
---

**Value:** 1.93d chain p75 measured — IN the term, and small
**Defined in:** `oasis/logic/order_up_to.py` — `sigma_lead()` resolves it per supplier, `demand_sigma_over()` consumes it, `recommend()` reports it back as `sigma_lead`

`sigma_P^2 = P*sigma_d^2 + d^2*sigma_L^2`. The lead-time half is present and is the **smaller** term, not the larger.

Measured on the live book, 3,474 lines reaching `order_up_to`:

| | |
|---|---|
| chain sigma_L, p75 of 650 measured suppliers | **1.93 d** (was a hardcoded 2.22) |
| median sigma_L on the book | 1.47 d |
| median L | 7.00 d |
| median share of `sigma_P^2` from the lead-time term | **1.6%** |
| lines where it is the larger half | **33 of 3,474** (0.95%) |
| lines where it contributes at all | 3,460 of 3,474 |
| supplier join rate for a measured spread | 91.2% |

## What this note used to say, and why it was wrong

> 2.22d — ABSENT from the term … not referenced in `calculate_order_quantity`
> `d^2 * sigma_L^2` is the larger of the two variance terms for any line that moves, and it is **not in the formula**.

Both halves described the CLASSIC path, where the horizon was the observed order gap (~2.29d mean) and `calculate_order_quantity` genuinely never referenced a lead-time spread. That reading was correct for what it looked at and is now the wrong path to look at: `OASIS_ORDER_MODEL` resolves to `order_up_to` through `global_settings.order_model`, set in both config tiers, so the derived model is what runs.

The share is also smaller than an earlier measurement of this same branch reported (3.6%). That figure was taken before the cv Poisson floor, which raised `sigma_d` on every line under 0.16/day and so shrank the lead-time term's relative weight. Both numbers were right when taken; 1.6% is the current one.

`d^2 * sigma_L^2` still earns its place — dropping it would understate the spread on the 33 lines where it dominates, and those are the long-lead suppliers where a missed cycle costs a month — but it is a correction, not the main term.

## Status history

- 2026-09-04 — `measured` → `stale` — downstream of falsified claim.ordering.no-absurd-orders

- 2026-09-13 — `stale` → `measured` — re-derived against the derived model: the term is present, measured per supplier at a 91.2% join rate, and contributes a median 1.6% of the protection-interval variance
