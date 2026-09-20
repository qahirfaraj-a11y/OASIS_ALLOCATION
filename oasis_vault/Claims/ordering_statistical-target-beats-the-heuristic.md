---
id: claim.ordering.statistical-target-beats-the-heuristic
type: claim
status: falsified
domain: ordering
title: A statistical order-up-to level orders better than the shipped heuristic
worth: 1.85x the inventory, for no measured gain in reach
ttl_days: 90
source: devkit/probe_rop_variants.py
supports: [param.z]
tested_by: [probe.rop-variants, probe.full-book-outcome-replay]
guarded_by: [trap.like-for-like, trap.denominator-sanity]
---

**Falsified, and the shipped heuristic is the reason.**

Three sizings, scored on whether the cover ordered reaches the protection
interval and how far past it goes, on the 6,313 lines that survive AMIT and
MANDE:

| sizing | service | cover carried | median cover |
|---|---|---|---|
| heuristic (shipped) | 100% | **1.45x** | 14.0 d |
| newsvendor ROP | 100% | 1.45x | 14.0 d |
| statistical order-up-to | 100% | 2.69x | 15.8 d |

The derived order-up-to level carries **1.85x the inventory for the same
reach**. The DDoS coverage target is tighter than the textbook level and
gets there anyway.

## The comparison had to be made honest first

Run across the whole book the answer inverted: the heuristic scored 41%
service and order-up-to scored 100%. That measured the BLACKLIST, not the
sizing — 9,098 lines came back at zero cover because AMIT and MANDE blocked
them, while the pure model has no governance to stop it. Restricting every
arm to the lines that survive the gates is what turned it into the question
that was asked.

**Caveat, and it matters.** At an empty shelf every sizing reaches P, so
100% service is a weak discriminator. The extra cover the statistical model
carries is safety stock, and safety stock only pays under demand variance
this test does not simulate. The honest claim is that order-up-to is not
better HERE, not that it is worse everywhere.

## Re-tested 2026-09-19 against the model that now SHIPS

Order-up-to has since become the configured default
(`global_settings.order_model`), and the probe had gone stale in a way that
would have hidden it: its two "heuristic" arms ran through the bridge, which
reads that setting, so re-run unchanged they would have been order-up-to too.
The probe now names every arm's model explicitly and runs all three through
the same bridge. AMIT and MANDE are in report mode, so nothing is blocked and
the whole book is compared (15,411 lines, against 6,313 survivors on 09-04).

| sizing | service | cover carried | median cover |
|---|---|---|---|
| classic heuristic | 83.9% | **1.41x** | 12.4 d |
| classic newsvendor | 83.9% | 1.41x | 12.4 d |
| order-up-to (shipped) | 87.4% | **2.19x** | 20.1 d |

**Still falsified.** The shipped model reaches the protection interval on
3.5 points more lines and carries 55% more cover to do it — the gap has
narrowed since 09-04 (1.85x → 1.55x) but not closed. Newsvendor is identical
to the heuristic: at an empty shelf the ROP is a trigger, not a size.

The 09-04 caveat stands and now matters more, because this is the default:
the extra cover is safety stock, which pays only under demand and lead-time
variance this test does not simulate. The outcome replay on the bread shelf
(devkit/bread_backtest.py) is the kind of test that can settle it — there
the shipped engine beat the bakeries' own drops on both fill and expiry.
Until a full-book outcome replay exists, **"order-up-to beats the heuristic"
is not a claim the website can make.** Blast radius: `param.z` and
`surface.purchase-order-quantity` (money).

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.rop-variants → contradicts
- 2026-09-19 — `falsified` → `falsified` — probe.rop-variants (re-tested on the shipped model) → contradicts
