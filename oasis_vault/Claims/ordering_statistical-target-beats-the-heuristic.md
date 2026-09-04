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
tested_by: [probe.rop-variants]
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

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.rop-variants → contradicts
