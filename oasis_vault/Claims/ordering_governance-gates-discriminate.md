---
id: claim.ordering.governance-gates-discriminate
type: claim
status: falsified
domain: ordering
title: Every governance gate earns its place by discriminating
worth: eight of fifteen never fire; two engines are effectively inert
ttl_days: 90
source: devkit/governance_sweep.py
supports: [param.lata-variance-multiplier]
tested_by: [probe.governance-sweep]
guarded_by: [trap.denominator-sanity]
---

A gate that never fires is decoration. A gate that fires on everything is a
tax. Between those it earns its place, and the only way to see which is to
run the whole universe through it and look at the distribution.

Across 15,411 lines:

| gate | lines | rate |
|---|---|---|
| ROP fallback | 6,313 | 41.0% |
| AMIT blacklist | 5,684 | 36.9% |
| LATA shield | 5,420 | 35.2% |
| MANDE purge | 3,414 | 22.2% |
| critical override | 2,448 | 15.9% |
| HALO protected | 10 | 0.06% |
| key SKU boost | 10 | 0.06% |

**Never fired at all:** discontinued, stale fresh, dead stock, GNN risk
burst, schedule hold, adequate coverage, order-up-to-covered.

Three things stand out.

**AMIT blocks 9,296 SKUs** of a 19,492-line catalogue — 48% of the book is
blacklisted before any arithmetic runs. **MANDE flags 300 of ~586
suppliers**, so half the supplier base is marked for delisting. Neither is
wrong on its face, but a gate rejecting half of everything is making the
assortment decision, not refining it.

**DHARAM loads 0 demand patches.** The engine is configured active and
corrects nothing. It is not tuned badly; it is not running.

**HALO protects 12 items** in a catalogue of nineteen thousand. That may be
exactly right for an anchor list, but it cannot be doing the work its name
suggests at that scale.

## Status history

- 2026-09-04 — `asserted` → `falsified` — probe.governance-sweep → contradicts
