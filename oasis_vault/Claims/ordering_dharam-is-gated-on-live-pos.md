---
id: claim.ordering.dharam-is-gated-on-live-pos
type: claim
status: asserted
domain: ordering
title: DHARAM corrects nothing because it has no live POS feed, which is correct
worth: distinguishes a dependency from a defect
ttl_days: 120
source: devkit/governance_sweep.py
supports: []
tested_by: [probe.governance-sweep]
guarded_by: [trap.category-error]
---

DHARAM loads **0 demand patches** and is configured active. The sweep
reported that as an inert engine, which was the wrong reading.

DHARAM patches demand from a live POS feed. Without one there is nothing to
correct, and emitting zero patches is the correct behaviour rather than a
failure — the same distinction as a supplier with no measured lead time
keeping the default instead of being assumed reliable.

It is recorded as a **dependency**, not a defect. What would make it a defect
is a live feed present and still zero patches, which is what this claim will
catch when the feed arrives.
