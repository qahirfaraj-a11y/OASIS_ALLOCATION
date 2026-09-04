---
id: trap.silent-join-failure
type: trap
status: trusted
domain: method
title: Silent join failure
code: T1
guards: [probe.pipeline-trace, probe.residual-cover, probe.comparable-store-correlation]
---

The derivation wrote lower-case supplier names where every consumer looks up upper-case. **Zero of 486 matched.** Invisible, because the file only ever got written on an instance too thin to pass the replace guard, and nothing asserted the match rate.

**Detector:** `traps.join_match_rate(left, right, name, floor=0.60)`. Also reports what the rate would be case-normalised, which is the diagnostic that names the bug rather than the symptom.
