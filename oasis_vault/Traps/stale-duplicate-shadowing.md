---
id: trap.stale-duplicate-shadowing
type: trap
status: trusted
domain: method
title: Stale duplicate shadowing
code: T2
guards: [probe.residual-cover, probe.term-attribution]
---

The loader took whatever `os.listdir` returned first, and `"..._2025 (3).json"` sorts before `"..._2025.json"` because a space precedes a dot. A stale duplicate shadowed three weeks of derived data; lead times inflated **3-7x**.

**Detectors:** `traps.newest_wins(paths)` at load time, and `traps.scan_unsorted_globs(root)` as a static sweep for any listdir/glob consumed without an explicit sort.
