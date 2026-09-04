---
id: claim.allocation.sixty-percent-rule-real
type: claim
status: measured
domain: allocation
title: The 60% staple/general split is real and encoded in shipped data
worth: department_scaling_ratios.csv = 60.7% essential
last_evidence: 2026-08-25
ttl_days: 180
source: oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md
supports: [param.department-scaling-ratios]
tested_by: [probe.backtest-allocation]
guarded_by: [trap.hierarchy-inversion, trap.category-error]
---

Not an accident: the documented Staples 60% / General 40% split. The 171 departments with no price are low-priority discretionary lines deliberately parked in the orphan reserve.

A turnover-share rebuild measured clean and was reverted. **The error was evaluating an initial-load allocator with replenishment concepts** — milk's wallet looked under-used at 11.8%, but fresh is JIT-capped by design, so that wallet was never meant to be drained. A working guard read as a symptom.
