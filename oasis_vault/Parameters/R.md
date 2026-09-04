---
id: param.R
type: parameter
status: measured
domain: ordering
title: Review period / order gap
value: 7d policy vs observed gap
defined_in: `oasis/logic/order_up_to.py`
feeds: [surface.purchase-order-quantity]
---

**Value:** 7d policy vs observed gap
**Defined in:** `oasis/logic/order_up_to.py`

The protection interval is `P = R + L`. `R` is currently a policy review period; the books say it should be the OBSERVED order gap. Worth **2.14x** on working capital — the single largest lever in the engine.

`R` is a commitment, not a coefficient: halving working capital requires ordering twice as often, which is an operations decision about buyer workload. Proposals touching it are `REQUIRES_HUMAN_COMMITMENT`.
