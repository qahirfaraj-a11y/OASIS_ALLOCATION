---
id: param.z
type: parameter
status: measured
domain: ordering
title: Service level multiplier
value: 5 multipliers x 11 category constants
defined_in: `oasis/logic/order_engine.py`
feeds: [surface.purchase-order-quantity]
---

**Value:** 5 multipliers x 11 category constants
**Defined in:** `oasis/logic/order_engine.py`

Worth only **1.52x** across the entire 50%->99% range. The cheapest term in the formula, and the one that had been tuned. The engine buys ~p75 service implicitly and untunably.
