---
id: param.gnn-ordering-weight
type: parameter
status: measured
neutralised: true
domain: ordering
title: OASIS_GNN_ORDERING_WEIGHT
value: 0 (default, set nowhere)
defined_in: "`oasis/logic/gnn_service.py:104`"
feeds: [surface.purchase-order-quantity]
---

**Value:** 0 (default, set nowhere)
**Defined in:** `oasis/logic/gnn_service.py:104`

**The gate that works.** The unvalidated GNN contributes to ordering risk only when this is > 0. It is 0 by default and set nowhere, so the model moves no purchase orders.

This parameter is the template for the whole architecture: a claim that has not earned `validated` does not get a non-zero weight.
