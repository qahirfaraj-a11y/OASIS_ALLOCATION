---
id: source.fulfilment-detail
type: source
status: trusted
domain: data
title: Supplier fulfilment detail
provenance: observed
location: `All_Suppliers_Fulfillment_Detail.xlsx`
---

**Provenance: observed** · `All_Suppliers_Fulfillment_Detail.xlsx`

107,165 line-level receipts across 18,040 items and 586 vendors, 2025-01-01 to 2025-12-09. PO date, GRN date, fulfilment days, quantity. **Observed** — the client's own book, and the strongest data on this install.\n\nTwo defects, both handled in `devkit/stock_ledger.py` rather than discovered downstream:\n\n* **16 subtotal rows** carrying 2,875,674 units — *exactly half* the naive total. Any sum of this file's quantities that kept them is 2x out.\n* **2025-04, 05 and 06 are absent entirely.** A SKU received in March and again in July shows a ~120-day gap that never happened. 9,660 inter-receipt gaps are discarded for spanning it.\n\nThe hole is also a gift: it separates the year into two blocks that never touch, which is the train/test split `probe.residual-cover` uses.
