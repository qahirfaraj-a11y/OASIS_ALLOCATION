# Chapter 4: The GRN Checkpoint

*The critical mechanics of Goods Received Notes and the intake bottleneck.*

## 🏁 The Gatekeeper of the Engine
The GRN (Goods Received Note) is the single most important document in a retail operation. It is the moment where theoretical order data meets physical reality. If the GRN is wrong, your inventory is wrong, your payments are wrong, and your neural network is blind.

### 1. The Mechanics of the Check
In the Rhapta store model, we track three distinct quantities:
*   **PO Qty**: What we asked for (The plan).
*   **GRN Qty**: What actually arrived (The reality).
*   **FOC Qty**: Free-of-charge samples or bonus stock.

### 2. The Fill Rate Metric
The "Fill Rate" (**GRN Qty / PO Qty**) is the ultimate measure of vendor compliance. 
*   **Case Point**: If a supplier like `Gold Crown` consistently shows a **67% Fill Rate**, they are "choking" your shelf nodes.
*   **The Neural Guard**: By monitoring this in real-time within the Obsidian nodes (e.g., `rhapta_fill_rate`), the system can automatically divert capital to a 100% fulfilled substitute.

### 3. Intake Bottlenecks
Intake failures usually happen at the physical gate:
*   **Barcode Mismatches**: If the barcode on the box doesn't match the SKU node, the items are "lost" at the point of entry.
*   **Cost Price Inflation**: If the price on the invoice is higher than the `Cost Price` in the database, your margin is leaking before the item even hits the shelf.

## 📊 Analytics at the Gate
| Metric | Ideal State | Risk indicator |
| :--- | :--- | :--- |
| **Fill Rate** | 95% - 100% | < 80% (Lost Sales) |
| **Lead Time** | 24 - 48 Hours | > 5 Days (Stockout Risk) |
| **Inventory Accuracy** | 100% | Discrepancies (Shrinkage) |

---
*Reference: [[rhapta_neural_analysis| Rhapta Fulfillment Audit]]*
