# Chapter 7: Mapping the SKU Universe

*Understanding nodes, edges, and weightings in the 23,511-SKU network.*

## 📐 The Network Typology
The Kenyan retail universe is not just a list of items; it is a complex, multivariable graph. In the OASIS paradigm, we categorize this graph into three primary components:

### 1. Nodes (The Product Identity)
Each SKU is a node. In our census, we have **23,511 active nodes**. Each node carries its own operational telemetry:
*   **Revenue Weight**: How much gravitational pull a node has on the store's P&L. (e.g., Brookside Milk = Sales Rank 1).
*   **Margin Weight**: Whether a node is an "Anchor" (negative margin) or a "Profit Builder."

### 2. Edges (The Relationships)
An edge is a link between two nodes. 
*   **Substitution Edges**: "If I can't find A, I will buy B." This is a horizontal relationship. There are **415,509 edges** in our network, showing immense redundancy in commodity categories.
*   **Supplier Edges**: Vertical links to the "Power Nodes" (Suppliers).

### 3. Node Weighting: The "Vitality Score"
We calculate node weighting using **Sales Velocity (ADS)**. 
*   **High-Vitality Nodes**: Nodes like `BROOKSIDE 500ML` (Velocity: 727 units/day) are "Systemic Hubs." If a Hub fails (stockout), it creates a structural void that the surrounding substitutes must fill.
*   **Low-Vitality Nodes**: Long-tail items with zero substitutes are "Glass Nodes"—fragile and potentially delistable if the margin doesn't justify the shelf-shuttering effect.

## 🕸️ The Global Network Map
When viewed as a whole, the 23,000-SKU network reveals a "Core-Periphery" structure.
*   **The Core**: 10% of SKUs (the top 2,000) drive 80% of the graph's energy (revenue).
*   **The Mesh**: Interconnected departments like Fresh Milk, Bread, and Sugar create a "Stability Mesh" that keeps the store operational.

## 🏁 Summary
Mapping the universe allows us to see the store as a living organism rather than a static inventory list. We are now ready to analyze how these nodes interact during a single customer journey.

---
*Reference: [[Chapter_8_Basket_Affinity| Ch 8: Basket Affinity and The Halo Effect]]*
