# 🧠 Global Neural Network Deep-Dive (100% Census)

A 100% audtit of all **23,511 SKU nodes** and their related connectivity within the retail network.

## 📐 Network Overview
*   **Total Product Nodes (SKUs)**: 23,511
*   **Total Structural Nodes (Departments)**: 258
*   **Total Supply Nodes (Suppliers)**: 549
*   **Data Integrity**: 4,903 SKUs (approx. 20%) are currently linked to "Unknown" suppliers.

---

## 🏗️ Structural Pillars (Top Departments)
The following departments are the heaviest "hubs" in your network based on SKU count and connectivity:

| Department | SKU Count | Connectivity (Avg Substitutes) | Financial Impact (Gross Profit) |
| :--- | :--- | :--- | :--- |
| **HOUSEHOLD ITEMS** | 1,008 | 4.99 | Low |
| **WINES** | 915 | 4.96 | Moderate |
| **BISCUITS** | 655 | 4.96 | High (KES 173K) |
| **CHOCOLATES** | 606 | 4.94 | Very High (KES 1.1M) |
| **YOGHURT** | 364 | 4.94 | Very High (KES 1.0M) |

---

## 🚩 The Fragility Index (High Risk / Zero Redundancy)
These represent "Single Points of Failure." High revenue SKUs with **zero substitutions** listed in the neural graph:

1.  **AQUAMIST 20LT EMPTY BOTTLE**: KES 6.1M Revenue | Supplier: AQUAMIST
    *   *Risk*: Pure monopoly node. A supply break here has no neural detour.
2.  **BLUE BAND 1KG SPREAD**: KES 996K Revenue | Supplier: SUPER SAVERS
    *   *Risk*: Critical staple with no mapped substitutes (likely due to unique pack size).
3.  **BROOKSIDE 500G SALTED BUTTER**: KES 1.8M Revenue | Supplier: BROOKSIDE
    *   *Risk*: Major revenue driver currently isolated from departmental substitutes.

---

## 🚛 Supplier Hub Concentration
Top 5 "Power Nodes" that dominate the upstream network traffic:

*   **BROOKSIDE DAIRY LIMITED**: KES 37.4M Revenue (165 SKUs)
*   **COCA COLA BEVERAGES**: KES 25.7M Revenue (87 SKUs)
*   **BIO FOOD PRODUCTS**: KES 14.0M Revenue (86 SKUs)
*   **KENYA BREWERIES LTD**: KES 13.7M Revenue (36 SKUs)
*   **AQUAMIST LIMITED**: KES 13.5M Revenue (40 SKUs)

---

## 📉 Actionable Global Insights
1.  **Substitutability Saturation**: Departments like **BEER** and **CANNED SODA** have hit 100% connectivity (avg 5.0 substitutes). Optimization here should shift from "finding substitutes" to "ranking by margin."
2.  **Unknown Supplier Cleanup**: KES 7.8M in revenue is tied to "Unknown" suppliers. Mapping these is the highest priority for supply chain transparency.
3.  **Profitability Correlation**: High-profit departments like **CHOCOLATES** and **YOGHURT** are extremely well-connected. Use the substitution links to steer customers toward the higher-margin nodes within these clusters.

---
*Analysis generated programmatically via 100% network traversal on 2026-03-27.*
