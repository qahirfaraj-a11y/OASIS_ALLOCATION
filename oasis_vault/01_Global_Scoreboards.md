---
type: Dashboard
title: "Global Active Scoreboards"
---

# 📊 Global Retail Scoreboards

This dashboard aggregates the active performance metrics from your SKUs, Departments, and Suppliers directly from the node frontmatter.

> [!NOTE] 
> These tables are generated dynamically using **Dataview**. If you update an individual SKU or Supplier's YAML frontmatter, the changes will reflect here automatically.

---

## 🟢 1. Active SKUs Scorecard (Top Revenue)

```dataview
TABLE 
  department AS "Department", 
  supplier AS "Supplier", 
  price AS "Price (KES)", 
  margin AS "Margin (%)", 
  revenue AS "Historical Revenue", 
  velocity_ads AS "ADS (Velocity)"
FROM "Nodes/SKUs"
WHERE contains(tags, "active") OR contains(tags, "loss_leader")
SORT revenue DESC
LIMIT 100
```
*(Showing top 100 active SKUs by Revenue. Change the LIMIT or remove it to see all).*

---

## 🏛️ 2. Departmental Scorecard

```dataview
TABLE
  length(rows) as "Total SKUs Mapped"
FROM "Nodes/SKUs"
WHERE department != null
GROUP BY department
SORT length(rows) DESC
```
*(Shows a count of active items linked to each department).*

---

## 🏭 3. Supplier Scorecard

```dataview
TABLE 
  length(rows) as "Total SKUs Sourced"
FROM "Nodes/SKUs"
WHERE supplier != null
GROUP BY supplier
SORT length(rows) DESC
```
*(Shows how many SKUs are supplied by each vendor).*

---
