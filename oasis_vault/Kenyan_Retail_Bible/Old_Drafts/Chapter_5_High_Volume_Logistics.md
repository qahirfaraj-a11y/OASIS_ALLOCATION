# Chapter 5: High-Volume Logistics and Storage

*Backroom dynamics, warehouse-to-shelf workflows, and the constraints of high-density storage.*

## 📦 Backroom Dynamics
The space between the loading dock and the shelf is the "Black Box" of retail. In high-volume stores like Rhapta, managing this flow is a 24/7 battle.

### 1. Warehouse-to-Shelf Workflows
*   **The Velocity Sort**: High-turnover items (Milk, Bread, Water) should never enter deep storage. They must flow directly from the GRN area to the display node.
*   **FIFO (First-In, First-Out)**: Essential for fresh nodes. The network tracks "Expiry Cascades" to ensure older stock is pushed to the front.

### 2. Managing the Physical Constraints
A store's shelf capacity is finite. In an urban minimart, you are limited by **Linear Feet**.
*   **The Density Formula**: High-margin slow moves (e.g., Spices) should be packed in high-density areas. Low-margin staples need "Face Width" to capture bulk demand.
*   **Shrinkage Control**: 70% of shrinkage in Kenyan retail occurs in the backroom. Tracking the transition from **GRN Qty** to **Shelf Qty** is the only way to detect internal leakage.

## 🕰️ Expiries and Damages
*   **Active Nodes**: The neural network monitors the shelf-life of each SKU.
*   **Pre-emptive Delisting**: When a node's "Last GRN Date" is old and stock is still high, the system flags it for a "Clearance Link" to prevent a total loss.

## 🚀 Throughput Optimization
Logistics success is measured by **dwell time**—how long an item sits in the backroom. The closer that time is to zero, the more "Neural" your retail engine becomes.

---
*Reference: [[Chapter_3_Setting_Up_for_Survival| Ch 3: Layout for Throughput]]*
