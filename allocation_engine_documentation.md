# OASIS Allocation Engine (v2.9) - Technical Logic Breakdown

## 1. High-Level Architecture
The engine uses a **Multi-Pass Waterfall** approach to allocate inventory. It prioritizes "Width" (assortment presence) before "Depth" (volume), ensuring stores look full even with limited budgets. It integrates financial, operational, and risk constraints at every step.

---

## 2. Core Allocation Flow

### Phase 1: Initialization & Profiling
Before allocating a single item, the engine profiles the store based on the total budget:
- **Dynamic Tiering**: Maps budget to 8 store profiles (Micro to Ultra).
- **Constraint Setting**:
  - `depth_days`: Target days of cover (e.g., 7 days for Micro, 14 days for Standard).
  - `price_ceiling`: Max allowed unit price (e.g., Micro stores cap at 750 KES).
  - `max_packs`: Max pack size allowed (prevents bulk items in small kiosks).
  - `min_display_qty`: Minimum units needed for shelf presence (MDQ).

### Phase 2: Pass 1 - Global Width (The "Survival" Pass)
**Goal**: Ensure every essential SKU is represented on the shelf with at least 1 display unit/pack.
**Logic**:
1. **Filtering**:
   - **Internal Production**: Exclude "BAKERY FOODPLUS" (internal transfer, not purchase).
   - **Price Ceiling**: Skip items > `price_ceiling` (except Staples).
   - **Dead Stock**: Skip C-Class items if disabled for this tier.
   - **Scaled Demand**: For small stores, skip non-staples with negligible demand.
2. **Quantifying**:
   - `raw_mdq` = Minimum Display Qty (e.g., 3 units).
   - **Pack Constraint**: Round `raw_mdq` UP to nearest `pack_size`.
   - **Safety Cap**: Check if `units > max_packs`. If yes, cap to `max_packs`.
3. **Budget Check**:
   - Calculate cost using **Actual Cost Priority** (GRN → Margin% → Estimate).
   - If `pass1_cost + item_cost > total_budget`, hard stop.
4. **Outcome**: Every valid item gets ~1 pack. Shelf width is secured.

### Phase 3: Pass 2 - Strategic Depth (The "Growth" Pass)
**Goal**: Allocate remaining budget to build volume for high-velocity items.
**Logic**:
1. **Wallet Partitioning**:
   - Divide remaining budget into Department Wallets (e.g., 25% to Staples, 10% to Beverages).
   - **Buffer**: Wallets get a "soft cap" (buffer pct) to allow flexibility.
2. **Prioritization**:
   - **Consignment First**: Items flagged `is_consignment` bypass cash budget checks (Free Capital).
   - **Fast Five Anchors**: (Cooking Oil, Flour, Sugar) get priority access to depth.
3. **Calculation**:
   - `target_days` = Store Tier Depth (e.g., 14 days).
   - **Smart Depth (Risk Buffer)**:
     - Supplier Reliability < 70%? **+25% Depth**.
     - Demand Volatility > 0.8? **+15% Depth**.
   - **New Product Logic**:
     - No sales history? Use **Lookalike Demand** or **Baseline** (0.3/day Fresh, 0.5/day Dry).
     - Caps: Max 7 days (Fresh), 14 days (Dry).
   - **Expiry Enforcement**:
     - `effective_days = min(target, shelf_life - 2 days)`.
     - Prevents 5-day yogurt getting 14-day stock.
4. **Execution**:
   - Iterate items by velocity (high sales first).
   - Fill up to `target_days`.
   - Deduct from Department Wallet.
   - Stop if Wallet Empty.

### Phase 4: Pass 2B - Budget Redistribution (The "Optimizer")
**Goal**: Ensure 100% budget utilization.
**Problem**: Some departments (e.g., Stationery) might have unused budget, while Staples are starved.
**Logic**:
1. Calculate `true_unused = total_budget - actual_spent`.
2. If `unused > 10%`:
   - Identify **Priority Items** (Staples/A-Class) that were capped by wallet limits.
   - Create a "Global Pool" from unused funds.
   - Re-allocate to these winners until pool is empty.

---

## 3. Financial Intelligence Logic

### ROI & Cost Calculation (v2.9)
To prevent budget overruns (the "Milk Problem"), the engine calculates cost with strict priority:
1. **GRN Database**: Actual historical purchase price (Most Accurate).
2. **Margin Derivation**: `Price * (1 - Margin_Pct)` (Accurate).
3. **Fallback**: `Price * 0.75` (Estimate).
*Result:* Allocations align perfectly with financial reporting.

### Funding Source Split
- **Cash Budget**: Strictly capped. Used for most items.
- **Consignment**: Treated as "Free Capital". Tracked separately (infinite ROI). 
*Result:* Engine maximizes consignment orders to preserve cash for other goods.

---

## 4. Constraint Matrix Summary

| Constraint | Logic | Purpose |
| :--- | :--- | :--- |
| **Price Ceiling** | `Price > X` (Tier dependent) | Keep expensive items out of small kiosks. |
| **Max Packs** | `Units > Max * PackSize` | Prevent bulk overflow (physically fitting on shelf). |
| **Expiry Cap** | `Days > ShelfLife - 2` | Prevent spoilage/waste. |
| **Wallet Cap** | `DeptSpend > DeptBudget` | Prevent one category eating all cash. |
| **Global Cap** | `TotalSpend > TotalBudget` | Hard financial limit. |
| **Risk Buffer** | `RelScore < 70 → +25%` | Prevent stockouts from bad suppliers. |
