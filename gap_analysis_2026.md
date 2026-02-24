# OASIS Gap Analysis 2026

## 1. Simulation Logic Gaps (`retail_simulator.py`)

### A. Demand Generation is "Too Perfect"
*   **Current Logic**: Uses either Poisson (low vol) or Normal (high vol) distribution.
*   **The Gap**: Real retail demand is **Not Normal**. It has "Fat Tails" (Black Swan events where a restaurant buys 50kg of sugar).
*   **Impact**: Simulation underestimates stockouts because it rarely generates "spikes" > 3 standard deviations.
*   **Fix**: Implement a **"Bulk Buyer Event"** probability (e.g., 1% chance of 5x demand).
*   **Status**: [x] Fixed (v2026)

### B. Weekend Multiplier is Static
*   **Current Logic**: Every SKU gets `1.3x` demand Fri-Sun.
*   **The Gap**: This is wrong.
    *   *Alcohol/Meat*: Should be `2.0x`.
    *   *School Supplies*: Should be `0.5x` (Nobody buys pencils on Sunday).
*   **Impact**: We overstock stationery and understock beer on weekends.
*   **Fix**: Move `weekend_multiplier` to `Category` level logic.

### C. Lead Times are Too Stable
*   **Current Logic**: `random.uniform(0.8, 1.2)`.
*   **The Gap**: Suppliers don't just "vary slightly". They **fail completely**.
*   **Impact**: Simulation never tests "Missed Delivery" scenarios (0% fill).
*   **Fix**: Add a `supplier_failure_rate` (e.g., 5% chance of delivery = 0).

---

## 2. Allocation Logic Gaps (`order_engine.py`)

### A. The "Cliff Edge" of Scaled Demand
*   **Current Logic**: Micro stores use `demand_scale = 0.0015` (0.15% of Mega).
*   **The Gap**: Mathematical scaling kills variety.
    *   *Mega ADS* for Niche Spice = 1.0.
    *   *Micro ADS* = 0.0015.
    *   *Result*: Engine sees ~0 sales and **orders nothing**.
*   **Impact**: Small stores become "Empty Shells" with only Coke and Bread.
*   **Fix**: Implement **"Minimum Viable Presence"** logic. If an item is Core, force `ADS = 0.1` (sell 1 every 10 days) to ensure at least 1 unit is on shelf.
*   **Status**: [ ] Skipped (User Request: Risk Aversion)

### B. Zombie Stock Logic
*   **Current Logic**: "Dead Stock" checks if `ADS > Threshold`.
*   **The Gap**: New items have `ADS = 0`.
*   **Impact**: Allocation might reject valid new listings because they have no history.
*   **Fix**: Ensure `is_new_listing` flag bypasses all velocity filters for first 30 days.

### C. The "Freshness" Trap
*   **Current Logic**: Milk/Bread capped at `Cycle + 0.5 days`.
*   **The Gap**: If delivery comes late (afternoon instead of morning), you sell NOTHING in the morning.
*   **Impact**: Morning stockouts on daily staples.
*   **Fix**: Shift Fresh Cover to `Cycle + 1.2 days` (Carry over into next morning).
*   **Status**: [x] Fixed (v2026)

---

## 3. Deep Dive Findings (Structural Gaps)

### A. The "Seasonality Disconnect" (CRITICAL)
*   **Current Logic**:
    *   `OrderEngine` **USES** Seasonality (`apply_greenfield_allocation` blends seasonal map).
    *   `retail_simulator.py` **IGNORES** Seasonality (`simulate_daily_demand` uses raw Scorecard Average).
*   **The Gap**: The Store is stocked for Christmas, but the Simulated Customers are shopping for January.
*   **Impact**: Massive overstock/deadstock in High Season simulations because demand doesn't rise to meet the allocated supply.
*   **Fix**: Pass `seasonal_demand_map` to `retail_simulator.py` and use it to adjust `simulate_daily_demand`.
*   **Status**: [x] Fixed (v2026: Pass Through from Allocation App)

### B. Supply Chain Optimism (No Failures)
*   **Current Logic**: Lead time varies by +/- 20%. Order *always* arrives.
*   **The Gap**: In emerging markets, suppliers have a ~5-10% "No Show" rate (Vehicle breakdown, stockout at depot).
*   **Impact**: Simulation shows 98% Fill Rate where reality would be 85% due to supplier reliability.
*   **Fix**: Implement `if random() < supplier_failure_rate: quantity = 0`.
*   **Status**: [x] Fixed (v2026: 5% Failure Rate)

### C. Financial Rigidity (Monopoly Money)
*   **Current Logic**: `BudgetManager` checks "Allocated Limit". It does not check "Cash in Bank".
*   **The Gap**: Stores can be "Budget Profitable" but "Cash Poor" (e.g., all cash tied up in slow-moving Whisky).
*   **Impact**: Simulator allows reordering even if the store theoretically has $0 cash flow.
*   **Fix**: Implement `CashFlowManager` that tracks `Cash = StartCash + Sales - Purchases`. Block orders if `Cash < 0` (even if Budget > 0).
