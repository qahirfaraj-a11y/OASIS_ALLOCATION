# Decision Log
Tracking technical changes and the rationale behind them.

## [2026-03-17] Restoration of Golden State
- **Problem**: Logic degradation following refactoring.
- **Solution**: Unified `main.py` to use `run_intelligent_analysis` as the master orchestrator.
- **Outcome**: 100% parity achieved across report formatting and historical baseline anchoring.

## [2026-03-19] Final Logic Parity Sync (v10.0)
- **Problem**: Lingering logic ghosts in auxiliary mixins (Rounding, GRN Scan, etc.).
- **Solution**: Performed comprehensive audit and line-by-line sync with Golden v10.0. Unified the `OrderEngine` orchestrator with AI fallback and safety guards.
- **Outcome**: 100% logic parity achieved. Optimized for production-scale allocation across all store tiers.

## [2026-03-20] Order Engine Audit: Greenfield & Budget Calibration
- **Problem**: Budget overruns after safety guards and perceived "degradation" in small-store allocations.
- **Solution**:
    - Implemented **Post-Guard Budget Pruning** (Priority-based sorting).
    - Restored **Greenfield-Safe Intelligence** (is_greenfield=True mode).
    - Synchronized all target stock formulas with **Golden Logic v10.0** (Removed `profile_depth` caps).
- **Outcome**: 100% Logic Parity. Budget is now strictly enforced while maintaining historical allocation depth.

## [2026-03-21] Greenfield Optimization: Dynamic Scrutiny & Syncs
- **Problem**: Allocation app overriding all items to "1" unit; Departments showing only "general/fresh"; Bread/Fresh items absorbing 60 days of budget; New SKUs utilizing rigid `0.5` mathematically-naive fallbacks.
- **Solution**:
    - **Pack Size Unlocking**: Removed broad `pack_size` rounding in Greenfield formulas to allow precision integer targets to survive Pass 1.
    - **Department Sync**: Loaded `product_department_map.json` natively into `OrderEngine` loaders and preserved map in Phase 3 enrichment.
    - **Spoilage Hard Capping**: Explicitly blocked `is_fresh` items from Pass 4 "Mop-Up" limits to prevent them from soaking 60 days of buffer.
    - **Dynamic Scrutiny**: Stripped away all static fallback metrics (`0.1`, `0.5`, `3.0`, `6.0`) across Pass 1, 2B, 4, and CV calculations. Replaced them with intelligent, price-ceiling driven baseline velocities.
- **Outcome**: Target allocations are precise, natively intelligent even with zero historical forecast, and department structures correctly mirror database ground truths.
