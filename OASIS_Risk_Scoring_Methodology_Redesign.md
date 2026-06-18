# O.A.S.I.S. — Risk-Scoring Methodology Redesign

> **Goal:** Replace the static, attribute-derived GNN risk (see
> `OASIS_GNN_Methodology_Review.md`) with a **dynamic, outcome-grounded** risk
> score built from real Rhapta sales/GRN/PO/transfer data — and architect it so
> the *same* design scales straight onto a live POS feed without a rewrite.
> Design document. Grounded in the actual data schemas inspected 2026-06-18.

---

## 1. The core reframe

The current GNN answers the wrong question. It predicts a **static store
vulnerability** (a formula over footfall/budget/category) that never moves with
operations. The question ordering actually needs is:

> **What is the forward stockout exposure of this SKU at this store, right now —
> and what does that aggregate to at the store/network level?**

That is a **demand-vs-supply timing** problem, and every input to it is already
a live signal in the data. Risk becomes a *consequence* of measurable state, not
a label we invent.

### Definition (bottom-up, SKU-store-day)

```
risk_SKU(t) = P(stockout within horizon H | state as of t) × impact_SKU
```

- **P(stockout)** = probability that demand over the replenishment lead time
  exceeds what's available + on order:
  `P( D_LTD  >  SOH(t) + OnOrder(t) )`
  where `D_LTD` = demand during lead time, modelled with its **mean and
  variance** (both observable per SKU), and lead time `L` with **its** mean and
  variance (observable per supplier).
- **impact_SKU** = value/criticality weight (`revenue`, `sales_rank`,
  `margin_pct` from `sales_profitability_intelligence`) — a stockout on a
  rank-1 staple matters more than on a long-tail SKU.

### Store / network rollup (this is where a GNN legitimately re-enters — §5)

```
risk_store(t) = value-weighted aggregate of risk_SKU(t) over the store's SKUs
```

This store score is now **dynamic** — it moves daily as stock depletes,
deliveries land, demand shifts, and suppliers slip. That is exactly the property
the old score lacked.

---

## 2. The label — learn from realized outcomes, not a formula

The model must be trained against **events that actually happened**, reconstructed
from the ledger we already have:

### 2a. Inventory reconstruction (per SKU, over time)
```
SOH(t) = SOH(t0)
        + Σ GRN_Qty         (grnds*.xlsx — dated receipts)
        + Σ STI_Qty (in)    (trn_*.xlsx — transfers in)
        − Σ sales           (sl*/sales_forecasting monthly → daily via ADS now,
                             exact daily once live POS is connected)
        − Σ STO_Qty (out)   (trout_*.xlsx — transfers out)
        − Σ returns         (prts*.xlsx)
```
Anchor `SOH(t0)` to the `dept.*` STOCK snapshot — **dated 2026-01-20** — and
walk backward/forward over the dated GRN/transfer/sales flows to build a real
time-indexed inventory universe. (A known-date snapshot is exactly what makes
the reconstruction verifiable; the live POS stream simply extends it forward.)

### 2b. Realized stockout / lost-sales labels
- **Demand-side stockout:** `SOH(t) ≈ 0` while `ADS > 0` → lost-sales day(s).
- **Supply-side failure (direct labels, no reconstruction needed):**
  - `PO Qty > GRN Qty` on a received line (grnds) → under-fill.
  - `prts.Reason == "Short Supply"` → supplier failed to deliver.
  - PO `Status == Authorized` with no GRN past `estimated_delivery_days` → late.
- **Label per SKU-day:** `y_t = 1` if a stockout/short-supply occurred in
  `[t, t+H]`, else `0` (classification). A regression variant predicts
  **lost-sales units** = `ADS × stockout_days`.

These are real, auditable outcomes — the thing the current GNN label completely
lacks.

---

## 3. Dynamic features (computed *as of* date t — leak-free)

Every feature below varies over time, which is the whole point. All are
computable now from the listed sources; all update naturally from a live feed.

| Group | Features | Source (now) |
|---|---|---|
| **Demand** | trailing ADS (30/90d), demand CV/volatility, trend %, intermittency (`months_active`/10), recent acceleration | `sales_forecasting` monthly series → ADS |
| **Position** | on-hand SOH, **days-of-cover = SOH/ADS**, on-order qty, expected-arrival gap | reconstructed SOH; open POs |
| **Supply** | supplier `estimated_delivery_days`, **lead-time σ** (`lata_stdev_days`/`lata_cv`), `avg_fulfillment_rate`, `reliability_score`, short-supply rate, days-since-last-GRN | `supplier_patterns`, `supplier_quality`, grnds |
| **Calendar / exogenous** | payday window, month-phase sin/cos, holidays, weather | derivable per date |
| **Impact** | `sales_rank`, `revenue`, `margin_pct`, department | `sales_profitability` |

> **This is the fix for the dead-feature bug.** In the current model, the
> dynamic columns are constant-zero during training so their weights never
> learn. Here, every sample is a (SKU, date) pair across 10 months × tens of
> thousands of SKUs, so demand/cover/lead-time **carry real variance** and the
> model is forced to learn a response to them.

---

## 4. The primary model — calibrated per-SKU stockout risk

The workhorse is **not** a GNN. Stockout risk is a per-SKU temporal problem with
tabular, interpretable drivers, so the right tool is a **calibrated
gradient-boosted classifier** (or a quantile demand model) predicting
`P(stockout in H)`:

- Inputs: §3 features. Target: §2 label.
- **Calibration** (isotonic/Platt) so `P=0.3` means ~30% empirically — essential
  because the score will set safety stock.
- **Interpretable**: SHAP/feature importances explain *why* a SKU is at risk
  ("low cover + slow supplier + payday spike") — far more trustworthy to
  operators than an opaque GNN number.
- **Maps directly to safety stock** (§6): replaces the crude `×1.3` with a
  service-level target derived from the predicted demand-during-lead-time
  distribution: `safety_stock = z(service_level) × σ_LTD`.

A pure statistical baseline (newsvendor / `σ_LTD` from demand & lead-time
variance) should be built **first** — it may already be good enough, and it's
the honest yardstick every ML model must beat.

---

## 5. Where the GNN legitimately belongs — the SKU substitution/supply graph

> **Revised after inspecting `neutral_network_export/` (2026-06-18).** The
> current model trains on the wrong graph (14 store nodes, static attributes).
> The right substrate already exists: a **heterogeneous product graph** of
> **23,511 SKU + 549 Supplier + 256 Department nodes** with **415,509 edges** —
> `substitution` (113k SKU↔SKU), `upstream_supply` (SKU→Supplier),
> `downstream_demand` (SKU→Department).

The GNN should sit **on top of** the per-SKU risk (§4), not replace it, and its
unique value is the structure a tabular model cannot see:

- **Nodes:** SKU (primary), Supplier, Department. **Node *state* (must be
  time-varying, refreshed from the feature store):** trailing ADS, days-of-cover,
  on-hand/on-order, supplier lead-time σ, recent stockouts. *(The export's
  `velocity_ads` is real but a static snapshot; node state has to update — see
  Appendix A.)*
- **Edges (topology is stable, reusable):**
  - **`substitution` (SKU↔SKU)** — the headline. When a SKU stocks out, demand
    spills to its substitutes; a SKU's *effective* stockout risk is lower when
    its substitutes are well-stocked. A per-SKU model is blind to this; a GNN
    over substitution edges learns it directly.
  - **`upstream_supply` (SKU→Supplier)** — a degrading supplier raises stockout
    risk for **all** its SKUs at once; the supplier node propagates that shared
    shock.
  - **`downstream_demand` (SKU→Department)** — category-level demand correlation.
- **What it learns (against realized outcomes from §2):** the *graph correction*
  to the bottom-up risk — substitution mitigation, shared-supplier shock,
  category co-movement.
- **Training:** time-varying node state, temporal walk-forward split, real
  stockout labels, fixed seed, held-out metric (§7).

When live POS spans multiple stores, add a **store / store×department layer**
on the same principle (edges = transfer feasibility from `haversine` + cluster +
historical `trn`/`trout` flow; learns "a neighbour's excess lowers my risk" and
correlated demand shocks). The SKU substitution layer is buildable first because
its graph already exists; the store layer waits for multi-store live data.

This is a principled use of a graph — model the substitution/supply *network*
effects a per-SKU model can't — not a single static store score doing everything.

---

## 6. Coupling risk → ordering (the safe operational wire)

Replace the opaque multiplier with inventory theory the risk model parameterises:

```
reorder_point   = ADS × lead_time            (cycle stock)
safety_stock    = z(service_level) × σ_LTD   (σ_LTD from demand σ + lead-time σ)
order_up_to     = reorder_point + safety_stock
```

The risk model supplies `σ_LTD` and (optionally) raises the **service level** for
SKUs it flags high-risk. This is self-calibrating, explainable, and — unlike the
current GNN — provably responsive to live state. Graceful degradation is
preserved: if the model/feed is down, fall back to the inventory heuristic, which
already runs on live SOH/ADS.

---

## 7. Training & validation discipline (non-negotiable)

The failures in the current model are as much process as design:

- **Temporal walk-forward split** — train months 1–7, validate 8, test 9–10.
  Never random-split a time series (leakage).
- **Fixed seed**, versioned artifacts, recorded data window → reproducible.
- **Held-out metrics:** PR-AUC / precision@recall at the operating threshold for
  the stockout classifier; pinball/MAE for demand quantiles; a **calibration
  curve**.
- **Business backtest:** simulate ordering *with vs without* the risk model over
  held-out months; measure realized stockouts and excess trapped capital. The
  model earns its way into ordering **only if it beats the inventory baseline on
  held-out outcomes.** Otherwise it stays a monitoring aid.

---

## 8. Live-POS architecture (future-proofing)

The design is feed-agnostic — the same features/labels/model scale from the
current monthly files to a live stream with no redesign:

1. **Feature store** updated incrementally from POS sales, GRN receipts, and
   transfer events → daily (later intraday) SOH, ADS, on-order, supplier stats.
2. **Daily scoring job** → `risk_SKU` → store/network rollup → cached for the
   consoles and the ordering path.
3. **Drift monitors** on features/labels auto-flag retraining.
4. **Graceful degradation** unchanged: model optional, inventory heuristic is the
   floor. (Mirrors the `gnn_service` pattern we already built.)

What changes with live POS: stockout reconstruction becomes **exact and daily**
(today it's monthly→daily-interpolated), enabling intraday risk. Nothing in §1–7
needs to change.

---

## 9. What we can build now vs. with live POS

| Capability | Now (these files) | With live POS |
|---|---|---|
| Demand mean/variance/trend per SKU | ✅ monthly series + ADS | exact daily |
| Lead-time mean/variance per supplier | ✅ `supplier_patterns` | continuously updated |
| Realized stockout / short-supply labels | ✅ reconstructed + `prts`/fill-rate | exact, real-time |
| Per-SKU stockout-risk model + backtest | ✅ at monthly granularity, 10mo | daily, online |
| Network GNN layer | ⚠️ single store (Rhapta) — design only | ✅ once multi-store live |

**So the immediate, buildable step is the per-SKU bottom-up risk model + the
statistical baseline + the backtest harness** — all on real data, with features
that actually vary and labels that actually happened. The GNN is repositioned as
a later network layer, retrained under §5/§7 discipline, and is **not** allowed
near order quantities until it beats the baseline on a held-out backtest.

---

## 10. Proposed phased implementation

1. **P1 — Ledger & labels.** Build the SKU inventory reconstruction + realized
   stockout / short-supply labels from grnds/po/trn/trout/prts + sales. Validate
   against the `dept` STOCK snapshot. *(pure, testable)*
2. **P2 — Feature builder.** As-of-date, leak-free feature store (§3) from the
   existing forecasting/supplier intelligence. *(pure, testable)*
3. **P3 — Statistical baseline.** `σ_LTD` newsvendor stockout probability +
   safety stock. The yardstick.
4. **P4 — Calibrated ML risk model.** GBM on P1+P2, walk-forward, calibrated,
   backtested vs P3.
5. **P5 — Wire to ordering** (the real S4): safety stock from §6, gated behind
   the backtest result and a service-level config.
6. **P6 — Network GNN layer** (multi-store/live): retrain per §5/§7; monitoring
   first, ordering only after a network backtest.
```
P1 → P2 → P3 (baseline) → P4 (beat baseline?) → P5 (wire, gated) → P6 (GNN, later)
```

**Bottom line:** risk should be the *output of measurable demand-vs-supply
state*, learned from stockouts that really occurred and validated on held-out
time — not a static formula a model memorises. The data already supports the
buildable core (P1–P4); the GNN earns a narrow, later, network-only role.

---

## Appendix A — Data assets & caveats (verified 2026-06-18)

The real signals exist and are cleanly separated by layer. Verified by direct
inspection of `oasis/data/` and `neutral_network_export/`.

| Layer | Asset | Granularity / note |
|---|---|---|
| **Graph topology** | `neutral_network_export/{nodes.csv, edges.csv, full_graph.json}` | 24,316 nodes (23,511 SKU / 549 Supplier / 256 Dept); 415,509 edges (link / substitution / upstream_supply / downstream_demand) |
| **Demand** | `corrected_ads_from_pos.json` (23,511 SKUs: old/new ADS, months_active, total_qty) | per-SKU corrected velocity from POS |
| **Demand (series)** | `sales_forecasting_2025` (24k SKUs: monthly_sales Jan–Oct, trend, months_active) | 10-month monthly series + intermittency |
| **Demand (live shape)** | `historical_sales.csv` (Date, Item, Qty_Sold, prices, Txn_ID) | transaction-level — the schema a live POS feed will stream |
| **Supply** | `supplier_patterns_2025` (est. delivery days, lata σ/CV, fulfillment rate, reliability) | per-supplier lead-time mean **and variance** |
| **Supply (cadence)** | `sku_grn_frequency.json` (19,492 SKUs) | per-SKU resupply frequency |
| **Supply (failures)** | `supplier_quality_scores_2025` (short_supply_returns, quality_score) + `prts_*.xlsx` ("Short Supply") + GRN `PO Qty`>`GRN Qty` | direct stockout-cause **labels** |
| **Position / flow** | `dept_*.xlsx` STOCK snapshot, `transfers_registry.json`, `trn_/trout_*.xlsx`, dated `grnds_*.xlsx` | anchor SOH + dated in/out flows |

**Caveats — noise to avoid (verified):**

1. **`rhapta_fill_rate` in the graph export is unusable** — 22,216 / 23,511 SKU
   nodes are 0. Compute fill rate from GRN `PO Qty` vs `GRN Qty`, not this column.
2. **Edges are unweighted** (`source, target, relation` only). Substitution
   *strength* is not encoded — derive it (same department + price proximity +
   demand co-movement) or start binary.
3. **Node features are a static snapshot** (export dated Mar 27). The topology is
   stable and reusable; **node state must be refreshed** from the feature store
   (§3) — this is the live-POS coupling.
4. **`supplier_delivery_gaps.json` is a test stub** (`{"TEST_VND":[1,2,3]}`) —
   ignore; real gap data is in `supplier_patterns`' `lata_*` fields.
5. **Demand is dominated by slow/intermittent movers** — `velocity_ads` median
   ≈ 0.038 units/day (max ≈ 727). The stockout model must handle **intermittent
   demand** (Poisson / compound, not a normal approximation).
