# O.A.S.I.S. — Intelligence → Ordering Pipeline (Deep Analysis)

> How intelligence (demand/ADS, GNN risk, AMIT/DHARAM/LATA/MANDE, supplier
> cadence) flows into **purchase-order quantities** across the three systems:
> **Command Center** (`run_command_center.bat` → `ops_dashboard.py`),
> **Operations Console** (`run_oasis.bat` → `app.py`/`shell.py`),
> **Intelligence Console** (`run_oasis_intel.bat` → `app_intel.py`/`intel.py`).
> Read-only analysis, code-traced. Generated 2026-06-19.

---

## 1. The shared pipeline (single source of truth)

All ordering runs through one core: `SimulationOrderUtil` in
[simulation_bridge.py](oasis/logic/simulation_bridge.py).

```
fetch_enriched_products(org)         # adapter: ADS, demand_cv, on_order, trend, months_active
        │
prepare_sku_data()                   # → engine.enrich_product_data()
        │
calculate_order_quantity(            # the core math (L99-324)
   enriched, gnn_risk_score=…)       #   ① AMIT blacklist  → qty 0
        │                            #   ② MANDE purge     → qty 0 (unless staple/essential)
        │                            #   ③ HALO            → key-SKU protect
        │                            #   ④ fresh-stale / dead-stock → qty 0
        │                            #   ⑤ supplier calendar (median_gap_days) → ordering day?
        │                            #   ⑥ safety_buffer = base·(1+vol·cv)·GNN_mult
        │                            #   ⑦ ROP → target coverage → net req → key-SKU +20%
finalize_orders()                    # apply_safety_guards (rounding/caps)
        │
CTS.optimize_network()               # transfers adjust orders (gap-plug)
        │
apply_minimum_order_gate()           # MOQ/MOT → below-threshold routed to transfers
```

**Intelligence inputs and where they enter:**

| Intelligence | Enters at | Effect on order qty |
|---|---|---|
| ADS / demand_cv / on_order | enrichment | drives ROP, target stock, net requirement |
| AMIT dead-stock blacklist | ① | hard block (qty 0) |
| MANDE supplier purge | ② | hard block unless staple/essential |
| HALO anchor list | ③ | protect/boost |
| Supplier cadence (`median_gap_days`, calendar) | ⑤ | ordering-day gating, cycle coverage |
| **GNN store risk** | ⑥ via `gnn_risk_score` | safety-buffer ×1.0→1.3 when risk>0.5 |
| DHARAM demand patch | *enrichment (ADS correction)* | indirectly via ADS |
| **LATA supplier toxicity** | **— (see F3) —** | **not applied to the replenishment safety buffer** |

---

## 2. Per-system invocation — where they diverge

| Stage | Command Center | Operations Console | Intelligence Console |
|---|---|---|---|
| Enrichment | `fetch_enriched_products` | `fetch_enriched_products` | (Sim-Lab only) |
| Core math | `calculate_order_quantity` | `calculate_order_quantity` | `calculate_order_quantity` (what-if) |
| **`gnn_risk_score`** | **`store_risk` from `get_all_store_risks`** ([ops_dashboard.py:1904-1908](ops_dashboard.py)) | **`0.0` (default)** ([shell.py render_ordering](oasis/ui/shell.py)) | **`0.0`** ([intel.py:482,490](oasis/ui/intel.py)) |
| finalize + CTS + MOQ | yes | yes | n/a (no PO output) |
| Transactional PO output | yes | yes | **no** (monitoring + what-if only) |

The pipeline is **identical** across Command Center and Operations Console
except for one input: **the GNN risk score.** The Command Center threads the
blended store risk; the Operations Console passes `0`.

---

## 3. Findings

### F1 · Risk-threading divergence — same store, different POs — **HIGH**
For an identical store and stock position, the **Command Center inflates safety
stock** (when blended risk > 0.5) while the **Operations Console does not**. An
operator moving from the legacy Command Center to the new Operations Console
gets **systematically lower safety stock** on at-risk stores — a silent,
material difference in PO output between two tools that claim the same logic.

### F2 · The unvalidated GNN is ALREADY in live PO quantities (Command Center) — **HIGH (governance)**
`get_all_store_risks` returns a **blend of inventory risk + the static GNN
prior** (sigmoidal brake, `gnn_risk_blend_ratio` default **0.5**). That blended
value is passed as `gnn_risk_score` into `calculate_order_quantity`, where
risk > 0.5 inflates safety stock up to ×1.3. **So the Command Center's PO
quantities are already shifted by the GNN** — the exact unvalidated-model-in-
operations risk we deliberately gated S4 for. The risk-validation gate (GAP-2 /
the methodology) is **not enforced in the Command Center**. (Mitigating: the
high-risk values are inventory-dominated; the static-GNN nudge is small — but
non-zero, at up to 50% blend weight, and unvalidated.)

### F3 · LATA supplier toxicity never reaches the replenishment safety buffer — **MEDIUM**
LATA's stated purpose (Supplier Shield) is "inflate safety stock for unreliable
suppliers." But `lata_variance_multiplier` is **not referenced** in
`calculate_order_quantity` or the order-engine ordering path; the safety buffer
is `base·(1+vol·cv)·GNN_mult` with **no LATA term**. LATA's output instead feeds
**AMIT's GMROI** (allocation prioritisation, `amit_gatekeeper.load_lata_patterns`).
So a toxic-supplier SKU is **not** given extra replenishment cover — LATA is
computed-but-underused for ordering, analogous to the old GNN issue.

### F4 · Live enrichment doesn't supply ROP / coverage targets — **LOW/MEDIUM**
The adapter's `fetch_enriched_products` sets ADS / cv / on_order but **not**
`reorder_point` or `target_coverage_days`; `enrich_product_data` doesn't set them
either. So in the live path the **ROP fallback always fires**
(`reorder_point ≤ 0 → ADS·(lead+base_safety·(1+cv))`) and coverage defaults to
the cycle heuristic. The math is sound, but the "intelligence ROP / forecast
coverage" is heuristic, not a dedicated forecasting layer — worth naming so it's
not mistaken for a model-driven target.

### F5 · Governance + core math are consistent (the good news) — ✓
AMIT / MANDE / HALO / fresh-dead blocks, the net-requirement math, key-SKU boost,
`finalize_orders`, CTS network optimisation, and the MOQ/MOT gate are **shared
and identical** across Command Center and Operations Console (both via the engine
+ `calculate_order_quantity`). The pipeline is genuinely single-sourced; only the
risk input diverges.

### F6 · Intelligence Console ordering is what-if only, and risk-blind — **LOW**
`app_intel` does no transactional ordering; its only ordering touchpoint is the
Sim-Lab scenario what-if, which calls `calculate_order_quantity` without a risk
score. Fine for a monitoring console, but its what-if therefore differs from the
Command Center's risk-aware numbers.

---

## 4. The core tension

Two findings pull in opposite directions and must be resolved **together**:
- **F1** says the Operations Console *should* be risk-aware (to match the Command
  Center).
- **F2** says the Command Center *shouldn't* be injecting the **unvalidated GNN**
  into POs (the gate we set).

The reconciling principle is the one from the risk-methodology work: **the
trustworthy risk signal is the inventory component; the GNN prior is
unvalidated.** So the fix is not "turn GNN on everywhere" nor "turn risk off
everywhere" — it's **make all three consume the same risk, sourced so the GNN
contributes nothing until validated.**

---

## 5. Recommendation

1. **Unify risk-threading through `gnn_service.store_risk`** for all ordering
   surfaces (Command Center, Operations Console, Intelligence Sim-Lab), replacing
   the bespoke `get_all_store_risks` call and the hard-coded `0.0`.
2. **Set the GNN blend weight to 0 (inventory-only) until validated** — a single
   config (`gnn_risk_blend_ratio = 0`). This simultaneously:
   - closes **F1** (all three identical, risk-aware),
   - closes **F2** (no unvalidated GNN in any live PO; the inventory-risk
     inflation — which IS trustworthy — still applies),
   - delivers **S4** (Operations Console becomes risk-aware) *safely*.
   Raise the weight only after the GNN beats the baseline on real-outcome
   backtests.
3. **F3 — thread LATA into the replenishment safety buffer** (or explicitly
   document that LATA is allocation-only) so the Supplier Shield does what it
   says for ordering.
4. **F4 — confirm/curate the ROP & coverage source** for the live path (decide
   whether the fallback heuristic is the intended production behaviour or a
   forecasting layer should supply it).

**Headline:** the ordering pipeline is admirably single-sourced — but the one
input that differs (risk) means the Command Center already ships unvalidated-GNN-
shifted POs while the Operations Console ships risk-blind ones. Unifying on
`store_risk` at **inventory-only weight** makes all three consistent, risk-aware,
and gate-compliant in one move.

---

## 7. Update — IMPLEMENTED (F1 + F2 closed)

`gnn_service.ordering_risk(products, gnn_risk_score=None)` is the single,
gate-compliant ordering-risk source: **inventory-only by default**; the GNN
contributes only when `OASIS_GNN_ORDERING_WEIGHT > 0` (set after the GNN beats
the baseline). All three surfaces now route through it:
- **Operations Console** (`shell.render_ordering`) — now risk-aware (S4 delivered
  safely, inventory-only).
- **Command Center** (`ops_dashboard` Smart Ordering) — ordering uses
  `ordering_risk` (gate-compliant); the blended `store_risk` stays for *display*
  only. **F2 closed**: the unvalidated GNN no longer shifts live PO quantities.
- **Intelligence Sim-Lab** — same `ordering_risk` threaded into the what-if.

Result: identical, risk-aware, gate-compliant ordering across all three (**F1
closed**), with one config switch to enable the GNN once validated. F3 (LATA→
safety buffer) and F4 (ROP source) remain open follow-ups.
