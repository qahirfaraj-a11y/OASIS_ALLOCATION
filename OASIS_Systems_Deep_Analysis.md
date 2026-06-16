# O.A.S.I.S. — Deep Systems Analysis (Four Launchers)

> **Scope.** A grounded, flow-level analysis of the four entry points the user
> named, their full launch chains, the logic/processes behind each, and the
> gaps + underutilizations across them. **Analysis only — no code changes.**
> Generated 2026-06-14.
>
> Launchers analyzed:
> `run_command_center.bat` · `run_market_intelligence_tool.bat` ·
> `run_oasis.bat` · `run_oasis_intel.bat`

---

## 0. The four systems at a glance

All four resolve through the unified `entrypoint.py` to a Streamlit app, but they
split into **two generations**:

| Launcher | entrypoint mode | Script | Lines | Generation |
|---|---|---|---|---|
| `run_oasis.bat` | `--mode shell` | `app.py` | ~40 | **New shell** (Operations) |
| `run_oasis_intel.bat` | `--mode intel` | `app_intel.py` | ~40 | **New shell** (Intelligence) |
| `run_command_center.bat` | `--mode dashboard --dashboard command` | `ops_dashboard.py` | 3,312 | **Legacy** monolith |
| `run_market_intelligence_tool.bat` | `--mode dashboard --dashboard stgat` | `st_gat_dashboard.py` | 844 | **Legacy** GNN monolith |

The two new consoles are thin callers of `oasis/ui/shell.run_console()` over a
shared foundation (`theme`, `auth`, `components`, `telemetry`, `journey_state`).
The two legacy apps are self-contained monoliths with their own inline auth,
CSS, data loading, and state. **This generational split is the root of most
gaps below: the same capability exists in both worlds, implemented differently.**

```
                         entrypoint.py (modes: shell|intel|dashboard|api|bridge|engine|migrate|...)
                                              │
        ┌──────────────────────┬──────────────┴───────────────┬───────────────────────────┐
   app.py (Operations)   app_intel.py (Intelligence)   ops_dashboard.py (Command)   st_gat_dashboard.py (Market Intel)
        │                      │                              │                              │
        └─ shell.run_console ──┘                              │                              │
                 │                                            │                              │
        oasis/ui/{theme,auth,components,telemetry}      inline auth + inline CSS       GNN (StoreGraphNetwork)
                 │                                      AlertMonitor, IntraDaySim       NetworkSimulator
        oasis/logic/* (engines)  ◄───────────── all four ultimately read ─────────►  oasis/logic + models/
                 │
        DB factory → mock_pos_erp.db (SQLite, WAL)
```

---

## 1. `run_oasis.bat` — Operations Console (app.py)

### Flow
1. `run_oasis.bat` → venv detect → `entrypoint.py --mode shell` → `run_shell(8500)`
   → `streamlit run app.py`.
2. `app.py`: `set_page_config` → `shell.run_console(registry=build_registry(), …)`.
3. `run_console`: inject SYS v2.9 theme → `ensure_seeded` (only if
   `OASIS_SEED_PASSWORD` set & no users) → `require_login` (one gate) → resolve
   role → filter the 9-page registry → sidebar nav + logout → page-view
   telemetry (deduped) → `safe_render(selected.render, ctx)`.

### Per-page processes (all native)
- **Home/Journey** — reads `journey_state.json`; renders mode/phase badge,
  value-recovered meter, 7-stage rail; operator/exec can human-confirm phase
  advance (`advance_phase`, audit-logged).
- **Ordering** — `SimulationOrderUtil.prepare → calculate_order_quantity →
  finalize` → `CTS.optimize_network` → `apply_minimum_order_gate`; MOQ failures
  recorded via `moq_failure_store`; PO grouped by supplier; push to approvals.
  Session-cached behind "Regenerate" (no render side-effects).
- **Approvals** — `fetch_pending_pos` → approve/reject via `update_po_status`
  (ops_admin / regional_manager).
- **Transfers** — `CTS.scan_network_opportunities` (PULL/PUSH, pending-aware) →
  queue non-fresh transfers.
- **Suppliers** — `run_lata` → reliability scorecard via `classify_supplier`.
- **Allocation** — `greenfield_runner` (the decomposed multi-pass engine).
- **Analytics** — `compute_health_metrics` vs Playbook targets.
- **Diagnose / Settings** — operator-only: forensic-output tracker; users +
  engine flags + adoption telemetry.

### Strengths
Clean separation, role-gated, error-safe (`safe_render`), telemetry-instrumented,
all on the unified/hardened logic layer. This is the healthiest of the four.

### Gaps / underutilization
- **G-OPS-1:** the Ordering page omits the on-order-intelligence display and the
  GNN risk multiplier that the command center applies — it calls
  `calculate_order_quantity` *without* a `gnn_risk_score`, so safety-stock never
  inflates for high-risk stores here (the legacy command center does). Net: the
  shell's PO math is slightly less risk-aware than the legacy one.
- **G-OPS-2:** `journey_state.value_recovered` is **never populated by real
  data** — nothing computes capital recovered and calls `set_value_recovered`.
  The meter shows 0 unless manually set. The AMIT capital-recovery figure exists
  conceptually but is not wired to the journey state.
- **G-OPS-3:** no scheduler integration — the daily pipeline (`scheduler_service`,
  `daily_pipeline`) is not reachable from the console; it lives only in the
  legacy sidebar "Run Daily Pipeline" button.

---

## 2. `run_oasis_intel.bat` — Intelligence Console (app_intel.py)

### Flow
Identical scaffolding to Operations (same `run_console`), different registry
(`intel.build_intel_registry`), port 8510. 7 native pages.

### Per-page processes
- **Pulse** — `compute_health_metrics` + `velocity_alert_rows` over network stock.
- **Velocity Alerts** — days-cover burn-down (`classify_cover`, threshold slider).
- **Stock Review** — `stock_review_summary` cover-class rollup.
- **Live Sales** — `fetch_sales_history` → revenue/units KPIs, `top_movers`,
  daily trend.
- **Network Intel** — `per_store_health` + read-only `CTS` opportunities
  (inventory-based, **no GNN**).
- **Executive ROI** — `roi_scorecard_rows` (live health vs Playbook targets).
- **Simulation Lab** — `black_swan_events.SCENARIO_TEMPLATES` what-if recomputing
  order qty via `SimulationOrderUtil`.

### Gaps / underutilization
- **G-INT-1 (biggest):** the Intelligence Console **does not use the GNN at all**.
  Network Intel is inventory-heuristic only; the ST-GAT store-risk model
  (`StoreGraphNetwork`) that the market-intelligence tool and command center load
  is absent here. The "intelligence" console is the one place the trained model
  *isn't* used.
- **G-INT-2:** no real-time/auto-refresh — every page is button-gated pull. There
  is no live polling, no alert push, no scheduled refresh. "Velocity alerts" are
  on-demand, not alerting.
- **G-INT-3:** `AlertMonitor` (the 200%-spike velocity detector in the command
  center) is **not reused** — the console reimplements a simpler days-cover
  heuristic instead of the existing spike engine.
- **G-INT-4:** Live Sales has no intraday dimension — the `IntraDaySimulator`
  (hour-by-hour projection) is command-center-only.

---

## 3. `run_command_center.bat` — ops_dashboard.py (legacy monolith)

### Flow (3,312 lines, single script)
1. `--mode dashboard --dashboard command` → `run_dashboard("command", port)` →
   `streamlit run ops_dashboard.py`.
2. Inline: `load_env_local` → resolve `DB_PATH` → `ensure_oasis_tables` →
   ~100 lines of inline glassmorphism CSS → cached resources
   (`get_connector/get_adapter/get_order_engine/get_calendar`) →
   `get_all_store_risks` (GNN) → **inline auth gate** (`authenticate`, showcase
   bypass) → sidebar (multi-day `IntraDaySimulator`, phase indicator, ERP health,
   Master Hub uplink, push-rebalance thresholds, daily pipeline button, logout,
   audit log) → 11 role-gated tabs.

### The 11 tabs (processes)
Executive ROI · Live Sales Feed · Transfer Intelligence (incl. GNN risk + intraday
live transfers + item-level heuristic + CTS scan) · End-of-Day Stock · Smart
Ordering (incl. chaos scenarios + network optimization + MOQ gate) · OASIS
Processor (batch upload) · Allocation Engine · Simulation Lab · Analytics ·
Settings · Supplier Intelligence.

### Unique machinery not present elsewhere
- **IntraDaySimulator** — hour-by-hour stockout/transfer projection driving the
  date/hour sidebar and the live sim views.
- **AlertMonitor** (spike_threshold 200%) — velocity-spike detection.
- **get_all_store_risks** — GNN risk blended with an inventory heuristic via a
  sigmoidal brake; injects risk into the ordering safety buffer.
- **OASIS Processor** — batch file (Excel/CSV) upload → bulk recommendations.
- **Master Hub uplink** / showcase narrative.

### Gaps / underutilization
- **G-CC-1:** it is the *only* place several capabilities live (intraday sim,
  AlertMonitor, batch processor, GNN-blended ordering). Retiring it would lose
  them; keeping it means they never reach the new consoles → **permanent feature
  fork** unless migrated.
- **G-CC-2:** uses its own inline auth (not `oasis/ui/auth`), its own CSS (not
  SYS v2.9), no telemetry, process-global caches shared across sessions →
  inconsistent with the consoles and the multi-user isolation concern.
- **G-CC-3:** the showcase ROI/Master-Hub uplink are partly **narrative
  placeholders** ("Last Pulse: 45s ago" is hard-coded), not live.
- **G-CC-4:** 3,312 lines, untestable by construction — business logic lives
  inside `with tab:` blocks. (The logic *engines* it calls are tested; the tab
  orchestration is not.)

---

## 4. `run_market_intelligence_tool.bat` — st_gat_dashboard.py (GNN monolith)

### Flow (844 lines)
1. `--dashboard stgat` → `run_dashboard("stgat", port)` → `streamlit run
   st_gat_dashboard.py`.
2. `load_resources()`: load `stores_network.json` → `NetworkSimulator` → derive
   feature dim → build `StoreGraphNetwork` → load `st_gat_v2.pt` (**with a
   runtime 29→30 input-dimension monkey-patch** padding the LSTM weight) →
   `model.eval()`. Falls back to **random initialization** if the checkpoint is
   missing or mismatched.
3. Sidebar what-if controls (rush hour, rain intensity, budget scaling, sim-day
   step) → `sim.step()` agent simulation → feature-matrix injection (weather at
   index 28, budget at index 17) → `model(x_t, sim.edge_index)` inference →
   transfer-score patch.
4. **5 tabs:** Live Graph (attention/friction map) · Store Intelligence (DNA,
   top movers, revenue, category mix) · Store Clusters (hidden relationships) ·
   Strategic Expansion Engine (site analysis, proximity/impact grid) · Neural
   Ecosystem (supplier fragility & dominance, SKU affinity/substitution,
   whitespace/market-moat finder).

### This is the most sophisticated and the most isolated system
It is the only home of: the trained GNN, agent-based network simulation,
strategic expansion analysis, SKU affinity/substitution, and whitespace finding.
**None of these surface in any other launcher.**

### Gaps / underutilization
- **G-MI-1 (fragility):** the 29→30 dimension patch is a runtime band-aid — the
  checkpoint and the model definition have drifted apart. If the model grows
  again, this silently mis-pads or fails to a **random-initialized GNN** that
  still renders confident-looking output. There is no guard that the loaded model
  is actually trained.
- **G-MI-2:** every expensive asset (GNN inference, agent sim, expansion grid,
  affinity engine) is locked in this one app and **not exposed via the API or
  reused** by the Intelligence Console — the richest intelligence is the least
  reachable.
- **G-MI-3:** hard-coded relative paths (`stores_network.json`, `st_gat_v2.pt`)
  and `st.stop()` on missing files — brittle outside the project root; not on the
  DB factory or central config.
- **G-MI-4:** the agent simulation and the IntraDaySimulator (command center) and
  retail_simulator (sim_lab) are **three separate simulation engines** with no
  shared abstraction.

---

## 5. Cross-cutting analysis

### 5.1 The shared logic spine (healthy)
All four ultimately call `oasis/logic`: the decomposed multi-pass allocation
(`procurement_mixin._gf_*` via `GreenfieldPipeline`), `ConsolidatedTransferService`,
`SimulationOrderUtil`, the governance engines (AMIT/DHARAM/LATA/MANDE), and the
DB factory. This layer is unified and tested (~150 tests) — the consolidation work
already done. **The divergence is entirely in the presentation/orchestration
tier, not the logic tier.**

### 5.2 Capability duplication matrix (the core problem)

| Capability | Operations | Intelligence | Command Center | Market Intel |
|---|:--:|:--:|:--:|:--:|
| Allocation (greenfield) | ✅ native | — | ✅ tab | — |
| Smart Ordering | ✅ native | — | ✅ (+GNN, +chaos) | — |
| Transfers | ✅ act | ◑ read-only | ✅ (+GNN, +intraday) | — |
| Supplier intel | ✅ LATA | — | ✅ tab | ✅ fragility matrix |
| Network risk (GNN) | — | — | ✅ blended | ✅ full model |
| Velocity alerts | — | ◑ heuristic | ✅ AlertMonitor | ✅ velocity |
| Live sales | — | ✅ | ✅ (+intraday) | ✅ store intel |
| Stock review | — | ✅ | ✅ | — |
| Simulation/what-if | — | ✅ scenarios | ✅ chaos | ✅ agent sim |
| Exec ROI | — | ✅ | ✅ showcase | — |
| Expansion / whitespace | — | — | — | ✅ only |
| Batch upload | — | — | ✅ only | — |

Reading across rows shows the **fork**: e.g. transfers exist three ways, velocity
alerts three ways (two heuristic, one AlertMonitor), simulation **four** ways
(scenarios, chaos, agent sim, retail_simulator). Reading down columns shows
**Market Intel is a silo** (3 capabilities exist nowhere else).

### 5.3 Underutilized assets (built but barely used)
- **The trained GNN (`st_gat_v2.pt`)** — used in 2 of 4 apps, absent from the
  console branded "Intelligence."
- **The FastAPI layer (`server.py`/`bridge.py`, `/health`, `/metrics`)** — *no UI
  consumes it.* All four apps talk to the DB directly; the API seam built for a
  future client is dormant.
- **The mobile API (port 8550)** — no client (known; deferred).
- **`journey_state.value_recovered`** — the spine of the trust narrative, never
  fed by real capital-recovery numbers.
- **The scheduler (`scheduler_service`, APScheduler)** — only reachable via a
  legacy sidebar button; no console wires the daily cycle.
- **`AlertMonitor` spike engine** — command-center-only; the Intelligence console
  reimplements a weaker heuristic instead.
- **Multi-tenancy (`TENANT_ID`)** — dormant by design (onsite), but every query
  carries the column.

### 5.4 Data & runtime concerns (all four)
- **Single SQLite file** under WAL is the shared store for up to four concurrent
  Streamlit processes + APIs + scheduler — the known bottleneck; Postgres path
  exists but unused.
- **Per-process `@st.cache_resource`** — each app caches its own engine/connector;
  no shared warm cache; 4 apps = 4 engine loads.
- **GNN loaded per app** that uses it (command center + market intel) — torch
  initialized twice if both run.

---

## 6. Maturity / utilization scorecard

| Subsystem | Maturity | Utilization | Note |
|---|---|---|---|
| Logic engines (alloc/CTS/governance) | High | High | Unified, tested |
| Operations Console | High | High | Healthy reference |
| Intelligence Console | Med | Med | Missing GNN, alerts, live refresh |
| Command Center | High (features) / Low (structure) | Med | Feature-rich, untestable, forked |
| Market Intel (ST-GAT) | High | **Low** | Powerful, siloed, fragile loading |
| FastAPI layer | Med | **~Zero (by UI)** | Built, unconsumed |
| Scheduler / daily pipeline | Med | Low | Button-only |
| GNN model | High | Low–Med | 2/4 apps; fragile patch |
| journey_state value thread | Med | **Low** | Never populated |

---

## 7. Synthesis — the three findings that matter most

1. **A generational fork, not a feature shortage.** The capability set is rich;
   it is *scattered across two implementation generations*. The new consoles are
   clean but incomplete; the legacy apps are complete but isolated and
   unstructured. Every "gap" in §1–§4 is really "this exists, but in the other
   generation."

2. **The most valuable intelligence is the least reachable.** The trained GNN,
   agent simulation, expansion/whitespace, and SKU-affinity engines live only in
   the Market Intelligence tool — a single 844-line app with fragile model
   loading and no API exposure. The Intelligence Console, ironically, doesn't use
   the model at all.

3. **Built-but-dormant seams.** The FastAPI layer, the scheduler, the
   journey value-recovered thread, and the AlertMonitor spike engine are all
   built and tested-or-working, yet effectively unconsumed by the UIs. The
   platform is under-wired more than it is under-built.

---

## 8. Where analysis would point (not changes — directions)

- **Wire the GNN into a shared service** (e.g. behind the API or a cached
  `oasis/logic` helper) so the Intelligence Console and consoles can consume
  store-risk without each app re-loading torch — and add a *trained-vs-random*
  guard so a missing checkpoint fails loudly, not silently.
- **Unify the simulation engines** (IntraDaySimulator / agent sim /
  retail_simulator / scenario templates) behind one abstraction; today there are
  four.
- **Feed `journey_state.value_recovered`** from the AMIT capital-recovery figure
  so the trust narrative is real.
- **Reuse `AlertMonitor`** in the Intelligence Console instead of the weaker
  days-cover heuristic.
- **Decide the Command Center's fate:** migrate its three unique capabilities
  (intraday sim, batch processor, GNN-blended ordering) into the consoles, then
  retire it — or formally designate it the "power-user/legacy" surface and stop
  the drift.
- **Consume the FastAPI seam** from at least one surface, or acknowledge it as
  future-only, so it isn't mistaken for live infrastructure.

None of these are proposed as edits here — they are the analysis-derived
directions for a subsequent, deliberate decision.
