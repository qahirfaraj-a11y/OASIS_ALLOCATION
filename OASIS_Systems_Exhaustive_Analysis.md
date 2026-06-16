# O.A.S.I.S. — Exhaustive Line-Level Systems Analysis (Six Launchers)

> **Scope.** A line-grounded walkthrough of the six launchers named, their full
> code paths, what each line/block actually does, and every gap, latent bug,
> mock surface, and underutilization found while reading the code in full.
> **Analysis only — no code was changed.** Supersedes the flow-level
> `OASIS_Systems_Deep_Analysis.md` with line citations and newly-found defects.
> Generated 2026-06-17.
>
> Launchers: `run_command_center.bat` · `run_market_intelligence_tool.bat` ·
> `run_stgat.bat` · `run_oasis.bat` · `run_oasis_intel.bat` · `run_app.bat`.

---

## 0. Launcher → target resolution (read in full)

| Launcher | Command | Resolves to | Note |
|---|---|---|---|
| `run_command_center.bat` | `entrypoint.py --mode dashboard --dashboard command` | `ops_dashboard.py` (3,312 ln) | Streamlit, port 8501 |
| `run_market_intelligence_tool.bat` | `entrypoint.py --mode dashboard --dashboard stgat` | `st_gat_dashboard.py` (848 ln) | Streamlit |
| `run_stgat.bat` | **`python st_gat_dashboard.py`** | `st_gat_dashboard.py` | **⚠ bare python, not `streamlit run`** |
| `run_oasis.bat` | `entrypoint.py --mode shell` | `app.py` → `shell.run_console` | Operations Console, 8500 |
| `run_oasis_intel.bat` | `entrypoint.py --mode intel` | `app_intel.py` → `shell.run_console` | Intelligence Console, 8510 |
| `run_app.bat` | `entrypoint.py --mode desktop` | `oasis/main.py` (`ft.app`) | Flet native desktop |

**Finding L-1 (launcher bug).** `run_stgat.bat` (line 3) runs
`python st_gat_dashboard.py` directly. `st_gat_dashboard.py` is a Streamlit
script — executed under bare Python it runs top-to-bottom with no Streamlit
server; every `st.*` call emits "missing ScriptRunContext" warnings and renders
nothing. The *working* path to the same file is `run_market_intelligence_tool.bat`
(via `streamlit run`). So **two launchers point at one file; one of them is
broken.**

---

## 1. `ops_dashboard.py` — Command Center (exhaustive)

### 1.1 Boot sequence (lines 1–214)
- **1–33** imports (streamlit, pandas, numpy, plotly, torch-free here), logging
  `basicConfig`.
- **38–55** imports the whole logic surface: `UniversalConnector`,
  `PosErpAdapter`, `AlertMonitor`, `OrderEngine`, `RuleBasedLLM`,
  `SupplierCalendar`, `apply_safety_guards`, black-swan analyzers,
  `ConsolidatedTransferService`, `TransferRecord`, auth + audit, and
  `intraday_sim.IntraDaySimulator`.
- **60–169** `st.set_page_config` then ~100 lines of **inline glassmorphism CSS**
  (neon-emerald/amber/ruby, glass cards) — its own theme, not `oasis/ui/theme`.
- **176–199** `load_env_local` (hand-rolled `.env` parser — duplicates
  `python-dotenv` which is a pinned dep), resolves `DATA_DIR`/`DB_PATH`, calls
  `ensure_oasis_tables`.
- **203–216** cached resources: `get_connector` (now via DB factory — fixed
  earlier this session), `get_adapter`, `get_distance_map`.
- **227–269** `@st.cache_data` loaders: `load_sales_data`, `load_products`,
  `load_network_stock`, `load_sales_intel`, `_cached_ads_map`.
- **271–392** `get_all_store_risks(sim_hour)` — the **GNN risk engine**: loads
  `get_gnn_resources` (torch model), injects live stockout/critical ratios into
  the feature matrix (indices 24/25), runs `model(x_t, edge_index)`, blends GNN
  risk with an inventory heuristic via a **sigmoidal brake** (lines 378–387) so a
  flat/untrained GNN defers to inventory signal. **This is the single best piece
  of risk logic in the codebase — and it lives only here.**

### 1.2 Auth + identity (lines 427–528)
- **443–513** inline auth gate: `show_login_screen` + `authenticate(...DB_PATH)`,
  with a **showcase bypass** prefilling `ops_admin` / `OASIS_SEED_PASSWORD`.
  This is *separate* from `oasis/ui/auth.require_login`.
- **516–528** **Finding CC-A (guest fallback):** if no user is in session it
  fabricates a `guest` user with `{'tabs':{'live_sales':True}}`. On the normal
  `streamlit run` path this is dead (the gate `st.stop()`s first), but it is a
  latent "fail-open to live_sales" should the gate ever be bypassed.

### 1.3 Header, notifications, scheduler (lines 530–605)
- **540–543** instantiates `NotificationService(get_connector(), None)` in session.
- **546–548** instantiates `OasisScheduler(DB_PATH)` in session.
- **553–560** pulls active alerts, toasts the newest unread.
- **570–605** dynamic header HTML + alert popover inbox.
  **Finding CC-B:** `NotificationService` (real alert store) and `OasisScheduler`
  (real APScheduler wrapper) are **instantiated only in the Command Center** —
  neither console touches them.

### 1.4 Sidebar (lines 602–831)
Multi-day `IntraDaySimulator` (650–689, hour-by-hour stockout/transfer
projection), phase indicator, ERP health pulse, **Master Hub uplink (752–763 —
hard-coded "Last Pulse: 45s ago", static)**, push-rebalance thresholds
(cold/hot node days), "Run Daily Pipeline" button (785–800), logout, audit-log
expander.

### 1.5 Tab registry (lines 835–875)
11 tabs assembled from `user_perms['tabs']`; **role labels (533–537) and the
Settings role-emoji map (3144) cover only the 3 legacy roles** — the journey
roles render unlabelled (functional, but unstyled).

### 1.6 The 11 tabs — what's real vs theatrical

| Tab | Lines | Reality |
|---|---|---|
| Executive ROI | 896–1022 | **Mostly hard-coded theater** — see CC-C |
| Live Sales | 1028–1303 | Real: `load_sales_data` + intraday sim rows |
| Transfer Intelligence | 1305–1755 | Real: GNN risk + intraday + item heuristic + CTS scan |
| End-of-Day Stock | 1760–1801 | Real (short) |
| Smart Ordering | 1804–2337 | Real: pipeline + GNN risk + chaos scenarios |
| OASIS Processor | 2341–2451 | Real: batch upload → RuleBasedLLM → Excel |
| Allocation Engine | 2455–2608 | Real: greenfield engine |
| Simulation Lab | 2613–2817 | Real sim, **but GNN run is a no-op — see CC-D** |
| Analytics | 2822–2996 | Real trend/KPIs, **but PO query hits a wrong table — CC-E** |
| Settings | 3001–3221 | Real: config editor + thresholds + **scheduler control panel** + users + audit |
| Supplier Intelligence | 3226–3299 | Real: HHI concentration + failure simulator |

**Finding CC-C (Executive ROI is theater):** lines 916–947 render hard-coded
"14,282 SKU nodes", "95.2% Inference Confidence", "14ms Neural Latency", "4,122
SKU Affinities"; lines 1004–1006 plot **fixed baseline/optimized arrays**;
953–974 hard-code before/after fulfilment except a shadow-log-derived branch;
"Recaptured Capital" reads a `showcase_roi_savings` config string. This is a
demo/sales surface, not live analytics — and it is the tab executives see first.

**Finding CC-D (Simulation Lab GNN no-op — latent bug):** lines 2699–2706
compute `gnn_risk = risk_scores_map.get(selected_org)` then build `sim_gnn =
RetailSimulator("GNN-Adjusted", config, seed=42, bridge=bridge,
initial_skus=sku_states)` and call `result_gnn = sim_gnn.run(sim_days)` — **the
risk score is never passed into the run or the bridge.** Both the "Heuristic" and
"GNN-Adjusted" simulators share identical `config`, `seed=42`, and `sku_states`,
so the two runs are deterministically identical. The "GNN value proposition"
(2804–2816, extrapolated annual uplift) is therefore computed from ~zero delta /
noise — it does not demonstrate what it claims.

**Finding CC-E (Analytics PO query — latent bug):** line 2889 reads
`SELECT * FROM INTEGRATION_PO_RECOMMENDATIONS WHERE ORG_CD = :org`. The schema
(and `push_purchase_order`) uses **`INTEGRATION_PURCHASE_ORDERS`**. The query is
wrapped in `try/except → po_count, po_value = 0, 0`, so the "Purchase Orders" KPI
card silently shows **0 forever**.

**Finding CC-F (per-click engine construction):** the OASIS Processor (2365) and
Sim Lab build fresh `OrderEngine`/`SimulationOrderUtil` instances on each run
rather than reusing the cached `get_order_engine()` — repeated heavy loads.

**Finding CC-G (structure):** the entire 3,312-line file is straight-line script
with business logic inside `with tab:` blocks — untestable by construction
(the logic *engines* it calls are tested; the orchestration is not).

---

## 2. `st_gat_dashboard.py` — Market Intelligence (exhaustive)

### 2.1 Model loading (lines 70–121)
- **70–84** loads `stores_network.json` → `NetworkSimulator`, derives feature dim.
- **87–108** loads `st_gat_v2.pt`; **Finding MI-A (dimension band-aid):** a
  runtime patch (95–105) detects a 29-wide `temporal_lstm.weight_ih_l0` and
  **zero-pads it to 30** ("Salary Hit" feature) because checkpoint and model
  drifted. `load_state_dict(strict=False)` masks any other mismatch.
- **109–113** **Finding MI-B (silent random fallback):** on load failure or
  missing checkpoint it `st.warning`s and proceeds with a **randomly-initialized
  GNN** that still renders confident risk scores, demand fans, transfer scores —
  with no "this model is untrained" guard downstream.

### 2.2 Inference (lines 123–189)
What-if controls (rush hour, rain, budget) inject into the feature matrix
(indices 28 weather, 17 budget); `sim.step()` advances the agent sim;
`model(x_t, sim.edge_index)` runs; **line 188 patches `outputs['transfer']`** in
because the base GNN forward doesn't return it.

### 2.3 The 5 tabs + transfer hub
- **Live Network Map (228–393):** PyDeck nodes/arcs/traffic. **Finding MI-C
  (mocked attention):** line 252 `attn_matrix = torch.eye(len(stores))` — the GCN
  has no attention heads, so the "Attention Arcs" are an identity matrix; arcs
  effectively show nothing meaningful. ZINB demand fan (322–365) and Store-DNA
  radar (367–393) are real (derive params from model output).
- **Store Intelligence (396–451):** lazy-hydrates SKU simulators; top movers,
  revenue, category mix — depends on having run the sim.
- **Cluster Analysis (454–483):** PCA + KMeans(4) over the feature matrix — real.
- **Expansion Engine (486–672):** Huff gravity + Random-Forest site scoring via
  `sim.expansion_engine`; folium map with click/draw site capture, heatmap grid,
  competitor markers. The most sophisticated analytics in the platform.
- **Neural Ecosystem (675–813):** reads `neutral_network_export/{nodes,edges}.csv`
  → supplier fragility scatter, NetworkX SKU-affinity graph, whitespace/moat
  finder. Real, but file-dependent.
- **Transfer Hub (816–848):** GNN transfer scores → recommended transfers.
  **Finding MI-D (fake dispatch):** line 847 "Commit Transfers" does only
  `st.sidebar.success("Dispatched N transfers to ERP!")` — **no DB write**, no
  `push_transfer_request`. The action is theatrical.

**Finding MI-E (isolation):** the GNN, agent sim, expansion engine, and
affinity/whitespace analytics exist **only in this file** and are exposed via
no API — the richest intelligence is the least reusable. Hard-coded relative
paths (`stores_network.json`, `st_gat_v2.pt`) and `st.stop()` make it brittle
outside the project root.

---

## 3. `app.py` / `app_intel.py` / `shell.run_console` — the consoles (exhaustive)

Both entries are ~40 lines: `set_page_config` → `shell.run_console(registry, …)`.
`run_console` (shell.py) line-walk:
1. `theme.inject_theme` — SYS v2.9, injected once per session (guarded).
2. `ensure_seeded` — seeds default users only if `OASIS_SEED_PASSWORD` set & none
   exist.
3. `require_login` — one bcrypt gate (idle timeout, role allowlist, branded login).
4. `visible_pages(registry, role)` — pure role filter.
5. sidebar identity + radio nav + logout.
6. page-view telemetry (deduped on page change).
7. `safe_render(selected.render, ctx)` — exceptions → logged + calm panel.

The two registries (`shell.build_registry` 9 pages; `intel.build_intel_registry`
7 pages) are all native, each renderer reusing the tested `oasis/logic` engines.
**This generation is clean, role-gated, error-safe, telemetry-instrumented, and
testable (pure helpers extracted).** Its weaknesses are *omissions* relative to
the Command Center, not defects:

- **Finding SH-A:** Operations Ordering calls `calculate_order_quantity` **without
  a `gnn_risk_score`** (shell.py) — so safety stock is not risk-inflated as it is
  in the Command Center (ops_dashboard line ~2022 passes `gnn_risk_score`).
- **Finding SH-B:** `journey_state.value_recovered` is never populated by real
  capital-recovery data — the Home/Exec-ROI value meter shows 0 unless set
  manually.
- **Finding SH-C:** Intelligence Console reimplements a days-cover heuristic for
  velocity alerts instead of reusing the Command Center's `AlertMonitor`
  spike engine; and uses **no GNN** at all (Network Intel is inventory-based).

---

## 4. `oasis/main.py` — Flet desktop (exhaustive, 182 ln)

- **20–21** builds `OrderEngine("oasis/data")` (relative path — CWD-dependent)
  and `load_local_databases`.
- **24–49** auto-detects a local `.gguf` model; uses `LocalLLM` if `llama_cpp` is
  importable, else `RuleBasedLLM` (graceful, good).
- **51–95** file picker, mode dropdown (replenishment/initial_load), results table.
- **87–143** `process_order` → `engine.run_intelligent_analysis(file, output,
  mode, budget)` per file (the unified master workflow), renders top-50 rows,
  writes `processed_*` files next to the input.
- **Finding FL-A:** **no authentication** (desktop app, single-user assumption);
  acceptable for a local operator tool but inconsistent with the gated web tier.
- **Finding FL-B:** this is a **fourth ingestion path** —
  `run_intelligent_analysis` here, `parse_inventory_file→enrich→RuleBasedLLM` in
  the OASIS Processor tab, `ForensicOperationsIngestor` in the pitch app,
  `load_scorecard_recommendations` in the consoles. Four parsers, four code paths.

---

## 5. Consolidated findings

### 5.1 Confirmed latent bugs (line-cited)
| ID | Location | Defect | Effect |
|---|---|---|---|
| **L-1** | `run_stgat.bat:3` | bare `python` on a Streamlit script | launcher serves nothing |
| **CC-D** | `ops_dashboard.py:2699–2706` | GNN risk computed but never injected; both sims identical (seed 42) | "GNN value prop" is meaningless |
| **CC-E** | `ops_dashboard.py:2889` | queries `INTEGRATION_PO_RECOMMENDATIONS` (wrong table) | PO KPI silently 0 |
| **MI-A** | `st_gat_dashboard.py:95–105` | 29→30 weight zero-pad band-aid | fragile to next dim change |
| **MI-B** | `st_gat_dashboard.py:109–113` | silent random-init GNN fallback | confident output from untrained model |
| **MI-C** | `st_gat_dashboard.py:252` | attention arcs = `torch.eye` | "attention" viz shows nothing |
| **MI-D** | `st_gat_dashboard.py:847` | "Commit Transfers" only toasts | fake ERP dispatch |
| **CC-A** | `ops_dashboard.py:518–524` | guest fail-open fallback | latent (gated today) |

### 5.2 Theatrical / placeholder surfaces (not live)
- Executive ROI tab (CC-C): hard-coded neural stats, fixed ROI curves.
- Master Hub uplink (ops_dashboard 752–763): static "Last Pulse: 45s ago".
- ST-GAT Transfer Hub commit (MI-D): success toast, no write.

### 5.3 Underutilized assets (built, barely wired)
- **GNN risk + sigmoidal blend** — best risk logic, only in Command Center;
  absent from both consoles and unexposed by API.
- **`AlertMonitor`** (200%-spike) — Command Center only; consoles use a weaker
  heuristic.
- **`IntraDaySimulator`** — Command Center only.
- **`NotificationService`** — Command Center only.
- **`OasisScheduler`** — full control panel in Command Center Settings
  (start/stop, per-job toggle, run-now, cron) — **not in either console**.
- **Expansion / affinity / whitespace engines** — ST-GAT only.
- **FastAPI layer** (`/health`,`/metrics`, server/bridge) — consumed by no UI.
- **`journey_state.value_recovered`** — never fed real numbers.

### 5.4 Duplication (same capability, N implementations)
- **Ingestion ×4** (FL-B): Flet `run_intelligent_analysis`, Processor tab,
  pitch `ForensicOperationsIngestor`, console `greenfield_runner` loader.
- **Simulation ×4**: `IntraDaySimulator`, agent `NetworkSimulator`,
  `RetailSimulator` (sim lab), `black_swan_events` scenario templates.
- **Auth ×2**: inline (Command Center / ST-GAT none) vs `oasis/ui/auth`.
- **Theme ×N**: per-file inline CSS vs `oasis/ui/theme`.
- **Transfers ×3**, **velocity alerts ×3**, **exec-ROI ×2**, **supplier intel ×3**
  (Suppliers LATA, Command Center HHI, ST-GAT fragility).

### 5.5 Structural / runtime
- One SQLite file (WAL) shared by up to 6 processes + APIs + scheduler.
- Per-process `@st.cache_resource`; GNN/torch loaded separately by each app that
  uses it; Command Center builds engines per-click in two tabs (CC-F).
- 3,312-line + 848-line monoliths with logic inside view blocks (untestable).

---

## 6. Maturity / utilization scorecard

| System | Lines | Code quality | Feature depth | Utilization | Headline issue |
|---|---|---|---|---|---|
| Operations Console | ~40 + shell | High | Med | High | Omits GNN risk (SH-A) |
| Intelligence Console | ~40 + intel | High | Med | Med | No GNN; weaker alerts (SH-C) |
| Command Center | 3,312 | Low (struct) | **Very high** | Med | Theater + 2 latent bugs (CC-C/D/E) |
| Market Intel (ST-GAT) | 848 | Med | **Very high** | **Low** | Siloed; fragile model; fake dispatch |
| Flet desktop | 182 | Med | Low | Low | No auth; 4th ingestion path |
| Shared logic engines | — | High | High | High | Healthy (tested) |

---

## 7. Three conclusions

1. **The defects are concentrated in the legacy monoliths, the strengths in the
   shared logic.** The engines are unified and tested; the two big Streamlit
   scripts carry the theater (Exec ROI), the latent bugs (CC-D, CC-E), and the
   fragile model loading (MI-A/B). The new consoles are clean but omit the
   Command Center's best logic (GNN risk).

2. **The platform's most valuable intelligence is its least trustworthy and least
   reachable.** The GNN demonstrably mis-wired in the Sim Lab (CC-D), can run
   untrained without warning (MI-B), drives a mocked attention viz (MI-C) and a
   fake dispatch (MI-D), and is exposed by no shared service — yet it is branded
   the centerpiece ("Neural", "ST-GAT", "Market Pulse").

3. **It is over-forked and under-wired, not under-built.** Four ingestion paths,
   four simulators, three transfer views, two auth systems, a dormant API, an
   un-fed value thread, and a scheduler reachable from only one of four UIs. The
   capability exists many times over; the wiring and single-source-of-truth do not.

*(All findings above are observations from reading the code; no edits were made.
They are inputs for a later, deliberate decision — not a change list.)*
