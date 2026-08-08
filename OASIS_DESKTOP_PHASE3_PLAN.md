# O.A.S.I.S. Desktop — Phase 3 Plan

**Date:** 2026-08-01
**Baseline:** `099d375` (branch `pre-mosaic-backup`), VERSION `2.3.0`
**Verified against:** `dist/OASIS_v2.3.0.zip` — 167 files, 0.6 MB, built fresh for this analysis.
Working-tree claims are not used anywhere in Part 1 or Part 2. Every statement about
what a client has is a statement about what is inside that zip.

---

## Part 1 — Question 1: What are our current entrypoints?

### 1.1 The four layers

| Layer | Count | Client-facing? |
|---|---|---|
| `OASIS.bat` menu options | 9 | Yes — the intended single front door |
| `.bat` files at zip root | 12 | Yes — all double-clickable |
| `entrypoint.py --mode` values | 52 | Only via docs/`--help` |
| `--dashboard` values | 8 | Only via the two bats that pass one |

A client-facing surface of **9** sits on a developer surface of **60**.

### 1.2 The menu (`OASIS.bat`)

```
0  Desktop App      (RECOMMENDED — single native window)   --mode desktop
1  Home             browser launcher + first-run setup     --mode home
2  Operations       console :8500                          --mode shell
3  Intelligence     console :8510                          --mode intel
4  Command Center   console :8501       --mode dashboard --dashboard command
5  Market Intel     console :8505       --mode dashboard --dashboard stgat
6  Cloud Hub        server  :8700                          --mode hub
7  OASIS Service    supervisor                             --mode serve
8  License status                                          --mode license-status
```

### 1.3 Root `.bat` files actually in the zip (12)

`OASIS.bat` · `install.bat` · `serve.bat` · `register_service.bat` · `unregister_service.bat`
`run_oasis_home.bat` · `run_oasis_live.bat` · `run_oasis_intel_live.bat` ·
`run_command_center_live.bat` · `run_command_center_multi.bat` ·
`run_market_intelligence_tool.bat` · `run_mock_pos.bat` · `run_multi_pos.bat`

### Findings

**E-1 — Wave 1c's single front door was never finished.**
Wave 1c archived 31 legacy launchers, but **8 `run_*.bat` are still on
`_ROOT_WHITELIST`** in `oasis/logic/release_packager.py:91-94` and ship today. A client
unzipping OASIS sees twelve things to double-click and no signal that eleven of them
are not the way in.

**E-2 — The legacy bats override the store resolution Waves A/B built.**
Four of them hardcode `set "OASIS_DB_PATH=%~dp0oasis\data\rhapta_pos.db"` (or
`rhapta_multi_store.db`), bypassing `onboarding.resolved_db_path()`. Three additionally
call `--mode build-pos-db` on a first-run branch — `run_oasis_live.bat` is named like
"run OASIS" but rebuilds data.

*Not* a data-loss bug: the build is guarded on the DB not existing, and `load_catalog()`
raises before the reset. The real defect is **silent store-switching** — a client who
onboarded to their own store and then double-clicks `run_command_center_live.bat` is
looking at the Rhapta demo snapshot, which directly contradicts the provenance-badge
work shipped in Wave 1a.

**E-3 — Eight scripts the shipped `entrypoint.py` dispatches to are not in the zip.**
Verified absent: `shadow_dashboard.py`, `approval_dashboard.py`, `integrated_app.py`,
`pitch_app_v2.py`, `generate_showcase_scenario.py`, `shadow_monitor.py`,
`run_simulation_scenario.py`, `production_diagnostic.py`.

Consequence, by surface:

| Surface | Behaviour in the artifact |
|---|---|
| `--dashboard shadow / approval / integrated / pitch` | clean error, `exit(1)` |
| `--mode simulation` | clean error, `exit(1)` |
| `--mode shadow` | **uncaught `FileNotFoundError` traceback** (`entrypoint.py:483` `Popen`) |
| `--mode showcase` | silently skips its data regeneration, then runs |
| preflight diagnostics | silently skipped |

So **4 of 8 dashboards are dead in a client install.** None are reachable from
`OASIS.bat`, so a menu-driven client never hits them — but they are live in `--help`
and in every doc that lists modes. This is the same class of defect as `099d375`
(the whole Flet app missing from every zip): the whitelist and the dispatcher have no
test tying them together.

**E-4 — The desktop app ships correctly.** All 9 `oasis/desktop/**` files are present.

---

## Part 2 — Question 2: Client readiness

### 2.1 What a client can do today

- Install, onboard (sample / empty / connect-POS), set an admin password, sign in.
- Run four browser consoles, the Cloud Hub, and a supervised Windows service.
- Open the native desktop window and **read**: stock position, pending-PO count,
  supplier spread, engine posture, provenance, license status, backup, value report.

### 2.2 What they cannot

**R-1 — The recommended front door is a viewer, not an operator.**
`oasis/desktop/views/ops_view.py` and `intel_view.py` are read-only. Every action is an
honest `_not_migrated()` card pointing back to `OASIS.bat → 2 / 4`. And `/command` and
`/market` are literal `_placeholder_view()` calls that still tell the client
*"Phase 2 of the desktop migration will bring this view to life."* (`app.py:59`,
`:310-312`). Option 0 says RECOMMENDED; it cannot generate a purchase order.

**R-2 — There is no license enforcement in the desktop app. This is the blocker.**
`console_gate()`, `render_lock_screen()`, `render_upsell()`, `render_license_activation()`
all take a Streamlit `st` and are Streamlit-only (`oasis/logic/license_manager.py:420-554`).
`ops_dashboard.py`, `st_gat_dashboard.py`, and `oasis/ui/shell.py` call `console_gate`.
**Nothing in `oasis/desktop/` does** — it *displays* `license_posture()` in Home and
Settings and never acts on it.

Today: an expired trial locks all four browser consoles, while the native window opens
and shows store data. That is already a read-only leak past the selling lock shipped in
Wave 2a.

The moment Phase 3 puts Smart Ordering (module `ordering`) or Transfers/Allocation
(module `network`) into the desktop, it becomes a **paid-module bypass** — the
unlicensed native window doing the work the licensed console refuses to.

**R-3 — Auth is present and fails closed.** `_needs_auth()` (`app.py:271-297`) returns
`True` on any unreadable store. This one is right.

**R-4 — The surface gap is not "two views to write".**

| Console | Tabs | Lines | Desktop today |
|---|---|---|---|
| Command Center (`ops_dashboard.py`) | 11 role-gated | 3,433 | placeholder |
| Market Intel (`st_gat_dashboard.py`) | 5 | 854 | placeholder |
| Desktop Operations | 4 (read-only cards) | 215 | — |

**R-5 — Market Intel's stack is browser-shaped.** It imports `torch`, `pydeck`,
`folium` + `streamlit_folium` (incl. `HeatMap`, `Draw`), and `plotly`. Pinned
`flet==0.28.3` has no map widget. A 1:1 native port of the Map / Expansion / Neural
Ecosystem tabs is not achievable in Phase 3 at any sensible cost.

**R-7 — The desktop app could not open on the flet version we ship.**
*(Found while implementing P3.0, 2026-08-01.)* `app.py` built its `NavigationRail`
with `icon_content=` / `selected_icon_content=`. Those were **removed in flet 0.28.0**;
`requirements.txt` pins `flet==0.28.3`. Verified against the real 0.28.3 wheel:

```
NavigationRailDestination.__init__() got an unexpected keyword argument 'icon_content'
```

So a client installing per `requirements.txt` got a `TypeError` before the first frame.
Nothing caught it because `.oasis_venv` still holds **flet 0.25.2**, where the old names
merely emit a DeprecationWarning — and no test constructed the nav rail. This is the
`f7c0274` defect inverted: there the pin was wrong and the code right; here the pin is
right and the code was wrong. Fixed in P3.0 (`icon=` / `selected_icon=`, valid on both),
and the boot-path test now constructs the rail so it cannot regress silently.

**The standing risk remains:** the dev venv is not the shipping environment.
`tests/test_dependency_pins.py::test_installed_flet_satisfies_the_pin` fails today for
exactly this reason and should be treated as a live warning, not noise.

**R-6 — The test baseline is green and the harness is the right one.**
`tests/test_desktop_views.py` — **8 passed in 53s**. It builds real Flet control trees
without a display, which is exactly what caught the Phase-1 assumed-API defects. Phase 3
extends this file; it does not invent a new pattern.

---

## Part 3 — What Phase 3 therefore is

> **Phase 3 = "the desktop app can run a day."** Not console parity.

Three principles:

1. **Actions before acreage.** A client who can generate → review → approve one PO in
   the native window is better served than one who can read sixteen tabs of numbers.
2. **The gate lands before the action.** No paid capability enters the desktop before
   licensing is enforceable there.
3. **Where native is the wrong medium, hand off explicitly.** No half-drawn maps, and
   no more placeholders that promise a future phase.

---

## Part 4 — Workstreams

### P3.0 — License enforcement in the desktop *(blocker — do first)*

- Split decision from rendering in `license_manager`: add a pure
  `gate_status(module) -> dict`; leave `console_gate(st, module)` as the Streamlit
  adapter over it, so both front doors share one decision.
- Flet lock screen preserving the audit E1/E3 invariant — **all three doors**: activate
  a key, export my data, see what a license buys. Their records are never held hostage
  in the native window either.
- Per-route module gating in `_render_view`: `ordering` → Command actions,
  `network` → transfers/allocation.
- Flet upsell stub + in-window key activation (paste, validate, unlock in place).

**Accept:** with an expired trial, `--mode desktop` shows the lock screen and **no store
data**; the DB export still works; pasting a valid key unlocks without a restart.
Control-tree tests for locked / evaluation / licensed.

### P3.1 — Command Center, natively, decision-first

Not 11 tabs. Port in this order and stop when a day is runnable:

1. **Smart Ordering** — generate → MOQ/budget gate → push to approvals `[ordering]`
2. **Pending Approvals** — approve/reject, same tables `[core]`
3. **End-of-Day Stock** `[core]`
4. **Executive ROI Overview** `[core]`
5. **Transfer Intelligence** `[network]`

Deferred to Phase 4: Live Sales Feed, OASIS Processor, Allocation Engine, Simulation
Lab, Analytics, Supplier Intelligence.

Every read and every write goes through `oasis/desktop/data.py` (extended). No parallel
queries, no invented API — that module exists precisely to encode the Phase-1 lesson.

**Accept:** control-tree test per tab, plus one end-to-end test that generates a PO
against a temp store and asserts the row lands in the **same table** the Streamlit
console writes to.

### P3.2 — Market Intelligence: honest handoff, not a half-port

- **Native:** Store Intelligence (top movers, revenue drivers, sales by category) and
  the cluster summary — tables and bar charts, which Flet does well.
- **Not native:** the ST-GAT map, the expansion grid, the neural ecosystem graph.
  `/market` gets a real launcher card that starts the Streamlit console, reports its
  port, and says plainly why (map and graph layers render in the browser).
- **Rule:** `/market` ends Phase 3 with zero `_placeholder_view`. Either it works, or it
  launches the thing that works.

### P3.3 — Entrypoint consolidation *(closes E-1, E-2, E-3)*

- Remove the 8 legacy `run_*.bat` from `_ROOT_WHITELIST`. Keep `OASIS.bat`,
  `install.bat`, `serve.bat`, `register_service.bat`, `unregister_service.bat`.
- Move the data-build steps they performed into an explicit `OASIS.bat` "Demo / sample
  data" submenu that uses `resolved_db_path()` — never a hardcoded `OASIS_DB_PATH`.
- Resolve the 8 missing scripts: strip the dev-only modes from the client build's
  `--mode choices`, or ship the scripts. Recommend stripping.
- Fix the uncaught `FileNotFoundError` in `run_shadow()` regardless.

**Accept:** a new assertion in `tests/test_release_zip.py` — *every script the shipped
`entrypoint.py` can dispatch to exists in the zip*. That test would have caught `099d375`
and catches E-3 permanently.

### P3.4 — Make the front door's claim true

Once P3.0–P3.2 land, reorder `OASIS.bat` so the browser consoles sit under an
"Advanced — browser consoles" group and option 0 is the default path, not a suggestion.

---

## Part 5 — Sequencing

```
P3.0  license gate            ── blocker, nothing paid ships before it
  └─ P3.1 (1) Smart Ordering
     └─ P3.1 (2) Approvals    ── "a day is runnable" milestone
        └─ P3.3 entrypoint consolidation   ── land early; it is small and de-risks the zip
           └─ P3.1 (3,4,5)
              └─ P3.2 market handoff
                 └─ P3.4 menu reorder
```

---

## Part 6 — Risks

- **`flet 0.28.3` API drift.** Every view is proven by building its control tree in
  `tests/test_desktop_views.py`. "It launched" is not evidence — that lesson is already
  in the memory and in the Phase-1 commit message.
- **`ops_dashboard.py` is 3,433 lines of Streamlit with logic inline.** Porting is
  *extraction*, not translation. Budget accordingly. **When logic is extracted, switch
  the Streamlit console to the extracted function in the same commit** — otherwise the
  two front doors drift and the desktop becomes a second source of truth.
- **Scope creep to parity.** 11 + 5 tabs is Phase 3 + 4 + 5. Phase 3 buys a runnable
  day and nothing more.

---

## Open decisions

1. Confirm Phase 3 = **"runnable day"** over console parity.
2. Drop the 8 legacy bats **now** (with P3.3 early) or after P3.1 lands?
3. The 8 missing scripts: **strip the dev modes** from the client build, or ship them?

## Still open from the previous checkpoint

Git hygiene tier 1 + 2 (`git gc` on 3.31 GiB of loose objects with zero packs; untrack
`mock_pos_erp_showcase.db`, `mande_purge_report.json`, `test_shadow_report.docx`) was
recommended and never run. Independent of Phase 3.
