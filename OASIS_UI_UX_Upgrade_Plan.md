# O.A.S.I.S. UI / UX & Client-Facing Workflow — Upgrade Plan

> **Status:** Review document — **no code changes yet.** For sign-off before
> implementation. Companion to `OASIS_Logic_Review_and_Hardening_Plan.md`
> (logic) and `OASIS_Onsite_Hardening_and_Tenancy.md` (deployment).
> Deployment model assumed throughout: **onsite, cost-sensitive** (one install
> per retailer/site). Generated 2026-06-13.

---

## 0. Thesis

We did to the *logic* what now needs doing to the *UI*: the engine was a
1,156-line monolith → it's now decomposed, unified behind `greenfield_runner` /
`ConsolidatedTransferService` / `GreenfieldPipeline`, and guarded by tests. The
client-facing layer is still in the "before" state — ~10 overlapping apps, each
re-implementing layout, auth, and styling. This plan consolidates the UI behind
**one shell + one shared component/theme layer**, with the FastAPI layer as the
durable contract so a richer client can be added later without touching logic.

---

## 1. Current state (evidence)

### 1.1 Entry points — there is no single front door
**19** `run_*.bat` launchers, each starting a separate process/port:

```
run_command_center  run_allocation_app   run_approval_dashboard  run_shadow_dashboard
run_stgat           run_integrated_app   run_pitch / run_pitch_app
run_kuber / run_kuber_terminal           run_app / run_app_online (Flet)
run_backend  run_frontend  run_hybrid_fleet  run_pipeline_daemon
run_market_intelligence_tool  run_showcase  run_simulation_menu
```

### 1.2 UI surfaces (~10 web apps + a 4-page app + 2 native apps)

| Surface | File | Lines | Login? | Notes |
|---|---|---:|:--:|---|
| Command Center | `ops_dashboard.py` | 3,312 | **Yes** | 10+ role-gated tabs; the only gated app |
| Allocation | `allocation_app.py` | 234 | No | Greenfield basket |
| Approval | `approval_dashboard.py` | 463 | **No** | **Approves PO spend — ungated** |
| Shadow audit | `shadow_dashboard.py` | 252 | No | Forensic audit |
| ST-GAT network | `st_gat_dashboard.py` | 844 | No | Network/transfer GNN |
| Integrated lifecycle | `integrated_app.py` | 611 | No | Sim + Mosaic handoff |
| Pitch | `pitch_app_v2.py` | 469 | No | Sales/demo |
| Kuber terminal | `kuber_terminal.py` | 319 | No | Exchange terminal |
| Phase pages | `pages/1..4_*.py` | — | No | Separate 4-page Streamlit app |
| Native desktop | `oasis/main.py`, `main_online.py` | 182/188 | No | Flet |

**Verified facts driving this plan:**
- **Auth in 1 of 8 web apps.** `authenticate()` is called only in
  `ops_dashboard.py`. The approval dashboard (authorizes spend) is ungated.
- **Shared component layer is missing.** The `Dockerfile` `COPY ui_components.py`
  references a file that **does not exist** in the tree — the image build is
  broken or the shared layer was deleted and never replaced.
- **Allocation UI is duplicated 4×** — `allocation_app.py`, the Command Center
  "Allocation Engine" tab, `integrated_app.py`, and `pages/3_Phase_3_Allocation.py`.
  (We already unified the *logic* behind `greenfield_runner.py`; the UI is still
  four copies.)
- **Styling is inline string-injected CSS** — `ops_dashboard.py` carries ~100
  lines of glassmorphism CSS in a `st.markdown(<style>…, unsafe_allow_html=True)`
  block, re-declared (divergently) in other apps. No design tokens.

---

## 2. Gap analysis (grouped, with severity)

### G1 — Architecture & maintainability  *(severity: high — root cause)*
- Ten entry points each re-implement layout/auth/data-load/styling. One UX change
  is an N-file edit.
- No component library (`oasis/ui/` does not exist; `ui_components.py` missing).
- Inline CSS string blocks; no theme tokens; `unsafe_allow_html` throughout.
- Streamlit's full-rerun model fights a stateful operational tool (the
  render-side-effect bug fixed in Smart Ordering was symptomatic).

### G2 — Security & access  *(severity: high)*
- 7 of 8 web apps have **no login**; the PO-approval app is ungated.
- `@st.cache_resource` connectors/engines are **process-global** (shared across
  browser sessions). Combined with the auth gap there is **no per-user data
  isolation boundary** outside the command center.
- No session timeout / consistent logout outside `ops_dashboard.py`.

### G3 — Navigation & information architecture  *(severity: medium)*
- No role-based home; non-command-center apps don't role-gate at all.
- No breadcrumbs, global search, or cross-app linking (e.g. "approve PO" →
  "why was it recommended" means closing one app and opening another).
- 10+ flat tabs with no grouping in the command center.

### G4 — Interaction, feedback & error UX  *(severity: medium)*
- **Raw Python tracebacks shown to clients** (`st.code(traceback.format_exc())`
  in multiple apps).
- No empty/first-run states; fresh installs show warnings, not guidance.
- Onboarding = "contact your administrator."
- Financial/destructive actions (approve PO, queue transfers, push orders) use
  plain buttons + `time.sleep(); st.rerun()` — no confirmation, occasional full
  page reloads.

### G5 — Visual design & accessibility  *(severity: medium)*
- **Status encoded by colour/emoji only** (🔴/🟠/🟢) — inaccessible to
  color-blind and screen-reader users. (The fix: icon + label + colour, now
  the standard in `oasis/ui`.)
- Per-app **divergent inline CSS** and neon-on-dark glare; no shared tokens.
  (Secondary-text contrast on the dark bg is actually acceptable; the real
  issues are inconsistency and colour-only meaning.)
- Custom HTML cards bypass Streamlit's built-in a11y.

### G6 — Mobile & field use  *(severity: medium)*
- A **mobile API exists** (`oasis/api/server.py`, port 8550) but **no mobile
  client** exists. The field workflow (clerk receives transfer / checks
  stockout) has a backend and no front-end.

### G7 — Observability of the client experience  *(severity: low–medium)*
- Audit log captures business actions, not **UI/usage events** (page views,
  action success/failure). Cannot grade which screens are used or where clients
  hit friction — "gradeable" requires this.

---

## 3. Target architecture — one upgradeable UI engine

```
run_oasis.bat ──► app.py  (single Streamlit shell)
                   │
                   ├─ oasis/ui/auth.py      require_login()  — one gate, all pages
                   ├─ oasis/ui/theme.py      design tokens, injected once (WCAG)
                   ├─ oasis/ui/components.py  metric_card / alert_card / kpi_row /
                   │                          data_table / confirm_button /
                   │                          error_panel / empty_state
                   ├─ oasis/ui/telemetry.py  page_view / action events → audit log
                   └─ pages/  (role-gated registry)
                        Allocation · Ordering · Transfers · Approvals ·
                        Audit · Analytics · Settings
                              │
                              └─ all call oasis/logic/* and oasis/api/* (the seam)
```

- **One front door**: `app.py` authenticates once, resolves role, exposes a
  page registry. Streamlit-native multipage (the existing but unused `pages/`)
  becomes the shell.
- **One component + theme layer** (`oasis/ui/`): the missing layer the Dockerfile
  already expects. Restyling the whole product becomes a single PR.
- **API as the durable contract**: pages call the same `oasis/api` endpoints a
  future web/mobile client would — so a richer front-end is additive, not a
  rewrite.

### Explicit decision: stay on Streamlit (for now)
For an **onsite, low-cost** target a React/Next rewrite is **not** justified.
Streamlit consolidation is high-ROI / low-risk. Keep `server.py` / `bridge.py`
(with `/health`, `/metrics`, auth) as the seam so a richer client *can* be built
later if a hosted or field-mobile need appears (Phase U6, deferred).

---

## 4. Phased implementation

Each phase is independently shippable. Effort is rough dev-days for one engineer.

> **Journey alignment (see `OASIS_Customer_Journey.md`, decisions confirmed
> 2026-06-13):** the IA and roles below are driven by the customer journey.
> Role model: `ilink_operator`, `executive`, `finance`, `approval_manager`,
> `branch_manager`. The pitch/diagnosis is an **operator-only `DIAGNOSE` mode**
> of this same shell (prospect receives outputs, never logs in). Mode/Phase
> advancement is **human-confirmed** (metrics prompt, human approves). Mobile is
> **deferred** (U6). Theme tokens come from the **SYS v2.9 Visual System Guide**
> (teal-turquoise / deep slate / platform white; Montserrat + Space Mono).

### U0 — Journey Spine  *(≈2–3 d · low risk — do with U1)*
The smallest slice that makes the trust ladder visible; everything else hangs off it.
- A persistent **Mode + Phase + value-recovered badge** in the shell header
  (`SHADOW/ACTIVE/AUTONOMOUS · Phase N · KES X recovered`).
- A **Home / Journey** screen: current stage, the next decision gate, cumulative
  value, and (for operator/exec) the human-confirmed "advance" action.
- Journey component primitives that live in the U1 library:
  `mode_phase_badge()`, `value_recovered_meter()`, `decision_gate_card()`,
  `journey_rail()` (the 7-stage ladder).
**Why first:** it is the product's emotional spine and the orienting chrome for
every persona; cheap, high-impact, and depended-on by the shell (U3).
**Test:** badge renders current mode/phase from state; gate card requires explicit
confirm and writes an audit event.

### U1 — Shared component + theme library  *(≈3–5 d · low risk)*
**Build** `oasis/ui/`:
- `theme.py` — **SYS v2.9 tokens** (teal-turquoise accent, deep-slate bg,
  platform-white surfaces; Montserrat display/UI, Space Mono for data/money);
  WCAG-AA contrast; status = **icon + label + color** (not color alone); single
  injected stylesheet (replaces the per-app glassmorphism blocks).
- `components.py` — `metric_card`, `alert_card`, `transfer_card`, `kpi_row`,
  `data_table`, `confirm_button`, `error_panel`, `empty_state`, plus the U0
  journey primitives and `supplier_status_chip()` (Green/Yellow/Red as
  icon+label+colour).
- Fix the broken `Dockerfile` reference (point at `oasis/ui/` or restore the
  copied path).
**Why first:** unblocks every later phase; converts "restyle = N edits" into
"restyle = 1 edit." No behavior change.
**Test:** import/smoke tests for each component; a contrast unit check on tokens.

### U2 — Unified auth & session middleware  *(≈2–3 d · low–med risk)*
- `oasis/ui/auth.py` `require_login()` wrapper using the existing hardened bcrypt
  auth; applied to the shell so **every** page (esp. Approvals) is gated.
- Consistent role→page visibility, session timeout, logout.
- Pair with per-session data scoping for role-filtered views.
**Why:** closes the 7-ungated-apps hole, including PO approval. High security
value, small surface.
**Test:** ungated-page regression (every page redirects to login when
unauthenticated); role-visibility matrix.

### U3 — Single shell & app consolidation  *(≈5–8 d · medium risk)*
- `app.py` shell + role-based `pages/` registry.
- Collapse the ~10 apps into pages; **19 `.bat` → 1** `run_oasis.bat` (plus the
  headless `--mode engine/api/bridge` we already have).
- Collapse the **4× allocation UIs into one page** calling `greenfield_runner`
  (finishes the UI half of the logic consolidation already done).
**Why:** the single front door — the biggest UX win.
**Risk:** medium — touches every app's entry; mitigate by migrating one surface
at a time behind the shell, keeping old `.bat`s until parity is confirmed.
**Test:** each migrated page renders under the shell with correct role gating;
manual parity pass against the legacy app before retiring its `.bat`.

### U4 — Error, onboarding & empty-state UX  *(≈3–4 d · low risk)*
- `safe_render()` decorator: exceptions → friendly `error_panel()` for the user,
  full detail to the structured logger (`logging_config.py`). No tracebacks
  on screen.
- First-run/empty states with a guided "load scorecard / connect data" path;
  showcase mode becomes an explicit "try with sample data."
- Confirmation + optimistic feedback on financial/destructive actions.
**Test:** forced-exception renders the panel (not a traceback); empty-data
states render guidance.

### U5 — UI usage telemetry  *(≈2 d · low risk)*
- `oasis/ui/telemetry.py` — page-view + action (attempt/success/fail) events into
  the existing audit log (new entity type `UI`).
- A small "Adoption" panel in the Analytics page.
**Why:** makes the engine *gradeable* — measure adoption and friction per screen.

### U6 — (Deferred) richer web / mobile client on the API seam  *(high effort)*
Only if a hosted multi-retailer or field-mobile need appears. Build a React/Next
or Flutter client against `oasis/api`. No logic changes. Out of scope for onsite.

---

## 5. Sequence, dependencies & decisions

**Recommended order:** U1 → U2 → U4 → U3 → U5. (U4 before U3 so the shell inherits
the error/empty-state primitives; U2 early because it's the security fix.)

**Dependencies:** U2/U3/U4 all consume U1's components. U5 is independent.

**Decisions needed before U3:**
1. **Native Flet apps** (`oasis/main.py`, `main_online.py`) — fold into the shell,
   keep as a separate desktop track, or retire? (They overlap the web allocation
   flow.)
2. **Pitch / Kuber / Integrated** — are these client-facing or internal
   sales/demo tools? If internal, they stay separate and *out* of the client
   shell (changes the consolidation scope).
3. **Mobile (U6)** — is there a real field-mobile need, or does the mobile API
   stay dormant? Determines whether the API seam must be hardened now.

**Net code for U1–U5:** a new `oasis/ui/` package, one `app.py`, a slimmed
`pages/` registry, and deletions of duplicated app bodies — a net *reduction* in
UI line count alongside a large maintainability gain.

---

## 6. What this is NOT
- Not a framework rewrite (Streamlit stays).
- Not a logic change (pages call existing `oasis/logic` / `oasis/api`).
- Not multi-tenancy/SSO work (covered, and deferred, in the onsite doc).

The guardrail mirrors the logic work: consolidate behind shared modules, gate
with tests, keep the API as the stable contract. Ship U1–U2 first for immediate
maintainability + security wins; U3 is the headline UX change once components and
auth exist to build it on.
