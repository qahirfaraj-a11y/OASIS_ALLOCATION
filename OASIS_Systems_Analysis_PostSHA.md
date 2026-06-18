# O.A.S.I.S. — Systems Analysis (post-SH-A)

> **Scope:** Line-level interrogation of the three console launchers the user
> named — `run_oasis.bat` (Operations Console), `run_oasis_intel.bat`
> (Intelligence Console), `run_command_center.bat` (Command Center) — and the
> logic they drive, *with the SH-A GNN-service work (S1/S2/S3/S5) applied*.
> Read-only: **no code changes.** Generated 2026-06-18.
>
> Companion to `OASIS_Systems_Exhaustive_Analysis.md` (the pre-SH-A pass). This
> doc lists what is **still open** after the recent fixes and the **new** gaps
> the consolidation exposed; it does not re-list resolved findings (MI-A,
> MI-B/C/D, CC-A/C/D/E, L-1, SH-B are closed).

---

## 1. Launch topology (verified)

| Launcher | entrypoint | Surface | Port |
|---|---|---|---|
| `run_oasis.bat` | `entrypoint.py --mode shell` → `app.py` | Operations Console — `shell.run_console(build_registry())` (transactional: ordering, transfers, suppliers, allocation, shadow, analytics, settings) | 8500 |
| `run_oasis_intel.bat` | `entrypoint.py --mode intel` → `app_intel.py` | Intelligence Console — `shell.run_console(build_intel_registry())` (monitoring: pulse, velocity, stock review, live sales, network intel, exec ROI, sim lab) | 8510 |
| `run_command_center.bat` | `entrypoint.py --mode dashboard --dashboard command` → `ops_dashboard.py` | Legacy Command Center monolith (3.3k lines; deep GNN/chaos/smart-ordering) | 8501 |

`app.py` and `app_intel.py` are thin (~37 lines each): both call the shared
`shell.run_console`, so the two new consoles cannot drift on theme/auth/seed/
telemetry. Good. The Command Center remains a standalone monolith.

**Post-SH-A consolidation now in place:** one guarded GNN loader
(`gnn_service._load_model`) shared by the Command Center
([ops_dashboard.py:1352](ops_dashboard.py)) and ST-GAT
([st_gat_dashboard.py:70](st_gat_dashboard.py)); one blend
(`risk_from_ratios`+`blend_risk`); the Intelligence Console now reads
`store_risk` (S5). The Operations Console does **not** yet read it — see G-1.

---

## 2. Open GAPS (logic that should exist and doesn't)

### G-1 · Operations Console ordering ignores store risk — **HIGH** *(this is SH-A S4, held)*
`shell.render_ordering` runs the PO pipeline at
[shell.py:299-300](oasis/ui/shell.py) —
`sim.calculate_order_quantity(enriched, use_real_date=True)` — with **no
`gnn_risk_score`**. The parameter exists and is load-bearing:
[simulation_bridge.py:102](oasis/logic/simulation_bridge.py) accepts it and
[:215-217](oasis/logic/simulation_bridge.py) inflates safety stock 1.0→1.3 for
`risk > 0.5`. The Command Center threads it; the **daily-driver console does
not**, so its POs are *less risk-aware than the legacy tool it replaces*. The
service to supply it (`gnn_service.store_risk`) now exists. The docstring at
[shell.py:248-249](oasis/ui/shell.py) still admits "GNN risk overlay remain in
the legacy command center." **Fix = S4 (gated on your sign-off + blend ratio).**

### G-2 · Intelligence Sim Lab what-if ignores risk — **MEDIUM**
`intel.render_sim_lab` computes baseline vs scenario order qty at
[intel.py:482](oasis/ui/intel.py) and [:490](oasis/ui/intel.py) — both
`calculate_order_quantity(..., use_real_date=True)` **without `gnn_risk_score`**.
The Command Center's Sim Lab threads GNN risk (the CC-D fix), so the same what-if
gives a *less* risk-aware answer in the Intelligence Console. One-line wiring
once S4 lands (reuse `store_risk` for the selected store).

### G-3 · Journey advance gate excludes the journey roles — **MEDIUM**
`render_home` gates phase advancement with **legacy roles only**:
`can_operate = role in ("ops_admin", "regional_manager")`
([shell.py:150](oasis/ui/shell.py)). But page *visibility* uses the `_OPERATOR`
group that includes `ilink_operator`, and the journey model adds `executive` —
the very roles meant to confirm a phase gate. Result: an `ilink_operator` or
`executive` sees the Home journey rail but the **advance gate never renders for
them**. The role-model migration is incomplete here. Fix: gate on a group
(e.g. `_OPERATOR + ("executive",)`) not a hardcoded legacy pair.

---

## 3. UNDERUTILIZATIONS (capability built but not wired through)

### U-1 · `log_ui_action` is dead — **MEDIUM**
`telemetry.log_ui_action` has **zero call sites** (grep across `oasis/`, `app*.py`,
`ops_dashboard.py`). Only `log_page_view` is used ([shell.py:127](oasis/ui/shell.py)).
So the platform's high-value, irreversible actions are **not** captured as UI
actions: Push to Approvals ([shell.py:337-340](oasis/ui/shell.py)), Queue
Transfers ([shell.py:457-472](oasis/ui/shell.py)), Run Allocation
([shell.py:193](oasis/ui/shell.py)), and the human-confirmed journey advance
([shell.py:161-163](oasis/ui/shell.py)). The adoption panel
([shell.py:702-715](oasis/ui/shell.py)) therefore shows *navigation* but not
*action* — and the trust story ("every decision is logged") is weaker than it
should be. Wire `log_ui_action` into those four buttons.

### U-2 · GNN demand head computed, then discarded — **MEDIUM**
The trained GNN emits three heads (demand distribution, risk, pairwise transfer
affinity). Post-SH-A the **risk** head feeds the consoles and the **transfer**
head feeds the Command Center transfer tab
([ops_dashboard.py:1433,1457](ops_dashboard.py)). The **demand** head is
consumed *only* in ST-GAT visualization
([st_gat_dashboard.py:226,316,318](st_gat_dashboard.py)) — never in ordering,
allocation, or transfers. The model's demand forecast is, in effect,
decorative. Opportunity: expose `gnn_service.demand_matrix()` (the deferred
phase-2 accessor in the SH-A plan) and let the engine cross-check its
heuristic ADS against the GNN's department demand — or at minimum flag stores
where the two diverge sharply.

### U-3 · `store_risk` is read-only everywhere — **(folds into G-1)**
S2/S3/S5 made `store_risk` a *read* across the Command Center and Intelligence
Console, but nothing **acts** on it yet. Its entire reason for existing
(risk-aware POs) is unrealized until S4. Until then it is an honest dashboard
number with no operational consequence.

---

## 4. ROBUSTNESS / correctness notes

### R-1 · `store_risk` org-code join is convention-coupled (silent-fail) — **LOW** *(new, from SH-A)*
`gnn_service.store_risk` maps the GNN graph to stock by
`store_id.replace("CFP-", "ORG")`. Verified working on the mock DB (graph
`CFP-001…` ↔ `ORG001…` ↔ DB `ORG_CD`). **But** on a real deployment whose
`ORG_CD` format differs, the join misses for every store and `store_risk`
silently returns **inventory-only** risk for all of them — graceful degradation
*masks* the misconfiguration, so no one notices the GNN stopped contributing.
Mitigation: when `model_status()=="trained"` but the org-code intersection is
empty, log a one-time warning ("GNN graph store_ids do not match DB ORG_CDs —
risk is inventory-only").

### R-2 · `render_ordering` refetches the whole network per single-store order — **MEDIUM (perf)**
[shell.py:303](oasis/ui/shell.py) builds
`{o: adapter.fetch_enriched_products(o) for o in org_ids}` on **every**
regenerate, to feed `cts.optimize_network`. That is an O(N-stores) DB sweep to
generate **one** store's PO, uncached — while the Intelligence Console already
caches network stock once per session (`_intel_netstock`,
[intel.py:131-141](oasis/ui/intel.py)). Reuse the same session cache here.

### R-3 · Engine flags are display-only in Settings — **LOW**
`render_settings` lists engine enable/disable flags read-only
([shell.py:686-700](oasis/ui/shell.py)); toggling still requires hand-editing
`oasis_engines_config.json`. Acceptable for now, but the console can't actually
administer the engines it advertises.

---

## 5. What's healthy (so the picture is balanced)

- **Two consoles share one runner** — theme/auth/seed/telemetry/safe_render are
  single-sourced; the consoles structurally cannot diverge.
- **GNN loader + blend are now single-sourced** (SH-A S1-S3); the MI-A dimension
  guard and the untrained banner live in exactly one place.
- **Transfer & allocation logic is already de-duplicated** into CTS and
  `greenfield_runner` (prior work) and reused natively by the shell.
- **Graceful degradation is real** — no console imports torch eagerly; the
  inventory-only path is the floor.

---

## 6. Prioritized backlog (suggested order)

| # | Finding | Severity | Effort | Note |
|---|---|---|---|---|
| 1 | **G-1 / S4** Operations ordering consumes `store_risk` | HIGH | S | The held SH-A step; needs blend-ratio decision |
| 2 | **G-2** Intelligence Sim Lab threads risk | MED | XS | Trivial once S4 lands |
| 3 | **U-1** Wire `log_ui_action` into the 4 action buttons | MED | S | Closes the audit/adoption gap |
| 4 | **G-3** Journey advance gate spans journey roles | MED | XS | One-line role-group fix |
| 5 | **R-2** Cache network stock in `render_ordering` | MED | S | Perf; reuse intel's pattern |
| 6 | **R-1** Warn on empty org-code join in `store_risk` | LOW | XS | Prevents silent GNN-off in prod |
| 7 | **U-2** Surface/consume the GNN demand head | MED | M | Bigger; product decision |
| 8 | **R-3** Editable engine flags in Settings | LOW | S | Admin convenience |

**Headline:** the SH-A consolidation did its job — risk intelligence is now
single-sourced and visible in both consoles. The biggest remaining gap is the
one we deliberately deferred (**G-1/S4**): the console that *acts* still doesn't
*use* the risk the others now display. After S4, the cluster G-2 / U-1 / G-3 are
all small, high-trust finishers.
