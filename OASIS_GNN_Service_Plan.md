# O.A.S.I.S. — GNN Shared-Service Extraction Plan (SH-A)

> **Status:** Review document — **no code changes yet.** For sign-off before
> implementation. Addresses finding SH-A (Operations Console ordering omits GNN
> risk) and G-INT-1 (Intelligence Console doesn't use the GNN) from the systems
> analyses, by extracting the GNN into a single shared service.
> Generated 2026-06-17.

---

## 1. Why

The trained store-risk GNN is the platform's headline intelligence, yet today it
is **loaded and used inconsistently in two monoliths and absent from the new
consoles**:

| Surface | GNN today |
|---|---|
| `ops_dashboard.py` | `get_gnn_resources()` (L1352) loads the model; `get_all_store_risks()` (L280) blends GNN risk with an inventory heuristic via a sigmoidal brake (`gnn_risk_blend_ratio`, default 0.5). Smart Ordering (L1911), chaos scenarios, and the Sim Lab consume it. |
| `st_gat_dashboard.py` | a **separate** `load_resources()` (L70) loads the same model with a **different** path — the runtime 29→30 dimension patch and the silent random-init fallback (MI-A/MI-B). |
| Operations Console (`app.py`/shell) | **none** — ordering calls `calculate_order_quantity` with no `gnn_risk_score` (SH-A). |
| Intelligence Console (`app_intel.py`) | **none** — Network Intel is inventory-only (G-INT-1). |

Consequences: torch + the checkpoint are loaded **twice** when both monoliths
run; the two loaders can drift (they already differ on the dim patch); the
console branded "Intelligence" can't see the model; and the Operations Console's
PO math is less risk-aware than the legacy one.

## 2. Goal

One **`oasis/logic/gnn_service.py`** that is the single source of store-risk
intelligence, with **graceful degradation**: it returns usable risk whether or
not torch and the checkpoint are present.

```
store_risk(stock_by_org, blend_ratio=0.5) -> {org_cd: risk 0..1}
    = sigmoidal_blend( gnn_risk (if model available else 0), inventory_risk )
```

- If torch + a **trained** checkpoint load → GNN-enhanced blended risk.
- If torch missing, checkpoint missing, or model **untrained** → inventory-only
  risk (no hard torch dependency, no silently-confident untrained output).
- Exposes a `model_status()` → `trained | untrained | unavailable` so every
  surface can show the right banner (fixes MI-B honestly, platform-wide).

This makes GNN risk an **enhancement, not a dependency** — so the Operations and
Intelligence consoles (which don't import torch today) can consume risk safely.

## 3. Target module surface (`oasis/logic/gnn_service.py`)

| Function | Purpose | Torch? | Testable |
|---|---|---|---|
| `inventory_risk(products) -> float` | stockout/critical ratio heuristic (extracted from `get_all_store_risks` L324–341) | No | **Pure ✓** |
| `blend_risk(gnn, inv, ratio) -> float` | the sigmoidal-brake blend (L378–387) | No | **Pure ✓** |
| `model_status() -> str` | trained / untrained / unavailable | lazy | ✓ |
| `_load_model()` (cached) | one loader: consolidates both loaders incl. the 29→30 patch + trained guard | lazy import | guarded |
| `store_risk(stock_by_org, ratio=0.5) -> dict` | per-store blended risk; GNN if available else inventory-only | lazy | ✓ (inv path without torch) |
| `demand_matrix()/transfer_scores()` (phase 2, for st_gat) | expose raw model outputs the ST-GAT views need | lazy | — |

The **pure** pieces (`inventory_risk`, `blend_risk`) are unit-tested without
torch; the torch path is guarded and degrades to the pure path in CI (torch CPU
is installed but the checkpoint/network JSON may be absent → exercises the
fallback).

## 4. Phased implementation (each phase shippable, behavior-preserving)

### S1 — Build `gnn_service.py` (no caller changes) · low risk
Extract `inventory_risk` + `blend_risk` as pure functions; add `_load_model`
(single loader with the dim patch + trained guard), `model_status`, and
`store_risk` with graceful fallback. Unit-test the pure math and the
inventory-only fallback. **Nothing consumes it yet** — pure addition.

### S2 — Command Center delegates · medium risk
Re-point `get_gnn_resources` and `get_all_store_risks` at the service. This must
be **behavior-preserving** — same blend, same `gnn_risk_blend_ratio` config.
Verify by capturing a before/after risk-score snapshot for the demo stores
(a small golden check) so the migration provably doesn't move the numbers.

### S3 — ST-GAT delegates · medium risk
Re-point `st_gat_dashboard.load_resources` at the service; the 29→30 patch and
the trained guard now live in one place. The MI-B untrained banner becomes
`model_status()`-driven (replacing the ad-hoc session flag added earlier).

### S4 — Operations Console consumes risk (fixes SH-A) · medium risk
`shell.render_ordering` calls `gnn_service.store_risk(...)` and passes the
store's score as `gnn_risk_score` into `SimulationOrderUtil.calculate_order_quantity`
— matching the Command Center. **This changes PO quantities** for high-risk
stores (safety stock inflates), so it's a deliberate behavior change: gate it
behind the existing thresholds and note it. No snapshot exists for replenishment
POs, so add a focused test asserting risk>threshold raises order qty.

### S5 — Intelligence Console consumes risk (closes G-INT-1) · low risk
Network Intel + Pulse show `store_risk` per store (GNN when available, inventory
otherwise) with the `model_status` banner. Read-only; no behavior change to
ordering.

## 5. Testing strategy
- **Pure unit tests** (no torch): `inventory_risk` ratios, `blend_risk`
  sigmoidal brake (incl. the "GNN uniform → defer to inventory" path),
  `store_risk` inventory-only fallback when the model is unavailable.
- **Guarded tests**: `model_status` returns `unavailable`/`untrained` cleanly
  when the checkpoint/network JSON is absent (the CI condition).
- **S2 golden check**: Command Center risk scores unchanged post-delegation.
- **S4 behavior test**: replenishment qty rises when `store_risk` exceeds the
  burst threshold.
- Boot-smoke all affected apps (HTTP 200) at each phase.

## 6. Risks & mitigations
- **R1 Behavior drift in the blend (S2).** Mitigate with the before/after golden
  snapshot of risk scores.
- **R2 PO math change (S4).** This is *intended* (it's the SH-A fix) but visible;
  document it and keep it behind the configurable blend ratio so it can be tuned
  to 0 (GNN off) if needed.
- **R3 torch import cost in the consoles.** Mitigated by lazy import — the
  consoles only pay it if a GNN page is opened and torch is present; the
  inventory fallback needs no torch.
- **R4 WIP bundling.** `ops_dashboard.py` / `st_gat_dashboard.py` carry the
  owner's in-tree WIP; edits there will bundle it with a note, as before.

## 7. Decisions needed before building
1. **Default blend ratio** for the consoles — match the Command Center's 0.5, or
   start conservative (e.g. 0.3, GNN nudges rather than drives)?
2. **S4 scope** — turn GNN risk on for Operations ordering immediately, or ship
   S1–S3+S5 first (consolidation + read-only intelligence) and gate S4 (the PO
   behavior change) behind explicit sign-off?
3. **Phase-2 accessors** (`demand_matrix`/`transfer_scores` for ST-GAT) — include
   now, or keep ST-GAT's direct model use until the service proves stable?

## 8. Explicitly NOT in scope
- **MI-A retrain** — the 29→30 mismatch is consolidated into one place but the
  real fix (a checkpoint matching the model) is a separate ML task.
- Changing the GNN architecture or the inventory heuristic's thresholds.
- The ingestion/simulator de-duplication (separate consolidation efforts).

---

**Recommended sequence:** S1 → S2 (golden-verified) → S3 → S5, then **pause for
sign-off before S4** (the only phase that changes ordering output). That delivers
the consolidation, the honest model-status banner everywhere, and GNN-in-the-
Intelligence-Console with zero behavior change — and isolates the one
behavioral step for a deliberate go/no-go.
