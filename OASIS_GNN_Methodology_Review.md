# O.A.S.I.S. — Store GNN Methodology Interrogation (pre-S4 gate)

> **Purpose:** Before wiring GNN store-risk into the operational ordering cycle
> (SH-A S4), interrogate *how the model is trained* and decide whether its risk
> output is safe to drive purchase-order quantities. The explicit concern: **do
> not inject an anomaly into operations.**
> Read-only investigation. Generated 2026-06-18.
>
> **Verdict up front:** The GNN risk head is a *static, attribute-derived
> vulnerability prior* that is **provably insensitive to every live operating
> signal** (stockouts, payday, rain, seasonality). It must **not** drive PO
> quantities as currently trained. The live risk that *should* drive safety
> stock is already captured, directly and trustworthily, by the inventory
> heuristic (`inventory_risk`). Recommendation: **S4 = inventory-risk only**;
> keep the GNN as a monitoring signal; open a separate retraining workstream.

---

## 1. How the model is trained (the methodology)

Source: [`models/train_store_gnn.py`](models/train_store_gnn.py),
[`models/store_gnn.py`](models/store_gnn.py),
[`network_simulation.py`](network_simulation.py).

- **Data:** one graph of ~14 stores from `stores_network.json`. The *same*
  file is used to train and to run inference. No train/validation/test split.
- **Labels are synthetic heuristics**, generated from the store JSON — not
  observed outcomes:
  - **Risk** ([train_store_gnn.py:93-136](models/train_store_gnn.py)) — a fixed
    weighted formula over **static** attributes: supplier-diversity, budget
    (size), category (Express vs anchor), footfall rank, SKU count. No
    stockout, lost-sale, or demand-variance term anywhere.
  - **Demand** ([:42-90](models/train_store_gnn.py)) — department share of
    scaled ADS from the stock profile.
  - **Transfer** ([:139-181](models/train_store_gnn.py)) — same-region + size
    differential + department overlap.
- **Training loop** ([:186-312](models/train_store_gnn.py)) — 100 epochs of
  full-batch gradient descent on the 14-node graph; combined KLDiv + MSE + MSE
  loss; tracks `best_loss` only.
  - **No random seed** → every retrain yields a different checkpoint.
  - **No validation / held-out metric** → `best_loss` is in-sample.
  - 100 epochs over 14 nodes → in-sample memorization, no generalization signal.

### 1.1 The fatal detail: the dynamic feature block is trained on constant zero

The 30 input features split into **static** (cols 0–23) and **dynamic** (cols
24–29). The feature builder hard-codes the dynamic block to zero:

`store_to_features(...)` ends with `features.extend([0.0] * 6)`
([store_gnn.py:378-385](models/store_gnn.py)) — i.e. at **training time**:

| Col | Feature | Value during training |
|---|---|---|
| 24 | stockout_ratio_signal | **0.0** |
| 25 | critical_ratio_signal | **0.0** |
| 26 | sin(month-phase) | **0.0** |
| 27 | cos(month-phase) | **0.0** |
| 28 | rain | **0.0** |
| 29 | salary_hit | **0.0** |

A GCN layer computes `x @ W`; a feature column that is constant zero contributes
**zero gradient** to its weight column, so `conv1.weight[24:30, :]` never learns
— it stays at (or near) initialization with no relationship to anything.

**But at inference these columns are populated:**
- `network_simulation.get_feature_matrix()` sets cols 26–29 live —
  sin/cos month phase, rain from the weather service, payday step
  ([network_simulation.py:568-587](network_simulation.py)).
- The Command Center additionally injects cols 24–25 with live stockout/
  critical ratios *after* building the matrix
  ([ops_dashboard.py:360-361](ops_dashboard.py)).

So the model is fed live signal on exactly the dimensions it learned **nothing**
about. Whatever those untrained weights happen to be, they have no earned
meaning.

---

## 2. Empirical confirmation (probes against the live checkpoint)

Run against the actual `st_gat_v2.pt` via `gnn_service`:

| Probe | Result | Interpretation |
|---|---|---|
| Risk spread across the 14 stores (`max−min`) | **0.039** | The model *did* learn the static vulnerability heuristic — stores differ by a fixed structural prior. Above the blend's 0.02 "uniform" threshold, so this prior **does** enter blended risk today. |
| Inject full stockout into store 0 (col 24=25=1.0) | risk Δ = **−0.00001** | The GNN risk head is **effectively blind to live stockouts.** The Command Center's injection of so/crit ratios into the GNN is a **no-op.** |
| Toggle payday signal store 0 (col 29: 0→1) | risk Δ = **0.00000** | Zero learned weight on the payday feature — confirms the entire dynamic block is inert. |

**What this means concretely:** the GNN's "risk" is a fixed number per store
derived from its static attributes (small / Express / low-footfall →
permanently higher). It does not move when the store actually stocks out, when
it's payday, or when it rains. The live risk signal reaches the *blended* score
**only** through the separate `inventory_risk` term — never through the model.

---

## 3. So what is the GNN risk, really?

- **It is:** a learned, graph-smoothed approximation of a 5-term linear
  heuristic over static store attributes — a *structural vulnerability index*.
- **It is not:** a forecast of today's stockout probability, demand spike, or
  lost-sales risk. It has no live, temporal, or outcome component.
- It is also **near-circular** (the same static attributes are both inputs and
  the basis of the label) and **non-reproducible** (unseeded), **unvalidated**
  (no held-out metric), and fit **in-sample on 14 nodes**.

This is why nothing has visibly broken: the platform's *real* risk decisions run
on `inventory_risk`, and the GNN has so far only been a dashboard number.

---

## 4. The anomaly S4 would inject (the user's concern, made precise)

S4 wires blended `store_risk` into `calculate_order_quantity(gnn_risk_score=…)`,
which inflates safety stock 1.0→1.3 for `risk > 0.5`
([simulation_bridge.py:215-217](oasis/logic/simulation_bridge.py)).

Because the GNN component is a **static** prior (spread 0.039, enters with weight
up to the blend ratio when inventory risk is low), letting it drive POs would
add a **persistent structural bias to order quantities** — certain stores would
be systematically over- or under-ordered based on attributes like footfall rank
and store category, **independent of their actual stock position**, and that
bias was never validated against a single real outcome. That is precisely an
anomaly injected into operations.

The inventory component, by contrast, is exactly what we want driving safety
stock: it responds directly to live stockout/critical ratios, is interpretable
(`so*1.5 + crit*0.5`), and needs no model.

---

## 5. Recommendation

1. **Proceed with S4, but inventory-risk only.** Wire `store_risk` into
   ordering with the GNN contribution off for the ordering path — i.e. drive
   `gnn_risk_score` from `inventory_risk` (or run S4 at `blend_ratio = 0`). This
   delivers the SH-A goal (risk-aware POs) using the trustworthy signal and
   injects **no** static GNN bias. It is also a one-line difference from the
   full S4.
2. **Keep the GNN where approximate is acceptable** — the ST-GAT dashboard and
   the read-only console risk columns (S5). Being a structural prior there is
   harmless and honest, especially with the `model_status` banner.
3. **Open a separate "GNN retraining methodology" workstream** before the GNN is
   ever allowed to drive quantities. A sound version needs, at minimum:
   - **Real labels** — observed stockouts / lost sales / demand variance from
     POS history, not a static attribute formula.
   - **The dynamic features actually exercised** — train across many simulated
     or historical days so cols 24–29 carry variance and earn weights (today
     they are dead).
   - **Train / validation / test split** and a **held-out metric** (e.g. risk
     AUC vs realized stockouts), not just training loss.
   - **A fixed seed** for reproducible checkpoints.
   - Then a bake-off: does the GNN beat `inventory_risk` on held-out outcomes?
     If not, it stays a monitoring aid.

---

## 6. One-line summary

The store GNN is a static, unvalidated, attribute-derived vulnerability prior
whose dynamic inputs are provably dead; it is safe to *show* but unsafe to *act*
on. Drive S4 from the inventory heuristic, and don't let the GNN touch order
quantities until it's retrained against real outcomes with its live features
actually trained.
