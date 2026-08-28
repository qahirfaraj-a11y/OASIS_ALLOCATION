# Session Handover — Ordering, Site Selection, Allocation, 2026-08-25

> **READ THIS FIRST** if you are picking up this work with no context.
> Branch `pre-mosaic-backup`. Commits `3e3de929` … `c24e5b95`.
> **Ten commits are unpushed** — everything after `14ea29d6`.
>
> Previous instalment: [`OASIS_Session_Handover_2026-08-23.md`](OASIS_Session_Handover_2026-08-23.md)

---

## Where it started and where it ended

**Started:** the Odoo transfer module was shipped and the ordering module was
about to be built.

**Ended:** ordering ships as its own Odoo module; the ordering *formula* has
been derived from first principles, measured against a year of the client's own
books, and found to have three identifiable defects; a new outcome metric can
score any change to it; the site-selection chain is proven end to end; and one
allocation "fix" was made, measured, found to be a regression, and reverted.

---

## 1. The Odoo modules — replenishment shipped

`oasis_ordering` ("OASIS Replenishment") is built, installed and live-verified.
Depends on `oasis_connector` + `purchase` and **nothing else** — not `stock`,
not transfers, not telemetry. Separability is asserted by tests.

The one thing ordering has that transfers does not is **basket coupling**: a
transfer line stands alone, but an order line is only admitted because the whole
supplier basket cleared that supplier's minimum. `_check_supplier_minimum`
refuses a part-approved basket before creating the PO, names the shortfall, and
points at the queue lines that would close it — or at Transfers, when the basket
is genuinely too small to buy.

**110 addon tests green** across all three modules. 300 suggestions posted from
two stores with 0 skipped; approve produced a draft PO aimed at the ordering
store's own Receipts operation; cancelling released the line with no orphan.

**Licensing settled**: `oasis_connector` LGPL-3 (the freely redistributable
hook), the three feature modules OPL-1. Two tests hold it, including one that
the LGPL-3 base never acquires a proprietary dependency. The `LICENSE` files in
the paid modules are **pointers** — paste the verbatim OPL-1 text before
publishing.

**Domain**: `oasisretail.co.ke` set in all four manifests. Publisher site is at
[`connectors/odoo/website/index.html`](connectors/odoo/website/index.html) and
published at <https://claude.ai/code/artifact/721a2b94-a8fd-4808-8d45-13dbd6e6bced>.
It must resolve over HTTPS before submitting to apps.odoo.com.

**Offline**: the compose file no longer demands hub secrets to parse. The whole
stack, hub included, starts with zero tokens set.

---

## 2. The ordering formula — derived, and where it is wrong

Full derivation published at
<https://claude.ai/code/artifact/52f2238a-ee48-42d3-926d-48f8d9e82305>.

The order-up-to level that has a derivation:

    S = d(R+L) + z·sqrt((R+L)·sigma_d^2 + d^2·sigma_L^2)

Mapped against the code, **three defects**, ranked by measured worth:

| defect | worth |
|---|---|
| `R` uses the OBSERVED order gap, not a policy review period | **2.14x** on working capital |
| `sigma_L` (lead-time variance) is absent entirely | ~2x the variance term we do model |
| `z` is a product of five multipliers + 11 constants | 1.52x across 50%→99% service |

We have been tuning the cheapest term.

### What the books actually say

Verified against **106,526 GRN lines and 17,138 PO lines, 626 vendors, 2025**:

* Lead time derivation: **0.0 d median error**. Cadence: **1.0 d**. The
  measurement is sound.
* `sigma_L` = **2.22 days against a 2.29-day mean** — lead time is as variable
  as it is long, and the formula treats it as a constant.
* `supplier_weekly_schedule.json` declares an order weekday for **940
  suppliers** — R as policy, sitting unused for months. Orders actually went in
  every 15.5 days, **2.21x** less often than the schedule allows.
* A line's own cadence runs **1.93x** its supplier's visit cadence. A supplier
  visit brings a median of **1.6 lines** — supplier cadence measures how often
  the van arrives, never how often a line is restocked.
* Gap CV median **1.27**; only **4%** of lines have anything like a rhythm.
  Cadence is a distribution, not a point estimate.
* The engine currently buys ~**p75 service** (25 days cover spans 76% of
  intervals) — implicitly, untunably.

### The derived model, behind a flag

`OASIS_ORDER_MODEL=order_up_to` (default `classic`). Measured, same catalogue,
same trigger, 98 scored lines:

    classic       28d cover   2.09x its own gap   11 dry   KES 5,309,151
    derived @90%  12d cover   0.97x its own gap   49 dry   KES 2,268,098

It lands on 1.0x for 41% of the capital. **It is not a drop-in**: 49 lines run
dry and no service level fixes it, because it sizes for a 7-day review while
deliveries historically arrive every 15. **R is a commitment, not a
parameter** — halving working capital requires ordering twice as often.

### The outcome metric

`--mode residual-cover`. For each delivery: cover carried vs the gap it actually
had to span. The only check on a horizon that cannot be circular, because it is
taken afterwards. On the real book: **1.9x — every delivery carries nearly twice
the interval it serves.**

---

## 3. The full-catalogue census

3,346 SKUs. Where every SKU leaves the funnel:

    no measured sales rate         1,402  41.9%
    above reorder point              718  21.5%
    triggered but already covered    580  17.3%
    other / no reason recorded       503  15.0%   <-- diagnosability hole
    ORDERED                          142   4.2%

**Every clamp is a literal**: `pack_size` is 1 on 100% of lines, `moq_floor` is
0 on 100%, `shelf_life` is `365 if dry else 7`. Pack rounding is inert; the
SKU-level MOQ gate cannot bind.

Over-ordering concentrates exactly where the multipliers compound — fast lines
on frequent deliveries: `fast x <=7d gap = 4.05x`, falling monotonically to
`very slow x 15-28d = 1.13x`.

---

## 4. Site selection (ST-GAT) — the least tested, and the honest read

**The chain works end to end.** Proven on four real Nairobi sites:
location → catchment score → store type → capital → tier → SKU basket.

    Karen      0.696 -> Medium Anchor  KES 23-32M -> Standard    10,742 SKUs
    Westlands  0.524 -> Express        KES 4-5.5M -> Mini-Mart    4,738 SKUs
    Kitengela  0.571 -> Mini-Mart      KES 550-700k -> Micro      1,052 SKUs

Westlands was correctly downgraded despite prime location: 0.8 km from an own
store, 36 competing sites inside 10 km.

**What is sound**: the Huff gravity model (textbook retail catchment),
travel-time isochrones with traffic friction, 335 real competitors,
cannibalisation against the own estate, and a clean four-type → four-tier
taxonomy with no overlap.

**What is not**: both ML artefacts are trained on **synthetic labels generated
from hand-written formulas, with no validation split in either**.

* `expansion_model.joblib` — RandomForest on 10,000 rows from
  `generate_synthetic_data()`, under a comment reading *GROUND TRUTH LOGIC
  (What we want the model to learn)*.
* `st_gat_v2.pt` — a 2-layer **GCN** (no attention, no temporal component,
  despite the name) trained to reproduce a weighted sum of its own input
  features. `best_loss = 0.0294` is training loss on the same rows.

Neither has seen an outcome. The GCN is a *lossy* re-encoding of a formula that
is exact, instant and interpretable in closed form.

**The gate is correct and holding**: `ordering_risk()` returns inventory-only
risk unless `OASIS_GNN_ORDERING_WEIGHT > 0`, which defaults to 0 and is set
nowhere. The unvalidated model moves no purchase orders.

**For marketing**: sell it as *catchment analysis and comparable-store
inference*, not *AI predicts store success*. The first is true, defensible under
a technical buyer's questions, and stronger.

Also: `get_detailed_analysis` returns **emoji inside data fields**
(`"High Cannibalization"` is prefixed U+1F6A8). Fine in Streamlit, fatal in a
console, CSV export or API consumer.

---

## 5. Allocation — one change made, measured, reverted

**Greenfield allocation is INITIAL LOAD, not replenishment.** The hierarchy,
from `Allocation_Logic_Breakdown.docx` and `Allocation_Field_Ops_Guide.docx`:

1. **Width first** — Pass 1 puts one pack of everything on the shelf ("Look
   Full"), guarded at ~70% of budget.
2. **Depth second** — Pass 2 funds the Fast Five anchors (Oil, Flour, Sugar,
   Milk, Bread). *"The 60% Rule: we hard-lock 60% of the budget for the Fast
   Five. Nothing else matters until these are safe."*
3. **Consolidate** — Pass 3 cancels sub-MOV supplier orders, reinvests in the
   top 3 anchors.
4. **Zero idle capital** — Pass 2B flex pool and Pass 4 mop-up.

Fresh goods **bypass depth logic** at `Cycle + 0.5 days`, deliberately, to
prevent spoilage.

### What I did, and why it was wrong

I found `department_scaling_ratios.csv` had `Avg_Price = 0` on 171 of 233
departments, rebuilt it from the fully-priced scorecard, and shipped it. It
looked like a clean data fix: worst department overrun fell 43,405% → 1,127%,
median utilisation landed on exactly 100%.

**It was a regression.** The shipped file held **60.7% in essential
departments** — that is the documented "Staples 60%" split encoded in data. The
unpriced departments are low-priority discretionary lines deliberately parked in
the orphan reserve. My rebuild took essentials to 31.3% and the Fast Five from
35.0% to 14.6%, inverting the engine's Day-1 survival priority.

Reverted exactly (`c24e5b95`, byte-identical). `dept_weights.py` is kept but now
**refuses to write** if a rebuild moves the staple share more than 5 points, and
a live test asserts the shipped file stays in the 50–70% band.

**The root error**: I evaluated an initial-load allocator with replenishment
concepts. Milk's wallet looked "under-used" at 11.8% — but fresh items are
JIT-capped by design, so that wallet was never meant to be drained. I read a
working guard as a symptom.

### Still open in allocation

* 53 departments overrun >200% — the **soft cap** in the later passes, not the
  weights. Pass 2B, 3B and 4 all spend outside the wallet system.
* 12,748 of 12,762 skips read `"other"` — nobody can answer "why isn't this
  product in my opening range?"

---

## 6. Traps paid for once

**A stale duplicate shadowed three weeks of derived data.** The loader picked
its file with `next(f for f in os.listdir(...))` — whatever the filesystem
returned first. `supplier_patterns_2025 (3).json` sorts before
`supplier_patterns_2025.json` because a space precedes a dot. Lead times were
inflated 3–7x. `pick_intelligence_file` now resolves deterministically and warns
on ambiguity.

**The derivation wrote keys nothing could read.** `odoo_supplier_rhythm` wrote
lower-case supplier keys; every consumer looks up upper-case. Zero of 486 keys
matched. Invisible because the file only ever got written on an instance too
thin to pass `_may_replace` — the first real customer would have got a valid
file the engine could not read one entry from, silently falling back to a flat
7 days.

**"Top SKU" meant "we have a record".** `is_top_sku` was set for any product
matching a profitability row, never checking the rank it had just read. It fired
on 85.9% of ordered lines, worth +20% each. Now equals `rank < 500` exactly.
**It did not move the 2.09x at all** — the ordered population is genuinely
top-ranked.

**Check the denominator before raising an alarm.** A 43,405% overrun turned out
to be 434x of *one hundred and three shillings*. Total overspend was 4.2% of
budget.

**Compare like with like.** I reported the engine as 40% deeper than the human
book (2.7x vs 1.9x). The two ratios had different denominators — supplier
cadence vs the line's own. Like-for-like they are ~1.8x vs 1.9x.

---

## 7. Repo state

Ten commits unpushed, `3e3de929` … `c24e5b95`:

    3e3de929  publisher website + oasisretail.co.ke
    6465f710  queue reports delivered cover, not target
    5121295f  deterministic intelligence-file loader
    0d519b82  cover to next delivery; ceilings guard daily lines
    be4c86de  confidence floor 6 -> 9; cohort fallback reverted
    c8512f8d  seed the depot from the books; supplier key casing
    2ba2dc42  residual cover metric
    8db81337  derived order model behind a flag
    362667a9  top-SKU gate
    e073cd38  department weight rebuild  (superseded)
    c24e5b95  revert of e073cd38 + hierarchy guard

Untouched throughout, per standing instruction: `oasis_allocation/`,
`license_manager.py`, `st_gat_dashboard.py`, `models/store_gnn.py`.

`recalculate_dept_weights.py` at the repo root is **superseded and should not be
run** — it would recreate the orphan pool.

---

## 8. Where to pick up

**Ordering** — the census queue, in order: the 503 unexplained funnel exits
(diagnosability, no behaviour change); the literals (`pack_size`, `moq_floor`,
`shelf_life` need client data, not code); then the horizon, which needs a
decision about buying rhythm rather than a coefficient.

**Site selection** — the least tested and the biggest risk. Decide whether the
RandomForest earns its place or is replaced by the formula it approximates, and
fix the emoji-in-data-fields before any non-Streamlit consumer.

**Allocation** — deliberately parked. Tuning belongs in a live environment with
visible outputs. The plumbing question is answered: it takes a budget and a
scorecard, so a multi-site retailer with department-matched SKUs can run it.
