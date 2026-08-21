# Session Handover — Transfers, 2026-08-20

> **READ THIS FIRST** if you are picking up the transfer work with no context.
> 16 commits, `a5bcba25` … `95356905`, all on branch `pre-mosaic-backup`.
> **Nothing is pushed** — see *Blockers*.

---

## Where it started and where it ended

**Started:** the transfer engine's fair-share allocation had just been made
order-independent, and the open question was whether the store-matrix axes
could be trusted — everything rested on one Rhapta snapshot.

**Ended:** a 14-store Odoo depot exists, the engine reads it through the real
adapter, an Odoo app presents the plan as a reviewable queue, and an
end-to-end test passes 12/0 against the live instance.

---

## 1. The Odoo test depot

`connectors/odoo/` — four scripts, run in this order:

| script | what it does |
|---|---|
| `build_store_network_seed.py` | 89 MB network file → 3,000-SKU slice |
| `differentiate_store_network.py` | **the essential step** — per-store assortment and demand |
| `seed_store_network.py` | pushes it into Odoo (14 warehouses `C001`–`C014`) |
| `verify_store_network.py` | proves it **through `OdooAdapter`**, 18 checks |

**Why differentiation is not optional.** The raw profile scales every store's
`qty` and `ads` by the same factor, so cover (`qty/ads`) cancels and every
store holds identical days of cover. **The engine found ZERO opportunities on
it.** Depletion alone got 215. Differentiation — assortment and demand derived
from each outlet's real floor area, catchment affluence and store category,
with stock allocated on *network-average* velocity — gets ~5,000.

The mismatch between local demand and central buying is what creates
transferable imbalance. That is the whole trick.

**Gotchas paid for once:** `res.company` orders by name (picks the wrong
company); categories must hang off Odoo's own root or every department reads as
the parent; Odoo 16's immediate-transfer wizard cannot be driven over XML-RPC;
quant writes group by value, so unrounded quantities mean thousands of calls
and dropped connections.

---

## 2. The engine — what changed

**Everything derived, nothing declared.** Four constants remain (`ρ=0.5`,
`κ=500`, `Θ=90`, `R_max=45`); a fifth appearing is a defect.

| was | is | source |
|---|---|---|
| deficit trigger 7d | `stock < ADS × relief` | LATA GRN history |
| fill target 14d | restore to relief, no further | LATA |
| relief `min(14, next+lead)` | `median_gap + lead × variance` | LATA |
| horizon cap 45d | the category's shelf life | AMIT tiers |
| PUSH donor `cover > 60d` | dead, **or** past its category tier | both |
| PUSH recipient: short | **active**, any velocity | the stated goal |
| dead release 50% | 100% — no demand, no service to protect | the definition |

**Measured:** the fixed constants were roughly **half** what the supplier book
requires — median delivery gap 15 days, median lead 3, so a store must survive
~23 days, not 7.

**One donor ledger.** Excess was drawn down by three mechanisms that could not
see each other. On one donor holding 600 units they together promised **1,568**;
now 460. `ProactiveRebalancer` was **deleted** — it ran the PUSH pass's job from
the other entry point with four constants of its own.

---

## 3. The Odoo app — transfers only, by design

**OASIS → Transfers → Suggestions.** The three embedded-console menus are gone;
they shipped the whole product into an iframe and gave away every module.

The model holds the **argument**, not just the movement:

> C007 runs out in about 2 days, before its next delivery is due in 7. C003 is
> holding 263 spare and has 39 days of cover, so moving 104 covers the gap
> without putting the donor short. Leaving it costs roughly KES 62,259.

Approve → **draft** internal transfer, grouped one-picking-per-route. Fresh
lines are never auto-queued. Columns: Decision (pull/push badge), donor and
recipient ADS, both covers, next delivery, computed-on. Pivot and graph views.

**Refresh from OASIS** calls `scan_service.py` (on-prem, stdlib only, refuses a
public bind without a token, one scan at a time). Wired into `--mode serve`,
enabled by `OASIS_ERP=odoo`, staggered +15 min off the telemetry cron.

---

## 4. Bugs found — the ones that mattered

Ranked by what they would have cost.

1. **POS sales counted twice.** Closing a POS order creates a picking whose
   moves land in a customer location — the same units `pos.order.line` already
   records. Proven: a 7-unit sale read as **14**. ADS feeds every horizon,
   reorder point and transfer quantity, so the engine would have confidently
   moved twice what the shop needed.
2. **The connector sync could never complete.** Unbounded collectors tried
   240,966 moves in one pass, killed the worker, never recorded a watermark, and
   held a lock on `ir_cron` that blocked addon upgrades. Now batched — and the
   watermark advances to *what was processed*, because capping alone would have
   silently skipped the remainder while reporting success.
3. **Four Settings rows were dead.** `max_transfer_cost_kes`, `min_excess_ratio`
   and two others shipped in the panel and **nothing read them**.
4. **Staleness could never fire.** `computed_on` was written in local time while
   Odoo stores UTC, so every plan looked permanently fresh. A safety feature
   present, green, and doing nothing.
5. **H1–H4** — see `OASIS_Transfer_Pipeline_Audit_2026-08.md`. All fixed.
6. **The 999 sentinel** reached operator-facing columns and poisoned pivot
   averages. Now stripped at the ingestion boundary.

**Two I introduced and caught:** the POS exclusion used a dotted domain that
dropped every move with a null `picking_id` — 240,962 of 240,966, zeroing all
demand; and storing the resolved horizon on the coverage entry conflated the
trigger's fallback (7d) with the target's (14d).

---

## 5. Documents

| file | role |
|---|---|
| `OASIS_Master_Transfer_Formulae.md` | **the specification** — authoritative |
| `OASIS_Transfer_Methodology_Deep_Dive_2026-08.md` | the evidence |
| `OASIS_Transfer_Methodology_Position_2026-08.md` | the measurement |
| `OASIS_Transfer_Pipeline_Audit_2026-08.md` | pre-test audit, H1–H4 |

---

## 6. State right now

- Odoo up (`oasis-odoo-*`), 14 warehouses, ~200 suggestions in the queue
- One draft transfer `C003→C007` from the test — clear with
  `python devkit/test_transfer_in_odoo.py --undo`
- Full suite: **1,257 passed**, 10 pre-existing failures (4 devkit bootstrap,
  1 licence, 3 trial-restart, 2 order-dependent `odoo_adapter` that pass alone)
- Order-independence **0.00%** on live Odoo data

---

## 7. Blockers and open items

**PUSH CANNOT BE PUBLISHED.** A **2.0 GB zip** committed in `0705d4ae`, plus
five other files over GitHub's hard 100 MB per-file limit. No retry will work;
clearing it means rewriting history across 203 commits. **All 16 commits are
local only.**

**Open, ranked:**

1. **Ordering has no Odoo surface.** Transfers first was the decision; ordering
   repeats the same pattern (`oasis.order.suggestion` → draft PO).
2. **The data is synthetic.** Cross-store variation is modelled, not observed.
   Structural findings hold regardless; the *numbers* — release fraction, dead
   window, category tiers — need re-deriving against real multi-store history.
3. **M1–M5** from the audit: sub-unit rounding breaches the release cap; read
   limits truncate silently (`_sales_by_product` 200k, `fetch_enriched_products`
   100k); the scan is not a consistent snapshot; per-store `safety_days` is a
   dead input; approving a stale suggestion fails confusingly in Odoo.
4. **`decide()` is the ordering path** — greedy, single-donor, raw lead time. It
   shares the donor ledger so it cannot double-spend, but it answers the same
   question with different arithmetic. **Out of scope for transfers.**

---

## 8. Running it

```bash
# scan and post the queue
python connectors/odoo/push_transfer_suggestions.py --limit 200
python connectors/odoo/push_transfer_suggestions.py --dry-run   # writes nothing

# the endpoint the Odoo button calls
OASIS_SCAN_TOKEN=<token> python connectors/odoo/scan_service.py \
    --host 0.0.0.0 --interval 30 --offset 15

# end-to-end test, and undo
python devkit/test_transfer_in_odoo.py
python devkit/test_transfer_in_odoo.py --undo

# analysis
python devkit/analyse_transfer_funnel.py --source odoo
python devkit/compare_transfer_methodologies.py --source odoo
```

Odoo: sign in as **`admin`** (an internal user — `qahirfaraj@gmail.com` is
*portal* and Odoo blocks portal users from the backend entirely).
`oasis.scan_url` = `http://host.docker.internal:8710/scan`.
