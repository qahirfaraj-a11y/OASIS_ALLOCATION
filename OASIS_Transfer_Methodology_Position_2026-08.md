# Transfer Methodology — Statement of Position

> **THE MEASUREMENT.** The derived methodology against the one the Command
> Center runs today, across six seeded networks. Formulae in
> `OASIS_Master_Transfer_Formulae.md`.


**Date:** 2026-08-20
**Question:** how does the transfer methodology now in the engine compare with
the one the Command Center actually runs?
**Answer, in one line:** they were the **same code in two different
configurations**, and the Command Center was wired to the weaker one. **Closed
in `4a5f667c`** — all five call sites now pass `data_dir`, so the product runs
the derived methodology.

> **Status.** Everything below §3 measures the gap **as it was**. It is kept
> as the evidence for why the wiring matters and the baseline a regression
> would be caught against — not because the gap is still there.
>
> **Live-verified** on the 14-store Odoo depot through `OdooAdapter`:
> 18 checks passed, 0 failed; the derived plan is **5,110 opportunities
> (4,285 PULL / 825 PUSH), 23,361 units, KES 5.81M, 12.1% fresh** against the
> degraded **4,492 / 29,774 units / 42% fresh**. Figures below the fold come
> from the offline reconstruction and differ only where Odoo holds rounded
> quantities; no conclusion changes.

Measured on the 14-outlet Odoo depot (2,971 ranged SKUs, 26,881 store-SKU
pairs), engine at commit `a5bcba25`.

---

## 1. The three methodologies in play

| | where it runs | entry point |
|---|---|---|
| **A. Scan, as wired** | Command Center → Transfer Intelligence tab | `scan_network_opportunities` with `next_delivery_days` only |
| **B. Gap-plug** | Command Center → Smart Ordering tab | `optimize_network` → `decide()` |
| **C. Scan, fully derived** | nothing in the product yet | `scan_network_opportunities` with `data_dir` |

**A and C are the same function.** The difference is entirely in what is
handed to the constructor. **B is a genuinely different maths** and was not
touched by the LATA/AMIT work.

---

## 2. Where they differ, in one line

Both are the same function. Only two inputs differ — see
[`OASIS_Master_Transfer_Formulae.md`](OASIS_Master_Transfer_Formulae.md) §11 for the formal statement:

| | derived | as the Command Center wires it |
|---|---|---|
| relief horizon $R$ | $g_v + \ell_v m_v$, capped at shelf life | $n_v + \ell_v$, capped at 45; else the constants 7 / 14 |
| category threshold $T_k$ | 76 AMIT tiers (bakery 5 … cereals 60) | **45 for every category** |
| unknown supplier | the book's median, 23.0 d | a constant |
| median $R$ | 16.0 d | 7.0 d |
| supplier coverage | 599 (LATA) | 288 (calendar) |

Everything downstream — excess, eligibility, ranking, pool, the fair-share
split — is bit-identical. The Smart Ordering tab's `decide()` is a genuinely
different maths and is stated in the master spec §12.

---

## 3. Measured difference — A vs C on the same depot

| | A — Command Center as wired | C — fully derived | Δ |
|---|---:|---:|---:|
| PULL lines | 3,437 | **4,001** | +16% |
| PULL units | 26,300 | **19,546** | −26% |
| PUSH lines | 681 | **827** | +21% |
| PUSH units | 3,080 | **3,572** | +16% |
| plan value | KES 5,819,974 | KES 5,795,017 | −0.4% |
| median relief | 7.0 d | 16.0 d | |
| category tiers loaded | **0** | 76 | |
| supplier coverage | 288 (calendar) | 599 (LATA) | |

**More lines, fewer units** is the correct direction: more stores are correctly
identified as short against their own supplier's rhythm, and each transfer is
sized to the real gap instead of over-filled.

### The finding with operational teeth

| | A — as wired | C — derived |
|---|---:|---:|
| **fresh / perishable units** | **12,499** | **5,453** |
| dry units | 16,736 | 17,381 |

**43% of the volume the Command Center currently proposes is perishables** —
because with no AMIT tiers loaded the horizon ceiling is a flat 45 days for
everything, including bakery whose real tier is 5 days. The derived
configuration cuts perishable volume by **56%** while moving slightly *more*
dry goods.

Fresh lines are flagged `manual_only` and never auto-queued, so this is not a
silent shipment of spoiling stock — but it is 12,499 units of perishables
placed in front of an operator as suggested movements, most of which the
category tiers would refuse outright.

---

## 4. Tested across six seeded networks

`devkit/compare_transfer_methodologies.py` runs both configurations over store
subsets chosen to stress different parts of the maths — assortment breadth in
this network scales with floor area, so a set of large stores and a set of
small ones are not the same test.

| scenario | | lines | units | fresh u | fresh % | dead cleared |
|---|---|---:|---:|---:|---:|---:|
| **pair** (2) | NETWORK | 44 | 2,965 | 194 | 7% | 72.1% |
| | DERIVED | **87** | 3,341 | 152 | 5% | 72.9% |
| **small-4** | NETWORK | 300 | 975 | 489 | **50%** | 88.9% |
| | DERIVED | **439** | 1,037 | 98 | 9% | 37.8% |
| **large-4** | NETWORK | 276 | 4,283 | 90 | 2% | 76.4% |
| | DERIVED | **403** | 4,907 | 55 | 1% | 77.3% |
| **extremes-4** | NETWORK | 918 | 15,780 | 4,873 | 31% | 83.8% |
| | DERIVED | 974 | **13,616** | 2,550 | 19% | 83.1% |
| **half-7** | NETWORK | 1,662 | 22,723 | 9,039 | 40% | 88.5% |
| | DERIVED | **1,996** | **18,303** | 4,324 | 24% | 86.5% |
| **full-14** | NETWORK | 4,118 | 29,380 | 12,518 | **43%** | 89.3% |
| | DERIVED | **4,828** | **23,118** | 5,450 | 24% | 89.3% |

### They agree on routing and disagree on scope

| scenario | shared (SKU,recipient) pairs | same donor | median qty gap | only NETWORK | only DERIVED |
|---|---:|---:|---:|---:|---:|
| pair | 36 | 100% | 0% | 8 | **47** |
| small-4 | 187 | 99% | 0% | 80 | **210** |
| large-4 | 206 | 98% | 0% | 19 | **123** |
| extremes-4 | 610 | 95% | 0% | 81 | **133** |
| half-7 | 1,039 | 95% | 7% | 178 | **398** |
| full-14 | 2,149 | 90% | 2% | 414 | **863** |

Where both serve a line they pick the **same donor 90–100% of the time** and
size it within a few percent. The methodologies do not disagree about routing.
They disagree about **which lines exist at all**: DERIVED uniquely serves
roughly twice as many (SKU, recipient) pairs in every scenario, because a
7-day trigger cannot see a store that is short against a fortnightly supplier.

Same-donor agreement decays as the network grows (100% → 90%), which is what
contention does: with more competing recipients the donor pools bind, and the
two allocate a scarce pool differently because they disagree on need.

### The small-store inversion, and why the metric lies

`small-4` is the one place NETWORK appears to win: **88.9% of dead stock
cleared against DERIVED's 37.8%.** It does not win.

| small-4 | dead-clearing units | of which FRESH | median recipient velocity |
|---|---:|---:|---:|
| NETWORK | 755 | **466 (62%)** | 0.40 /day |
| DERIVED | 321 | 33 (10%) | 0.33 /day |

NETWORK reaches that number by moving **perishables into stores selling under
half a unit a day**. With no AMIT tiers loaded every category shares a 45-day
threshold, so a receiver's absorption is computed as though bakery kept for six
weeks. That is not clearing dead capital; it is relocating it somewhere it will
be written off instead.

At full network scale the two clear **exactly the same dead stock** — 6,102
units, 89.3%, 18% fresh in both. The clearance advantage is entirely an
artifact of small, slow stores.

### What actually separates them

1. **Scope.** DERIVED finds ~2x more uniquely-served lines at every network
   size. The shipped 7-day trigger is blind to stores short against a
   fortnightly supplier.
2. **Perishable volume.** 12,518 units vs 5,450 at full scale. This is the
   single largest difference and it grows with network size (7% → 43% for
   NETWORK, 5% → 24% for DERIVED).
3. **Sizing.** NETWORK moves 27% more units on 17% fewer lines: it over-fills,
   because its horizon carries no variance term and its ceiling ignores shelf
   life.
4. **Not routing.** 90–100% same-donor agreement. Whatever else is wrong, both
   pick the right store to take from.

---

## 5. Position

1. ~~**The methodology is in the engine; the product is not receiving it.**~~
   **CLOSED.** All five call sites — `desktop/data.py` (×2), `ui/intel.py`,
   `ui/shell.py` (×2) — now pass `data_dir`. Verified at construction: 599
   LATA suppliers, 23.0 d median relief, 76 AMIT tiers, bakery threshold 5 d
   rather than 45. The Command Center construction reproduces the derived plan
   exactly (4,001 PULL / 19,546 units / 5,450 perishable).

   `desktop/data.py` passes **both** `next_delivery_days` and `data_dir`. That
   is deliberate and strictly better than either alone: LATA answers first,
   and the supplier calendar covers part of the tail LATA has not seen before
   the network-median fallback is reached.

2. ~~**Verification against the live depot.**~~ **CLOSED.**
   `connectors/odoo/verify_store_network.py` — **18 passed, 0 failed** through
   `OdooAdapter` over XML-RPC, and every scenario in §4 re-run with
   `--source odoo`. The live figures confirm the offline reconstruction:
   differences are confined to where Odoo holds rounded quantities, and no
   conclusion changes.

3. **Not a transfer matter — `decide()`.** The Smart Ordering path has its own
   arithmetic, but it is an **ORDERING** engine: it decides whether a shortfall
   is met by a supplier order, a transfer, or both. Transfers are one branch of
   an ordering decision there, not the subject. It shares the donor ledger, so
   it cannot double-spend against the transfer passes. Whether its branch
   should delegate to the scan belongs to the ordering workstream and is
   deliberately out of scope here.

4. **Standing caveat.** All measurements are on a synthetic multi-store
   network built from one real Rhapta snapshot. The *structural* findings hold
   regardless of data; the *numbers* need re-deriving against genuine
   multi-store history before they are trusted. This is the one open item on
   the transfer methodology.
