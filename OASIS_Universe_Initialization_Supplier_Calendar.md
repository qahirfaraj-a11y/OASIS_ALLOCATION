# O.A.S.I.S. — Universe Initialization: Supplier & Ordering-Calendar Intake (v1.1)

> Extends the **Bootstrap Data Ingestion Pipeline (v1.0)** (`stock.csv` /
> `sales.csv` / `grn.csv`) with the **supplier-level intake** v1.0 is missing.
> This is the information a client must give us — beyond what their ERP carries —
> to set up their *retail universe*: which suppliers are **daily**, which are
> **fresh/perishable**, lead times, and the **ordering calendar** (theirs, or
> system-derived). Generated 2026-06-19.

---

## Why this exists

The RXL/iRetail ERP (see `OASIS_RXL_Porting_Reconciliation.md`) carries items,
stock, prices, sales, and GRN history — but **not** lead time, order frequency,
supplier reliability, supplier *type* (daily/fresh), or an ordering calendar.
Those are exactly the inputs the ordering engine needs
(`calculate_order_quantity` gates on `is_ordering_day`, `is_fresh`,
`median_gap_days`, `lead_time_days`). So they come from **one of two sources**,
and onboarding must establish which **per supplier**:

- **CLIENT_PROVIDED** — the client has a known supplier policy / ordering
  calendar → they give it to us (this dataset).
- **SYSTEM_DERIVED** — no fixed policy → OASIS derives it from GRN cadence (the
  `bootstrap-intel` / LATA path already computes `avg_gap_days`,
  `estimated_delivery_days`, fill-rate reliability).

Default: **provided where known, derived for the rest.**

---

## Dataset #4 — Supplier Master & Ordering Calendar

**Standard filename:** `suppliers.csv` (dropped alongside the v1.0 files in
`C:\Oasis\inbound_drops\bootstrap`).

| Column | Mapping target | Required | Logic / requirement |
|---|---|---|---|
| `Supplier_Name` | `supplier` | **Yes** | Must match the `Supplier`/`primary_vendor` in `stock.csv` + `grn.csv`. |
| `Type` | supplier class | **Yes** | `DAILY` \| `FRESH` \| `REGULAR`. Drives schedule + transfer rules (below). |
| `Order_Days` | weekly schedule | No | e.g. `Mon,Wed,Fri`, or `DAILY`, or **blank** → derive from GRN. |
| `Lead_Time_Days` | `lead_time_days` | No | Order→delivery days. Blank → derive (median PO→GRN gap), else default 3. |
| `Min_Order_Value` | MOQ/MOT | No | Supplier-level minimum order value (KES). Feeds the MOQ gate. |
| `Min_Order_Qty` | MOQ | No | Supplier/SKU minimum units. |
| `Reliability` | `reliability_score` | No | 0–1 if known; else LATA derives from GRN fill rate. |
| `Fresh_Departments` | fresh map | No | Depts this supplier delivers fresh (if `Type=FRESH` covers only some). |

### Supplier `Type` → engine behaviour
- **DAILY** → `SupplierCalendar` schedule `'DAILY'`; `median_gap_days = 1`;
  ordering allowed every day; tight cycle stock.
- **FRESH** → `is_fresh` / fresh-department path: **no auto-transfers**
  (perishable; transit shortens shelf life), DDoS-tight coverage (~1.2 days),
  stale-block rules apply. Each store orders fresh to its own sell-through.
- **REGULAR** → scheduled ordering by `Order_Days` (or derived cadence); standard
  safety buffer + gap-plug transfers eligible.

---

## The ordering-calendar question (ask per supplier, or network-wide)

> *"Do you follow a fixed ordering calendar, or should OASIS set the cadence from
> your delivery history?"*

| Answer | What we capture | Where it lands |
|---|---|---|
| **Fixed calendar** | `Order_Days` per supplier (or DAILY) | `supplier_weekly_schedule.json` → `SupplierCalendar.get_schedule()` returns the day-set |
| **Derive it** | nothing — leave `Order_Days` blank | OASIS computes `avg_gap_days` from GRN → DAILY (≤1.5d) or a weekly set → `supplier_rhythm_analysis.json` |
| **Hybrid** | provide for key suppliers, blank the rest | provided overrides; derived fills gaps |

---

## Where each field flows (engine mapping)

| Intake field | OASIS artifact | Consumed by |
|---|---|---|
| `Order_Days` / `DAILY` | `supplier_weekly_schedule.json` | `calculate_order_quantity` → `is_ordering_day` |
| `Lead_Time_Days` | `supplier_patterns[*].estimated_delivery_days` | critical threshold, cycle coverage |
| `Reliability` | `supplier_patterns[*].reliability_score` (LATA) | LATA / AMIT GMROI *(safety-buffer wiring is a known follow-up, F3)* |
| `Type=FRESH` / `Fresh_Departments` | fresh-department map | fresh no-auto-transfer + DDoS coverage |
| `Min_Order_Value` / `Min_Order_Qty` | MOQ/MOT thresholds | `apply_minimum_order_gate` |
| (derivation source) | GRN cadence + fill rate | `bootstrap-intel` / LATA when fields blank |

---

## Onboarding checklist (what to ask the client)

1. **Supplier list** with `Type` (daily / fresh / regular) — at minimum the daily
   and fresh ones; the rest default to REGULAR.
2. **Ordering calendar?** Fixed (give `Order_Days`) or derive-from-history.
3. **Lead times** for key suppliers (else derived from PO→GRN gaps).
4. **Minimum order** value/qty per supplier (for the MOQ gate).
5. **Fresh scope** — which departments/suppliers are perishable (no auto-transfer).
6. Anything blank → **system-derived from GRN** (no blocker to go-live; accuracy
   improves as GRN history accumulates).

---

## Net

v1.0 ingests *what happened* (stock/sales/GRN). **v1.1 adds the supplier
*policy*** — the daily/fresh classification and ordering calendar — which the ERP
doesn't hold and the engine needs. Capturing it up front (with GRN-derivation as
the fallback) is what lets us stand up a correct, ready-to-order retail universe
on day 0. This is the client-provided complement to the ERP port: the porting
views supply *transactional truth*; this intake supplies *supplier policy*.
