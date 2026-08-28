# The Ordering Formula, and What ST-GAT Actually Is — 2026-08

The session that derived the order quantity from first principles, measured it
against a year of the client's own books, and found the three things wrong with
it. Also: proved the site-selection chain end to end, and reverted an allocation
"fix" that inverted the engine's own priority.

Narrative and commit list: `OASIS_Session_Handover_2026-08-25.md`.
Derivation: <https://claude.ai/code/artifact/52f2238a-ee48-42d3-926d-48f8d9e82305>

Related: [[Odoo_Transfer_Module_2026-08]] · [[Log]] · [[OASIS_ERP_Agnostic_2026-08]]

---

## The formula that has a derivation

    S = d(R+L) + z·sqrt((R+L)·sigma_d^2 + d^2·sigma_L^2)

`P = R + L` is the protection interval — the only horizon in the whole engine
with a derivation. Safety grows with the **square root** of it, not linearly,
and enters **additively**, not as a multiplier.

Three defects, ranked by measured worth on the client's book:

* **`R` is the observed order gap, not a policy review period** — worth 2.14x
  on working capital, the single largest lever.
* **`sigma_L` is absent** — lead time is as variable as it is long (2.22d spread
  against a 2.29d mean), and `d^2·sigma_L^2` is the larger of the two variance
  terms for any line that moves.
* **`z` is a product of five multipliers and eleven category constants** — worth
  only 1.52x across the whole 50%→99% range. The cheapest term, and the one that
  had been tuned.

## What the books settled

* The derivation is sound: lead time **0.0 d median error**, cadence **1.0 d**.
* `supplier_weekly_schedule.json` declares an order weekday for **940
  suppliers**. R was in the data all along, unused for months.
* Actual ordering ran **2.21x** less often than the declared schedule — that gap
  is demand-driven, not a supply constraint.
* A line's cadence is **1.93x** its supplier's visit cadence. A visit brings a
  median of 1.6 lines: supplier cadence measures how often the van arrives,
  never how often a line is restocked.
* Gap CV median **1.27**; only **4%** of lines have a rhythm worth the name.
  **Cadence is a distribution, not a point estimate** — which is why every
  attempt to derive it more precisely felt like chasing something.
* The engine buys ~**p75 service**, implicitly and untunably.

## R is a commitment, not a parameter

The derived model lands cover-to-gap on **0.97x for 41% of the capital** — and
runs 49 lines dry, which no service level fixes. It sizes for a 7-day review
while deliveries arrive every 15. **Halving working capital requires ordering
twice as often**; that is an operations decision about buyer workload, not a
coefficient.

## The measure that cannot be circular

`--mode residual-cover`: for each delivery, cover carried against the gap it
actually had to span. Taken afterwards, so the ordering habit cannot contaminate
it — the circularity that caught me twice. The book scores **1.9x**.

## ST-GAT is site selection, and the method is sound; the models are not

The chain works: location → Huff catchment score → store type → capital → store
profile tier → SKU basket. Proven on four real Nairobi sites, with Westlands
correctly downgraded for cannibalisation despite prime location.

The **Huff gravity model**, travel-time isochrones, competitor friction and
cannibalisation are textbook and correctly implemented. But **both ML artefacts
are trained on synthetic labels generated from hand-written formulas, with no
validation split**: the RandomForest on 10,000 generated rows, the GCN on a
weighted sum of its own input features. Neither has seen an outcome. The GCN is
a *lossy* re-encoding of a formula that is exact in closed form — and it is a
GCN, not a GAT: no attention, no temporal component.

Sell it as **catchment analysis and comparable-store inference**. That claim is
true and survives a technical buyer; "AI predicts store success" does not.

The ordering gate is correct and holding: `OASIS_GNN_ORDERING_WEIGHT` defaults
to 0 and is set nowhere, so the unvalidated model moves no purchase orders.

## Greenfield allocation is INITIAL LOAD, not replenishment

Width first (one pack of everything, "Look Full", ~70% of budget), depth second
(Fast Five anchors), then consolidate, then zero idle capital. Fresh goods
bypass depth entirely at `Cycle + 0.5 days` to prevent spoilage.

**The 60% Rule is real and encoded in data.** The shipped
`department_scaling_ratios.csv` holds 60.7% in essential departments — that is
the documented "Staples 60%, General 40%" split, not an accident. The 171
departments with no price are low-priority discretionary lines deliberately
parked in the orphan reserve.

I rebuilt those weights from turnover share and shipped it. It halved the staple
share (60.7% → 31.3%, Fast Five 35.0% → 14.6%) and was reverted. **The error was
evaluating an initial-load allocator with replenishment concepts** — milk's
wallet looked under-used at 11.8%, but fresh items are JIT-capped by design, so
that wallet was never meant to be drained. A working guard read as a symptom.

## Traps worth keeping

**A stale duplicate shadowed three weeks of derived data** — the loader took
whatever `os.listdir` returned first, and `"…_2025 (3).json"` sorts before
`"…_2025.json"` because a space precedes a dot. Lead times inflated 3–7x.

**The derivation wrote keys nothing could read** — lower-case supplier names
where every consumer looks up upper-case. Zero of 486 matched. Invisible,
because the file only ever got written on an instance too thin to pass the
replace guard.

**Check the denominator before raising an alarm.** A 43,405% budget overrun was
434x of one hundred and three shillings.

**Compare like with like.** I called the engine 40% deeper than the human book;
the two ratios had different denominators. Like-for-like they are the same.

**And the standing one:** a fix that measures clean can still invert the design.
Read the operating docs before changing weights that encode a hierarchy.
