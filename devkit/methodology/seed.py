"""Seed the methodology vault from what the repo already established.

The ledger does not start empty. Every claim below was already derived,
measured or falsified in OASIS_GNN_Methodology_Review.md,
OASIS_Intelligence_Ordering_Pipeline_Analysis.md, or
oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md.

Idempotent: re-running preserves any node whose file already exists unless
--force is passed, so hand edits to a claim's body survive a re-seed.

    python devkit/methodology/cli.py ...   # after seeding
    python devkit/methodology/seed.py [--force]
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from devkit.methodology.vault import VAULT, Node, dump_frontmatter  # noqa: E402

SRC_ORD = "oasis_vault/Decisions/Ordering_Formula_And_Site_Selection_2026-08.md"
SRC_GNN = "OASIS_GNN_Methodology_Review.md"
SRC_PIPE = "OASIS_Intelligence_Ordering_Pipeline_Analysis.md"

# ---------------------------------------------------------------- surfaces
SURFACES = [
    ("surface.purchase-order-quantity", "ordering", "Purchase order quantity",
     "Money leaves the business here. `calculate_order_quantity` in "
     "`oasis/logic/simulation_bridge.py`, reached by Command Center and "
     "Operations Console alike."),
    ("surface.transfer-suggestion", "ordering", "Transfer suggestion",
     "`ConsolidatedTransferService.scan_network_opportunities`. Moves stock "
     "between stores; costs logistics and shifts service."),
    ("surface.initial-load-allocation", "allocation", "Initial load allocation",
     "Greenfield opening stock. Width first, depth second — NOT replenishment. "
     "See trap.category-error before judging it with replenishment metrics."),
    ("surface.allocation-priority", "allocation", "Allocation priority (AMIT/GMROI)",
     "Ranks what gets scarce capital. LATA's toxicity signal reaches ordering "
     "only through here — see claim.ordering.lata-not-in-safety-buffer."),
    ("surface.site-recommendation", "siting", "Site recommendation",
     "A capital commitment on the order of a store build. `rank_sites` / "
     "`score_site` in `oasis/logic/site_scoring.py`."),
    ("surface.store-risk-display", "ordering", "Store risk (display only)",
     "Dashboard number. Deliberately NOT money: the blended GNN prior is shown "
     "here and gated out of ordering. Kept as a node so the distinction is "
     "explicit rather than remembered."),
]

# ------------------------------------------------------------- parameters
# (id, domain, title, value, where, feeds, body)
PARAMS = [
    ("param.R", "ordering", "Review period / order gap", "7d policy vs observed gap",
     "`oasis/logic/order_up_to.py`", ["surface.purchase-order-quantity"],
     "The protection interval is `P = R + L`. `R` is currently a policy review "
     "period; the books say it should be the OBSERVED order gap. Worth **2.14x** "
     "on working capital — the single largest lever in the engine.\n\n"
     "`R` is a commitment, not a coefficient: halving working capital requires "
     "ordering twice as often, which is an operations decision about buyer "
     "workload. Proposals touching it are `REQUIRES_HUMAN_COMMITMENT`."),
    ("param.L", "ordering", "Lead time", "2.29d mean",
     "`oasis/data/grn_intelligence_cache.json`", ["surface.purchase-order-quantity"],
     "Median error of the derivation against the client's book: **0.0 days**. "
     "The point estimate is sound; its variance is the problem — see "
     "`param.sigma_L`."),
    ("param.sigma_L", "ordering", "Lead time standard deviation", "2.22d — ABSENT from the term",
     "not referenced in `calculate_order_quantity`", ["surface.purchase-order-quantity"],
     "`d^2 * sigma_L^2` is the larger of the two variance terms for any line "
     "that moves, and it is **not in the formula**. Lead time is as variable as "
     "it is long: 2.22d spread against a 2.29d mean."),
    ("param.z", "ordering", "Service level multiplier", "5 multipliers x 11 category constants",
     "`oasis/logic/order_engine.py`", ["surface.purchase-order-quantity"],
     "Worth only **1.52x** across the entire 50%->99% range. The cheapest term "
     "in the formula, and the one that had been tuned. The engine buys ~p75 "
     "service implicitly and untunably."),
    ("param.gnn-ordering-weight", "ordering", "OASIS_GNN_ORDERING_WEIGHT", "0 (default, set nowhere)",
     "`oasis/logic/gnn_service.py:104`", ["surface.purchase-order-quantity"],
     "**The gate that works.** The unvalidated GNN contributes to ordering risk "
     "only when this is > 0. It is 0 by default and set nowhere, so the model "
     "moves no purchase orders.\n\nThis parameter is the template for the whole "
     "architecture: a claim that has not earned `validated` does not get a "
     "non-zero weight."),
    ("param.gnn-risk-blend-ratio", "ordering", "gnn_risk_blend_ratio", "0.5 (display path)",
     "`oasis/logic/gnn_service.blend_risk`", ["surface.store-risk-display"],
     "Blends the static GNN prior into the DISPLAYED store risk. Harmless here "
     "and honest with the `model_status` banner. It reached live POs once "
     "(finding F2) and was closed by routing ordering through `ordering_risk`."),
    ("param.department-scaling-ratios", "allocation", "Department scaling ratios", "60.7% essential",
     "`oasis/data/department_scaling_ratios.csv`", ["surface.initial-load-allocation"],
     "The documented Staples 60% / General 40% split, encoded in shipped data. "
     "A turnover-share rebuild halved it (60.7% -> 31.3%, Fast Five 35.0% -> "
     "14.6%) and was reverted. See `trap.hierarchy-inversion`."),
    ("param.lata-variance-multiplier", "ordering", "LATA variance multiplier", "computed, not applied",
     "`amit_gatekeeper.load_lata_patterns`", ["surface.allocation-priority"],
     "LATA's stated purpose is Supplier Shield: inflate safety stock for "
     "unreliable suppliers. It does not appear in `calculate_order_quantity`. "
     "It feeds allocation priority instead. Finding F3, still open."),
    ("param.size-exponent", "siting", "SIZE_EXPONENT", "1.0 — a convention",
     "`oasis/logic/site_scoring.py:114`", ["surface.site-recommendation"],
     "The exponent on floor area in the Huff attractiveness term. Never fitted. "
     "`ALPHA_RANGE = (0.6, 0.8, 1.0, 1.2)` exists to sweep it; nothing has ever "
     "chosen a value from data."),
    ("param.distance-decay", "siting", "DISTANCE_DECAY", "2.0 — a convention",
     "`oasis/logic/site_scoring.py:162`", ["surface.site-recommendation"],
     "The distance exponent in `u = A / d^beta`. Never fitted. "
     "`BETA_RANGE = (1.5, 2.0, 2.5, 3.0)` exists to sweep it."),
    ("param.catchment-km", "siting", "CATCHMENT_KM", "10.0 — a convention",
     "`oasis/logic/site_scoring.py:50`", ["surface.site-recommendation"],
     "The catchment radius. A convention, not a measurement. Every capture "
     "figure the model reports is conditional on it."),
    ("param.cannibalisation-km", "siting", "CANNIBALISATION_KM", "3.0 — a convention",
     "`oasis/logic/site_scoring.py:87`", ["surface.site-recommendation"],
     "The radius within which an own-store is counted as cannibalised. Drives "
     "the Westlands-style downgrades, which are the model's most defensible "
     "output — and rest on an unfitted constant."),
]

# ----------------------------------------------------------------- probes
# (id, domain, title, entrypoint, tests, body)
PROBES = [
    ("probe.residual-cover", "ordering", "Residual cover",
     None, ["claim.ordering.R-is-observed-gap", "claim.ordering.p75-service-implicit"],
     "For each delivery, the cover carried against the gap it actually had to "
     "span. Taken **afterwards**, so the ordering habit cannot contaminate it — "
     "the circularity that caught us twice. The book scores **1.9x**.\n\n"
     "This is Loop B's objective function. TO BUILD: "
     "`devkit/probe_residual_cover.py`, reading the decision ledger joined to "
     "realised GRN."),
    ("probe.term-attribution", "ordering", "Formula term attribution",
     None, ["claim.ordering.sigma-L-missing", "claim.ordering.z-over-engineered"],
     "Decompose residual-cover error across dR, dL, dsigma_L, dz. Must "
     "reproduce the known ranking (R 2.14x > sigma_L > z 1.52x) before it is "
     "trusted to rank anything new.\n\nTO BUILD: `devkit/probe_term_attribution.py`."),
    ("probe.order-sensitivity", "allocation", "Allocation order sensitivity",
     "devkit/measure_order_sensitivity.py", ["claim.allocation.iteration-order-matters"],
     "Runs the identical scan with stores iterated in three different orders "
     "and compares what each recipient got. Already written."),
    ("probe.siting-robustness", "siting", "Siting robustness",
     "devkit/siting_robustness.py",
     ["claim.siting.size-exponent-unfitted", "claim.siting.distance-decay-unfitted",
      "claim.siting.catchment-km-unfitted"],
     "Perturbs each uncertain input against a real shortlist and reports "
     "top-1 held, top-10 overlap, and rank drift. Already written — what it "
     "has never had is a **series**. Run every cycle; a recommendation whose "
     "stability is falling is a signal even with no outcome label."),
    ("probe.pov-sweep", "siting", "Chain point-of-view sweep",
     "devkit/pov_sweep.py", ["claim.siting.trade-is-market-property"],
     "Every chain's view of the same city, point by point. Already written; "
     "produced the finding that all six chains score an identical 0.95% capture "
     "at a dense-core point."),
    ("probe.comparable-store-correlation", "siting", "Comparable-store correlation",
     None,
     ["claim.siting.size-exponent-unfitted", "claim.siting.distance-decay-unfitted",
      "claim.siting.catchment-km-unfitted", "claim.siting.ml-artefacts-unvalidated"],
     "**The highest-value unbuilt probe.** Score the EXISTING estate with the "
     "same pipeline and correlate predicted capture against realised store "
     "revenue. A real held-out test using stores that already exist — and it "
     "fits all three siting conventions at once.\n\n"
     "TO BUILD: `devkit/probe_comparable_stores.py`. Sweep `ALPHA_RANGE` x "
     "`BETA_RANGE` x catchment, pick by held-out correlation, not by convention."),
    ("probe.gnn-feature-injection", "ordering", "GNN feature injection",
     None, ["claim.gnn.risk-blind-to-stockouts", "claim.gnn.dynamic-block-untrained"],
     "Inject full stockout into a store (cols 24-25 = 1.0) and toggle payday "
     "(col 29) against the live checkpoint; measure the risk delta. Produced "
     "-0.00001 and 0.00000 respectively. Re-run on every retrain — this is the "
     "check that keeps the gate honest."),
    ("probe.pipeline-trace", "ordering", "Ordering pipeline trace",
     None, ["claim.ordering.lata-not-in-safety-buffer", "claim.ordering.rop-fallback-always-fires"],
     "Static trace of which intelligence signals actually reach "
     "`calculate_order_quantity`. Produced findings F1-F6. Cheap to re-run and "
     "catches the whole class of computed-but-unused signals."),
    ("probe.backtest-allocation", "allocation", "Allocation backtest",
     "backtest_allocation.py", ["claim.allocation.sixty-percent-rule-real"],
     "Allocate -> simulate -> measure fill rate -> repeat. Already written. "
     "Guard with `trap.category-error`: this is an INITIAL-LOAD allocator and "
     "must not be scored with replenishment metrics."),
    ("probe.transfer-methodology-compare", "ordering", "Transfer methodology comparison",
     "devkit/compare_transfer_methodologies.py", ["claim.transfer.network-vs-derived-diverge"],
     "Command Center's transfer scan vs the derived methodology, across "
     "several store subsets because they do not diverge uniformly. "
     "Already written."),
]

# ----------------------------------------------------------------- claims
# (id, domain, status, title, worth, ttl, supports, tested_by, guarded_by, body)
C = [
    ("claim.ordering.R-is-observed-gap", "ordering", "measured",
     "R should be the observed order gap, not a policy review period",
     "2.14x working capital", 90, ["param.R"], ["probe.residual-cover"],
     ["trap.circularity", "trap.like-for-like"],
     "`P = R + L` is the only horizon in the engine with a derivation. Safety "
     "grows with the **square root** of it and enters **additively**.\n\n"
     "`supplier_weekly_schedule.json` declares an order weekday for **940 "
     "suppliers**. R was in the data all along, unused for months. Actual "
     "ordering ran **2.21x** less often than the declared schedule — a "
     "demand-driven gap, not a supply constraint.\n\n"
     "The derived model lands cover-to-gap on 0.97x for 41% of capital and runs "
     "49 lines dry: it sizes for a 7-day review while deliveries arrive every "
     "15. **This cannot be promoted without an operations commitment** — "
     "halving working capital means ordering twice as often."),
    ("claim.ordering.sigma-L-missing", "ordering", "measured",
     "sigma_L belongs in the safety term and is the larger variance contributor",
     "2.22d spread on a 2.29d mean", 90, ["param.sigma_L"], ["probe.term-attribution"],
     ["trap.denominator-sanity"],
     "The formula is `S = d(R+L) + z*sqrt((R+L)*sigma_d^2 + d^2*sigma_L^2)`. "
     "The `d^2*sigma_L^2` term is absent from the implementation. Lead time is "
     "as variable as it is long, so for any line that moves this is the larger "
     "of the two variance terms."),
    ("claim.ordering.z-over-engineered", "ordering", "measured",
     "z as five multipliers times eleven category constants is over-engineered",
     "only 1.52x across 50%->99%", 180, ["param.z"], ["probe.term-attribution"], [],
     "The cheapest term in the formula, and the one that had been tuned. Effort "
     "went where the leverage was not. Ranking terms by measured worth is the "
     "whole point of the attribution probe."),
    ("claim.ordering.cadence-is-a-distribution", "ordering", "measured",
     "Cadence is a distribution, not a point estimate",
     "gap CV median 1.27; 4% of lines have a rhythm", 180, ["param.R"],
     ["probe.residual-cover"], ["trap.circularity"],
     "A line's cadence is **1.93x** its supplier's visit cadence, and a visit "
     "brings a median of 1.6 lines. Supplier cadence measures how often the van "
     "arrives, never how often a line is restocked.\n\nThis is why every attempt "
     "to derive cadence more precisely felt like chasing something."),
    ("claim.ordering.p75-service-implicit", "ordering", "measured",
     "The engine buys ~p75 service, implicitly and untunably",
     "service level is an emergent property, not a setting", 180, ["param.z"],
     ["probe.residual-cover"], [],
     "Nobody chose p75. It falls out of the safety buffer's shape. A service "
     "level the business did not pick is a service level it cannot trade off "
     "against working capital."),
    ("claim.ordering.lata-not-in-safety-buffer", "ordering", "measured",
     "LATA supplier toxicity never reaches the replenishment safety buffer",
     "F3 — open, no owner", 60, ["param.lata-variance-multiplier"],
     ["probe.pipeline-trace"], [],
     "The safety buffer is `base*(1+vol*cv)*GNN_mult` with no LATA term. A "
     "toxic-supplier SKU gets no extra replenishment cover. LATA is "
     "computed-but-underused for ordering — exactly the shape of the old GNN "
     "issue.\n\nResolve as either: thread it into the buffer, or document "
     "explicitly that LATA is allocation-only."),
    ("claim.ordering.rop-fallback-always-fires", "ordering", "measured",
     "Live enrichment supplies no ROP or coverage target, so the fallback always fires",
     "F4 — open", 60, ["param.L"], ["probe.pipeline-trace"], [],
     "`fetch_enriched_products` sets ADS / cv / on_order but not "
     "`reorder_point` or `target_coverage_days`. The maths is sound, but this "
     "is a heuristic, not a forecasting layer — worth naming so it is not "
     "mistaken for one."),
    ("claim.gnn.risk-blind-to-stockouts", "ordering", "falsified",
     "GNN store risk responds to live stockouts",
     "risk delta = -0.00001 on full stockout injection", 365,
     ["param.gnn-ordering-weight"], ["probe.gnn-feature-injection"], [],
     "**Falsified.** Injecting a full stockout into store 0 (cols 24-25 = 1.0) "
     "moved risk by -0.00001. The Command Center's injection of stockout ratios "
     "into the GNN is a no-op. Live risk reaches the blended score only through "
     "the separate `inventory_risk` term.\n\nThe gate holds: "
     "`OASIS_GNN_ORDERING_WEIGHT` is 0."),
    ("claim.gnn.dynamic-block-untrained", "ordering", "falsified",
     "The GNN's dynamic feature block (cols 24-29) carries learned weight",
     "payday toggle delta = 0.00000", 365, ["param.gnn-ordering-weight"],
     ["probe.gnn-feature-injection"], [],
     "**Falsified.** `store_to_features` ends with `features.extend([0.0] * 6)`, "
     "so cols 24-29 are constant zero at training time. A constant-zero column "
     "contributes zero gradient; those weights never left initialisation. At "
     "inference the same columns are populated live.\n\nThe model is fed live "
     "signal on exactly the dimensions it learned nothing about."),
    ("claim.gnn.is-static-attribute-prior", "ordering", "measured",
     "GNN risk is a static, attribute-derived vulnerability prior",
     "spread 0.039 across 14 stores", 365, ["param.gnn-risk-blend-ratio"],
     ["probe.gnn-feature-injection"], ["trap.circularity"],
     "Learned, graph-smoothed approximation of a 5-term linear heuristic over "
     "static store attributes. Near-circular (same attributes are inputs and "
     "the basis of the label), non-reproducible (unseeded), unvalidated (no "
     "held-out metric), fit in-sample on 14 nodes.\n\nSafe to **show**, unsafe "
     "to **act** on. That distinction is why `surface.store-risk-display` "
     "exists as a separate node."),
    ("claim.allocation.sixty-percent-rule-real", "allocation", "measured",
     "The 60% staple/general split is real and encoded in shipped data",
     "department_scaling_ratios.csv = 60.7% essential", 180,
     ["param.department-scaling-ratios"], ["probe.backtest-allocation"],
     ["trap.hierarchy-inversion", "trap.category-error"],
     "Not an accident: the documented Staples 60% / General 40% split. The 171 "
     "departments with no price are low-priority discretionary lines "
     "deliberately parked in the orphan reserve.\n\nA turnover-share rebuild "
     "measured clean and was reverted. **The error was evaluating an "
     "initial-load allocator with replenishment concepts** — milk's wallet "
     "looked under-used at 11.8%, but fresh is JIT-capped by design, so that "
     "wallet was never meant to be drained. A working guard read as a symptom."),
    ("claim.allocation.iteration-order-matters", "allocation", "asserted",
     "Greedy allocation's iteration order changes who gets stock",
     "unmeasured — fair-share only matters if it does", 120, [],
     ["probe.order-sensitivity"], [],
     "Fair-share is a refinement with nothing to fix unless the current "
     "order-dependent allocation produces a different outcome from a different "
     "order. The probe exists; the verdict has never been recorded as evidence."),
    ("claim.siting.chain-is-sound", "siting", "measured",
     "The site-selection chain is methodologically sound end to end",
     "proven on four real Nairobi sites", 180, [], ["probe.siting-robustness"], [],
     "location -> Huff catchment score -> store type -> capital -> store "
     "profile tier -> SKU basket. Westlands correctly downgraded for "
     "cannibalisation despite prime location. The Huff gravity model, "
     "travel-time isochrones, competitor friction and cannibalisation are "
     "textbook and correctly implemented."),
    ("claim.siting.ml-artefacts-unvalidated", "siting", "falsified",
     "The siting ML artefacts have predictive validity",
     "neither has seen an outcome", 365, [], ["probe.comparable-store-correlation"],
     ["trap.circularity"],
     "**Falsified.** The RandomForest is trained on 10,000 rows generated from "
     "hand-written formulas; the GCN on a weighted sum of its own input "
     "features. No validation split in either. The GCN is a *lossy* re-encoding "
     "of a formula that is exact in closed form — and it is a GCN, not a GAT: "
     "no attention, no temporal component.\n\n"
     "**Permitted claim:** catchment analysis and comparable-store inference. "
     "That survives a technical buyer. \"AI predicts store success\" does not."),
    ("claim.siting.size-exponent-unfitted", "siting", "asserted",
     "SIZE_EXPONENT = 1.0 is a convention that was never fitted",
     "UNMEASURED — every site recommendation depends on it", 120,
     ["param.size-exponent"], ["probe.comparable-store-correlation"], [],
     "`ALPHA_RANGE = (0.6, 0.8, 1.0, 1.2)` exists precisely to sweep this, and "
     "nothing has ever chosen a value from data. Together with distance decay "
     "and catchment radius this is the largest unmeasured exposure in the "
     "system."),
    ("claim.siting.distance-decay-unfitted", "siting", "asserted",
     "DISTANCE_DECAY = 2.0 is a convention that was never fitted",
     "UNMEASURED — every site recommendation depends on it", 120,
     ["param.distance-decay"], ["probe.comparable-store-correlation"], [],
     "The exponent in `u = A / d^beta`. `BETA_RANGE = (1.5, 2.0, 2.5, 3.0)` "
     "sweeps it; no fit has been performed against realised revenue."),
    ("claim.siting.catchment-km-unfitted", "siting", "asserted",
     "CATCHMENT_KM = 10.0 is a convention that was never fitted",
     "UNMEASURED — conditions every capture figure reported", 120,
     ["param.catchment-km"], ["probe.comparable-store-correlation",
                              "probe.siting-robustness"], [],
     "Every capture percentage the model reports is conditional on this "
     "radius, and the radius is a convention."),
    ("claim.siting.cannibalisation-km-unfitted", "siting", "asserted",
     "CANNIBALISATION_KM = 3.0 is a convention that was never fitted",
     "UNMEASURED — drives the model's most defensible output", 120,
     ["param.cannibalisation-km"], ["probe.comparable-store-correlation",
                                    "probe.siting-robustness"], [],
     "The radius within which an own-store counts as cannibalised. It produces "
     "the Westlands-style downgrades — the outputs a buyer finds most "
     "convincing — and it rests on a constant nobody fitted.\n\n"
     "Found by the graph itself: this parameter fed a live decision with **no "
     "supporting claim at all**, which is a worse position than an unfitted one."),
    ("claim.siting.trade-is-market-property", "siting", "measured",
     "Trade available at a point is a property of the market, not of the chain asking",
     "all six chains scored identical 0.95% capture", 180, [], ["probe.pov-sweep"], [],
     "Measured at one point in the dense core. Not a bug — it falls out of the "
     "model being right: since the own/rival asymmetry was fixed, an existing "
     "store exerts the same pull on the Huff denominator whoever owns it.\n\n"
     "What differs between chains is how much trade you would take from "
     "yourself: Jaza 2.3% cannibalised, Naivas 47.0%."),
    ("claim.transfer.network-vs-derived-diverge", "ordering", "asserted",
     "The shipped transfer scan and the derived methodology give different answers",
     "unmeasured across store subsets", 120, [],
     ["probe.transfer-methodology-compare"], ["trap.like-for-like"],
     "The shipped path passes a supplier-calendar `next_delivery_days` and "
     "cold/hot windows of 60/14 and nothing else — no LATA rhythm, no AMIT "
     "tiers, so the variance term is unreachable and every category shares one "
     "45-day threshold. The probe exists; no verdict has been recorded."),
]

TRAPS_SEED = [
    ("trap.silent-join-failure", "T1", "Silent join failure",
     ["probe.pipeline-trace", "probe.residual-cover", "probe.comparable-store-correlation"],
     "The derivation wrote lower-case supplier names where every consumer looks "
     "up upper-case. **Zero of 486 matched.** Invisible, because the file only "
     "ever got written on an instance too thin to pass the replace guard, and "
     "nothing asserted the match rate.\n\n"
     "**Detector:** `traps.join_match_rate(left, right, name, floor=0.60)`. "
     "Also reports what the rate would be case-normalised, which is the "
     "diagnostic that names the bug rather than the symptom."),
    ("trap.stale-duplicate-shadowing", "T2", "Stale duplicate shadowing",
     ["probe.residual-cover", "probe.term-attribution"],
     "The loader took whatever `os.listdir` returned first, and "
     "`\"..._2025 (3).json\"` sorts before `\"..._2025.json\"` because a space "
     "precedes a dot. A stale duplicate shadowed three weeks of derived data; "
     "lead times inflated **3-7x**.\n\n"
     "**Detectors:** `traps.newest_wins(paths)` at load time, and "
     "`traps.scan_unsorted_globs(root)` as a static sweep for any listdir/glob "
     "consumed without an explicit sort."),
    ("trap.denominator-sanity", "T3", "Denominator sanity",
     ["probe.term-attribution", "probe.backtest-allocation"],
     "A **43,405% budget overrun** was 434x of one hundred and three shillings.\n\n"
     "**Detector:** `traps.ratio(num, den, label)` — any ratio outside "
     "[0.1, 10] must print both terms, so the reader can see whether the number "
     "is large or the base is small."),
    ("trap.like-for-like", "T4", "Like-for-like comparison",
     ["probe.transfer-methodology-compare", "probe.residual-cover"],
     "\"The engine is 40% deeper than the human book\" was two ratios over "
     "different denominators. Like-for-like they were the same.\n\n"
     "**Detector:** `traps.like_for_like((name_a, denom_a), (name_b, denom_b))`."),
    ("trap.circularity", "T5", "Circular measurement",
     ["probe.residual-cover", "probe.comparable-store-correlation"],
     "A measure fed by the behaviour it measures proves only that the behaviour "
     "exists. This caught us **twice** before it was named.\n\n"
     "The pattern that works is `--mode residual-cover`: cover carried against "
     "the gap the delivery actually had to span, taken **afterwards**.\n\n"
     "**Detector:** `traps.non_circular(measure, inputs, downstream_of_behaviour)`."),
    ("trap.hierarchy-inversion", "T6", "Hierarchy inversion",
     ["probe.backtest-allocation"],
     "A fix that measures clean can still invert the design. The rebuilt "
     "department weights scored well and halved the staple share "
     "(60.7% -> 31.3%). A working guard read as a symptom.\n\n"
     "**Detector:** `traps.rank_diff(before, after, hierarchy=...)` — diff the "
     "resulting RANK ORDER against the hierarchy the operating docs encode, "
     "not just the loss. Read the operating docs before changing weights that "
     "encode a hierarchy."),
    ("trap.category-error", "T7", "Category error across regimes",
     ["probe.backtest-allocation"],
     "Greenfield allocation is **initial load**, not replenishment. Width first "
     "(one pack of everything, ~70% of budget), depth second, then consolidate. "
     "Fresh bypasses depth entirely at `Cycle + 0.5 days` to prevent spoilage.\n\n"
     "Milk's wallet looked under-used at 11.8% — but fresh is JIT-capped by "
     "design, so it was never meant to be drained.\n\n"
     "**Detector:** `traps.regime(metric, declared, expected)`. Every metric "
     "must declare its regime."),
]


def write(path, fm, body, force):
    p = Path(path)
    if p.exists() and not force:
        return False
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_text(dump_frontmatter(fm) + "\n\n" + body.strip() + "\n", encoding="utf-8")
    return True


# Evidence that already exists, in prose. (claim, source doc, date, metric)
BACKFILL = [
    ("claim.ordering.R-is-observed-gap", SRC_ORD, "2026-08-25",
     {"working_capital_multiple": 2.14, "cover_to_gap": 0.97,
      "capital_share": 0.41, "lines_run_dry": 49, "schedule_gap": 2.21}),
    ("claim.ordering.sigma-L-missing", SRC_ORD, "2026-08-25",
     {"lead_time_mean_days": 2.29, "lead_time_sd_days": 2.22}),
    ("claim.ordering.z-over-engineered", SRC_ORD, "2026-08-25",
     {"worth_multiple_50_to_99": 1.52}),
    ("claim.ordering.cadence-is-a-distribution", SRC_ORD, "2026-08-25",
     {"gap_cv_median": 1.27, "share_with_rhythm": 0.04,
      "line_vs_supplier_cadence": 1.93, "lines_per_visit_median": 1.6}),
    ("claim.ordering.p75-service-implicit", SRC_ORD, "2026-08-25",
     {"implied_service_level": 0.75, "residual_cover_score": 1.9}),
    ("claim.allocation.sixty-percent-rule-real", SRC_ORD, "2026-08-25",
     {"essential_share": 0.607, "rebuilt_share": 0.313,
      "fast_five_before": 0.350, "fast_five_after": 0.146}),
    ("claim.siting.chain-is-sound", SRC_ORD, "2026-08-25",
     {"sites_proven": 4, "westlands_downgraded": True}),
    ("claim.siting.ml-artefacts-unvalidated", SRC_ORD, "2026-08-25",
     {"rf_synthetic_rows": 10000, "validation_split": False,
      "gcn_has_attention": False, "gcn_has_temporal": False}),
    ("claim.gnn.risk-blind-to-stockouts", SRC_GNN, "2026-06-18",
     {"risk_delta_on_full_stockout": -0.00001, "risk_spread_14_stores": 0.039}),
    ("claim.gnn.dynamic-block-untrained", SRC_GNN, "2026-06-18",
     {"risk_delta_on_payday_toggle": 0.0, "dead_feature_cols": [24, 25, 26, 27, 28, 29]}),
    ("claim.gnn.is-static-attribute-prior", SRC_GNN, "2026-06-18",
     {"risk_spread": 0.039, "train_nodes": 14, "seeded": False,
      "held_out_metric": None}),
    ("claim.ordering.lata-not-in-safety-buffer", SRC_PIPE, "2026-06-19",
     {"finding": "F3", "lata_in_safety_buffer": False}),
    ("claim.ordering.rop-fallback-always-fires", SRC_PIPE, "2026-06-19",
     {"finding": "F4", "reorder_point_supplied": False}),
    ("claim.siting.trade-is-market-property", "devkit/pov_sweep.py", "2026-08-25",
     {"capture_pct_all_six_chains": 0.0095, "people": 31773,
      "cannibalisation": {"jaza": 0.023, "cleanshelf": 0.043, "chandarana": 0.079,
                          "quickmart": 0.163, "carrefour": 0.223, "naivas": 0.470}}),
]


def backfill(force=False):
    """Write the prose-era measurements in as dated evidence.

    Dated to the SOURCE DOCUMENT, not today. Several will already be past their
    TTL, which is the honest position: a measurement from June is not fresh
    evidence in September, and the loop should say so.
    """
    import json
    n = 0
    for claim, src, date, metric in BACKFILL:
        d = VAULT / "Evidence" / claim
        d.mkdir(parents=True, exist_ok=True)
        f = d / f"{date.replace('-', '')}T000000_backfill.json"
        if f.exists() and not force:
            continue
        f.write_text(json.dumps({
            "claim": claim, "probe": "probe.prose-backfill",
            "verdict": "contradicts" if "unvalidated" in claim or "blind" in claim
                       or "untrained" in claim else "supports",
            "metric": metric, "held_out": False, "beat_baseline": None,
            "traps": [], "config_hash": "",
            "notes": f"Backfilled from {src}. Measured before the ledger existed; "
                     "dated to the source document so the TTL is honest.",
            "run_at": f"{date}T00:00:00",
        }, indent=2), encoding="utf-8")
        node = VAULT / "Claims" / f"{claim.split('.', 1)[-1].replace('.', '_')}.md"
        if node.exists():
            t = node.read_text(encoding="utf-8")
            if "last_evidence:" not in t:
                t = t.replace("ttl_days:", f"last_evidence: {date}\nttl_days:", 1)
                node.write_text(t, encoding="utf-8")
        n += 1
    print(f"backfilled {n} evidence record(s)")


def main(force=False):
    n = 0
    for nid, dom, title, body in [(s[0], s[1], s[2], s[3]) for s in SURFACES]:
        n += write(VAULT / "Surfaces" / f"{nid.split('.')[-1]}.md",
                   {"id": nid, "type": "decision", "status": "trusted",
                    "domain": dom, "title": title,
                    "money": nid != "surface.store-risk-display"}, body, force)
    for nid, dom, title, val, where, feeds, body in PARAMS:
        n += write(VAULT / "Parameters" / f"{nid.split('.')[-1]}.md",
                   {"id": nid, "type": "parameter", "status": "measured",
                    "domain": dom, "title": title, "value": val,
                    "neutralised": nid == "param.gnn-ordering-weight",
                    "defined_in": where, "feeds": feeds},
                   f"**Value:** {val}\n**Defined in:** {where}\n\n{body}", force)
    for nid, dom, title, entry, tests, body in PROBES:
        fm = {"id": nid, "type": "probe", "status": "measured", "domain": dom,
              "title": title, "guards": [], "tests": tests}
        if entry:
            fm["entrypoint"] = entry
        else:
            fm["status"] = "asserted"
        n += write(VAULT / "Probes" / f"{nid.split('.')[-1]}.md", fm,
                   (f"**Entrypoint:** `{entry}`\n\n" if entry
                    else "**Entrypoint:** none yet — this probe is a placeholder.\n\n") + body,
                   force)
    for nid, code, title, guards, body in TRAPS_SEED:
        n += write(VAULT / "Traps" / f"{nid.split('.')[-1]}.md",
                   {"id": nid, "type": "trap", "status": "trusted",
                    "domain": "method", "title": title, "code": code,
                    "guards": guards}, body, force)
    for (nid, dom, status, title, worth, ttl, supports, tested_by,
         guarded_by, body) in C:
        src = SRC_GNN if ".gnn." in nid else (SRC_ORD if dom != "siting" else SRC_ORD)
        n += write(VAULT / "Claims" / f"{nid.split('.', 1)[-1].replace('.', '_')}.md",
                   {"id": nid, "type": "claim", "status": status, "domain": dom,
                    "title": title, "worth": worth, "ttl_days": ttl,
                    "source": src, "supports": supports,
                    "tested_by": tested_by, "guarded_by": guarded_by},
                   body, force)
    print(f"seeded {n} node(s) into {VAULT}")
    backfill(force)


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true",
                    help="overwrite existing nodes (discards hand edits)")
    main(ap.parse_args().force)
