"""Pin the contract between the sandbox and the real OASIS order engine.

Phase 0 of the extraction. Two jobs:

  1. Establish the EXACT enriched-SKU record the engine consumes. Not by reading
     the source and writing down what looks important - by wrapping the product
     dictionary in a recorder and running the real entry points over a matrix of
     representative SKUs, then reporting every key that was actually touched.
     Reading source misses fields set by one mixin and read by another; this
     does not.

  2. Snapshot `oasis_engines_config.json` with a revision hash, so a sandbox
     result can be attributed to a named configuration. Without this, a number
     that moved because someone toggled an engine is indistinguishable from a
     number that moved because a parameter was tuned.

Everything runs against a TEMPORARY data directory. Nothing is written into
oasis/data.

    python pin_contract.py            # regenerate the contract + config snapshot
    python pin_contract.py --check    # fail if production has drifted from the pin
"""
from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import sys
import tempfile
from datetime import datetime, timezone

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
OUT = os.path.join(HERE, "contract")
sys.path.insert(0, REPO)

CONFIG_SRC = os.path.join(REPO, "oasis", "data", "oasis_engines_config.json")


# ── the recorder ────────────────────────────────────────────────────────────
_MISSING = object()


class Recorder(dict):
    """A dict that remembers what was asked of it, and what it was let off with.

    The distinction that matters is not "was this key present in my fixture" -
    that only tells you about the fixture. It is whether the ENGINE supplied a
    fallback:

      `no_default` - read via p[k], or p.get(k) with nothing to fall back on.
                     The sandbox MUST supply these.
      `defaults`   - read via p.get(k, X). Optional, and X is recorded, so the
                     sandbox knows which fallback path it takes by omitting it.
      `written`    - keys the engine SET. Outputs, not inputs.
    """

    def __init__(self, data, log):
        super().__init__(data)
        self._log = log

    def __getitem__(self, k):
        self._log["read"].add(k)
        self._log["no_default"].add(k)          # p[k] raises when absent
        return super().__getitem__(k)

    def get(self, k, default=_MISSING):
        self._log["read"].add(k)
        if default is _MISSING:
            self._log["no_default"].add(k)
            return super().get(k)
        self._log["defaults"].setdefault(k, default)
        return super().get(k, default)

    def __contains__(self, k):
        self._log["read"].add(k)
        return super().__contains__(k)

    def __setitem__(self, k, v):
        self._log["written"].add(k)
        super().__setitem__(k, v)

    def copy(self):
        # the engine does `rec = p.copy()`; keep recording through the copy
        return Recorder(dict(self), self._log)


def new_log():
    return {"read": set(), "no_default": set(), "defaults": {}, "written": set()}


# ── representative SKUs, chosen to exercise every branch ────────────────────
# Each entry is (label, why it is here, fields). Between them these hit the
# daily-fresh JIT path, the long-life guardrail, the dry-goods cap, the
# lookalike/greenfield path and the missing-supplier fallback.
MATRIX = [
    ("daily fresh anchor", "hits the 1.2-day DAILY_FRESH_JIT branch", {
        "product_name": "BROOKSIDE 500ML DAIRY BEST (POUCH)",
        "avg_daily_sales": 727.3, "current_stocks": 800, "on_order_qty": 0,
        "estimated_delivery_days": 2, "supplier_name": "BROOKSIDE DAIRY LIMITED",
        "is_fresh": True, "median_gap_days": 1.0, "unit_cost": 59.01,
        "pack_size": 12, "department": "FRESH MILK"}),
    ("long life, keyword present", "hits the UHT/ESL/LONG LIFE guardrail", {
        "product_name": "RAVINE GOLD 500ML ESL UHT MILK",
        "avg_daily_sales": 14.1, "current_stocks": 40, "on_order_qty": 0,
        "estimated_delivery_days": 2, "supplier_name": "RAVINE DAIRIES LIMITED",
        "is_fresh": True, "median_gap_days": 4.0, "unit_cost": 41.77,
        "pack_size": 12, "department": "FRESH MILK"}),
    ("long life, keyword ABSENT", "the misclassification case", {
        "product_name": "TUZO 500ML WHOLE MILK (FINO PACK) 180DAYS",
        "avg_daily_sales": 83.5, "current_stocks": 90, "on_order_qty": 0,
        "estimated_delivery_days": 2, "supplier_name": "BROOKSIDE DAIRY LIMITED",
        "is_fresh": True, "median_gap_days": 1.0, "unit_cost": 57.99,
        "pack_size": 12, "department": "FRESH MILK"}),
    ("dry goods", "hits the cycle+lead+14 dynamic cap", {
        "product_name": "SOKO 2KG MAIZE MEAL",
        "avg_daily_sales": 48.6, "current_stocks": 300, "on_order_qty": 0,
        "estimated_delivery_days": 3, "supplier_name": "CAPWELL INDUSTRIES LTD",
        "is_fresh": False, "median_gap_days": 4.0, "unit_cost": 146.9,
        "pack_size": 6, "department": "FLOUR"}),
    ("slow mover", "hits the 0.8x low-velocity multiplier", {
        "product_name": "AMERICANO 90G ALMONDS SQUARE TIN",
        "avg_daily_sales": 0.4, "current_stocks": 6, "on_order_qty": 0,
        "estimated_delivery_days": 7, "supplier_name": "UNKNOWN SUPPLIER LTD",
        "is_fresh": False, "median_gap_days": 21.0, "unit_cost": 480.0,
        "pack_size": 4, "department": "CHOCOLATES"}),
    ("zero velocity", "greenfield / lookalike path", {
        "product_name": "NEW LISTING NO HISTORY 1L",
        "avg_daily_sales": 0.0, "current_stocks": 0, "on_order_qty": 0,
        "estimated_delivery_days": 7, "supplier_name": "UNKNOWN SUPPLIER LTD",
        "is_fresh": False, "median_gap_days": 0.0, "unit_cost": 100.0,
        "pack_size": 1, "department": "HOUSEHOLD ITEMS"}),
    ("minimal record", "what happens with almost nothing supplied", {
        "product_name": "BARE MINIMUM ITEM",
        "avg_daily_sales": 5.0}),
]


def temp_data_dir():
    """A throwaway data dir, seeded with the files the engine reads at init."""
    tmp = tempfile.mkdtemp(prefix="oasis_contract_")
    parent = os.path.join(tmp, "parent")
    data_dir = os.path.join(parent, "data")
    os.makedirs(data_dir, exist_ok=True)
    # The calendar resolves against data_dir/.. then cwd then data_dir. Copy it
    # in, or the schedule check runs against a calendar with zero suppliers.
    for f in ("supplier_rhythm_analysis.json", "supplier_weekly_schedule.json",
              "Supplier_Order_Calendar_2026.xlsx"):
        src = os.path.join(REPO, f)
        if os.path.exists(src):
            shutil.copy2(src, os.path.join(parent, f))
    # the engine reads its config from the installed package, not data_dir,
    # so nothing needs copying for that - but record where it came from
    return tmp, data_dir


def probe():
    from oasis.logic.order_engine import OrderEngine
    from oasis.logic.simulation_bridge import SimulationOrderUtil

    tmp, data_dir = temp_data_dir()
    eng = OrderEngine(data_dir)
    util = SimulationOrderUtil(data_dir, engine=eng)

    results = {"target_stock": new_log(), "order_quantity": new_log(),
               "order_pipeline": new_log()}
    observed = []

    for label, why, fields in MATRIX:
        # --- entry point 1: the target-stock calculation -------------------
        log = results["target_stock"]
        p = Recorder(dict(fields), log)
        try:
            td = eng.calculate_replenishment_target_stock(p, {})
        except Exception as e:
            td = f"ERROR: {type(e).__name__}: {e}"

        # --- entry point 2: the quantity decision, stage 2 alone -----------
        log2 = results["order_quantity"]
        p2 = Recorder(dict(fields), log2)
        try:
            recs = util.calculate_order_quantity([p2], store_config={}, current_day=1)
            qty = recs[0].get("recommended_quantity") if recs else None
            reason = (recs[0].get("reasoning") or "").strip() if recs else ""
        except Exception as e:
            qty, reason = f"ERROR: {type(e).__name__}: {e}", ""

        # --- entry point 3: the WHOLE live pipeline ------------------------
        # Stage 2 is not the store's answer. finalize_orders applies the aging
        # and dead-stock guards, the global 3x cap and pack rounding; the MOQ
        # gate then decides whether the line stays on the purchase order at
        # all. Fields those two stages read - cost_price, abc_rank, moq_floor,
        # last_days_since_last_delivery - are invisible to a pin that stops at
        # stage 2, which is why this pipeline log exists alongside it.
        log3 = results["order_pipeline"]
        p3 = Recorder(dict(fields), log3)
        try:
            raw = util.calculate_order_quantity([p3], store_config={}, current_day=1)
            q_raw = raw[0].get("recommended_quantity") if raw else None
            fin = util.finalize_orders(raw)
            q_fin = fin[0].get("recommended_quantity") if fin else None
            gate = util.apply_minimum_order_gate(list(fin))
            if gate.get("po_recs"):
                q_ord, fulfil = gate["po_recs"][0].get("recommended_quantity"), "PO"
            elif gate.get("transfer_recs"):
                q_ord, fulfil = 0, "TRANSFER_FIRST"
            else:
                q_ord, fulfil = 0, "NONE"
            reason3 = (fin[0].get("reasoning") or "").strip() if fin else ""
        except Exception as e:
            q_raw = q_fin = q_ord = f"ERROR: {type(e).__name__}: {e}"
            fulfil, reason3 = "ERROR", ""

        observed.append({
            "case": label, "why": why,
            "product_name": fields.get("product_name"),
            "target_days": td,
            "recommended_quantity": qty,
            "reasoning": reason[:160],
            "qty_raw": q_raw, "qty_finalized": q_fin,
            "ordered_quantity": q_ord, "fulfillment": fulfil,
            "pipeline_reasoning": reason3[:220],
        })

    shutil.rmtree(tmp, ignore_errors=True)
    return results, observed


#: The engine SOURCE, not just its configuration.
#:
#: The original pin hashed `oasis_engines_config.json` alone, on the assumption
#: that behaviour follows configuration. It does not. During the phase-4 work
#: another process added a LATA Shield term to `order_up_to.py` (+467 lines)
#: and `simulation_bridge.py` (+155) - and the config hash never moved, so
#: `--check` and the bridge's drift guard both said OK while 38 golden vectors
#: changed their ordered quantity and 11 changed fulfilment, one line by 90%.
#:
#: A pin that can be defeated by editing a .py file is not a pin. These are the
#: modules the ordering and transfer answers actually come out of.
ENGINE_SOURCES = (
    "oasis/logic/order_engine.py",
    "oasis/logic/intelligence_mixin.py",
    "oasis/logic/simulation_bridge.py",
    "oasis/logic/order_up_to.py",
    "oasis/logic/order_logic_guards.py",
    "oasis/logic/rounding.py",
    "oasis/logic/engines_config.py",
    "oasis/logic/lata_shield.py",
    "oasis/logic/residual_cover.py",
    "oasis/logic/consolidated_transfer_service.py",
    "oasis/logic/fulfillment_decider.py",
    "oasis/logic/transfer_state.py",
)


def engine_source_snapshot():
    """Per-file hashes plus one combined hash over all of them."""
    per, combined = {}, hashlib.sha256()
    for rel in ENGINE_SOURCES:
        path = os.path.join(REPO, rel.replace("/", os.sep))
        if not os.path.exists(path):
            per[rel] = None
            combined.update(f"{rel}:MISSING".encode())
            continue
        h = hashlib.sha256(open(path, "rb").read()).hexdigest()
        per[rel] = h
        combined.update(f"{rel}:{h}".encode())
    return {"files": per, "combined_sha256": combined.hexdigest()}


def config_snapshot():
    if not os.path.exists(CONFIG_SRC):
        return None
    raw = open(CONFIG_SRC, "rb").read()
    cfg = json.loads(raw.decode("utf-8"))
    engines = cfg.get("engines") or {}
    active = sorted(k for k, v in engines.items()
                    if (v.get("enabled") if isinstance(v, dict) else bool(v)))
    return {
        "source": os.path.relpath(CONFIG_SRC, REPO).replace("\\", "/"),
        "sha256": hashlib.sha256(raw).hexdigest(),
        "bytes": len(raw),
        "active_engines": active,
        "engine_sources": engine_source_snapshot(),
        "config": cfg,
    }


def build():
    os.makedirs(OUT, exist_ok=True)
    logs, observed = probe()
    snap = config_snapshot()

    contract = {
        "generated_utc": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "repo_relative": "devkit/retail_sandbox/contract",
        "entry_points": {},
        "observed_behaviour": observed,
        "engines_config": {k: v for k, v in (snap or {}).items() if k != "config"},
    }
    for name, log in logs.items():
        outputs = sorted(log["written"])
        required = sorted(log["no_default"] - log["written"])
        # a field read BOTH ways is required: some path has no fallback
        optional = {k: repr(v) for k, v in sorted(log["defaults"].items())
                    if k not in log["written"] and k not in set(required)}
        contract["entry_points"][name] = {
            "required_fields": required,
            "optional_fields": sorted(optional),
            "optional_defaults": optional,
            "fields_the_engine_sets": outputs,
        }

    with open(os.path.join(OUT, "engine_contract.json"), "w", encoding="utf-8") as f:
        json.dump(contract, f, indent=2, sort_keys=False, default=str)

    if snap:
        with open(os.path.join(OUT, "oasis_engines_config.snapshot.json"),
                  "w", encoding="utf-8") as f:
            json.dump(snap, f, indent=2)

    write_markdown(contract, snap)
    return contract, snap


def write_markdown(contract, snap):
    ep = contract["entry_points"]
    L = []
    L.append("# Engine contract\n\n")
    L.append("Generated by `pin_contract.py` on "
             f"{contract['generated_utc']}. **Do not hand-edit** - re-run the script.\n\n")
    L.append("This is what the real OASIS order engine actually reads, recorded by "
             "wrapping the product dictionary and running the live entry points "
             "over a matrix of representative SKUs. It is not a reading of the "
             "source, so it includes fields one mixin sets and another consumes.\n\n")

    if snap:
        L.append("## Pinned configuration\n\n")
        L.append(f"- Source: `{snap['source']}`\n")
        L.append(f"- SHA-256: `{snap['sha256']}`\n")
        L.append(f"- Active engines: {', '.join('`%s`' % e for e in snap['active_engines']) or '_none_'}\n\n")
        L.append("Any sandbox result should cite this hash. `pin_contract.py --check` "
                 "fails if production has moved away from it.\n\n")

    L.append("## The live path is four stages\n\n")
    L.append("Read off `oasis/desktop/data.py:327-369`, which is what the store runs:\n\n")
    L.append("| # | call | what it does |\n|---|---|---|\n")
    L.append("| 1 | `prepare_sku_data` | `reorder_point`, `target_coverage_days`, "
             "`safety_stock`, `demand_cv` |\n")
    L.append("| 2 | `calculate_order_quantity` | the ROP trigger and the net requirement |\n")
    L.append("| 3 | `finalize_orders` | aging and dead-stock guards, the global 3x cap "
             "(ADS x 63d), pack rounding |\n")
    L.append("| 4 | `apply_minimum_order_gate` | MOQ/MOP per SKU then MOT per supplier; "
             "failures routed to `TRANSFER_FIRST` |\n\n")
    L.append("A quantity read off stage 2 is **not** the number the store sees. Stage 3 "
             "can cap it and stage 4 can take the line off the purchase order entirely. "
             "The `order_pipeline` table below records all four; `order_quantity` records "
             "stage 2 alone and is kept only so the difference stays visible.\n\n")
    L.append("Production runs a fifth step between 3 and 4 - "
             "`ConsolidatedTransferService.optimize_network`. It was expected to "
             "subtract network stock from the PO before the MOQ gate, which would have "
             "made any single-store quantity an upper bound. **Measured, it does not.** "
             "The decider sizes a transfer at the stockout GAP "
             "(`transfer_target = min(gap_qty, shortfall_qty)`), so donors never cover a "
             "whole shortfall, so every decision is `BOTH` - \"order kept for buffer\" - "
             "and only the `TRANSFER` branch calls `_adjust_order(..., 0.0, ...)`. Across "
             "nine networks, including a store holding nothing with donors on a thousand "
             "days of cover, the purchase order was never reduced by one unit and "
             "`total_orders_reduced` was 0 every time.\n\n")
    L.append("So the network step is **additive**: it moves stock between branches and "
             "leaves the order alone. A single-store quantity is the same quantity a "
             "network run produces. See `golden_transfer.py` for the vectors.\n\n")

    for name in ("target_stock", "order_quantity", "order_pipeline"):
        d = ep.get(name) or {}
        fn = {"target_stock": "OrderEngine.calculate_replenishment_target_stock",
              "order_quantity": "SimulationOrderUtil.calculate_order_quantity (stage 2 only)",
              "order_pipeline": "the full pipeline - stages 2, 3 and 4"}[name]
        L.append(f"## `{fn}`\n\n")
        L.append(f"**Required** - read with no fallback, so the sandbox must supply them "
                 f"({len(d.get('required_fields', []))}):\n\n")
        L.append("".join(f"- `{k}`\n" for k in d.get("required_fields", [])) or "- _none_\n")
        defs = d.get("optional_defaults", {})
        L.append(f"\n**Optional** - the engine supplies a fallback ({len(defs)}). "
                 "Omitting one is safe, but it means the sandbox takes the fallback "
                 "path rather than the real one - so every omission is a place the "
                 "two implementations can quietly diverge.\n\n")
        L.append("| field | engine falls back to |\n|---|---|\n")
        L.append("".join(f"| `{k}` | `{v}` |\n" for k, v in defs.items())
                 or "| _none_ | |\n")
        L.append(f"\n**Set by the engine** - outputs, not inputs "
                 f"({len(d.get('fields_the_engine_sets', []))}):\n\n")
        L.append("".join(f"- `{k}`\n" for k in d.get("fields_the_engine_sets", [])) or "- _none_\n")
        L.append("\n")

    L.append("## How to read the tables above\n\n")
    L.append("Two caveats the recorder cannot infer for itself.\n\n")
    L.append("**A recorded default may be the result of a nested lookup.** "
             "`current_stock` appears to default to `800`, but the code is "
             "`p.get('current_stock', p.get('current_stocks', 0))` - Python "
             "evaluates the inner lookup first, so the recorder sees whatever "
             "the *alias* returned. Treat any default that happens to equal "
             "another supplied field as an alias chain, not a constant.\n\n")
    L.append("**Known alias:** stock-on-hand may be supplied as either "
             "`current_stocks` (plural) or `current_stock` (singular). "
             "Enrichment does `p['current_stock'] = p.get('current_stocks', 0)`, "
             "so the sandbox should supply the **plural** and let enrichment "
             "derive the singular. Supplying only the singular skips a step "
             "enrichment expects to perform.\n\n")
    L.append("### The engine never refuses a bad record\n\n")
    L.append("`calculate_order_quantity` has **zero required fields** - every "
             "lookup has a fallback. Feed it an empty dictionary and it returns "
             "a confident number rather than an error.\n\n")
    L.append("That is the single most important thing on this page for anyone "
             "wiring the sandbox up. A missing or misspelled field does not "
             "fail; it silently changes which branch the engine takes. The "
             "`reasoning` string is the only signal, which is why the golden "
             "vectors in phase 3 should assert on it and not just on the "
             "quantity.\n\n")
    L.append("## Observed behaviour on the probe matrix\n\n")
    L.append("Each stage's answer for the same record, so the cost of stopping early "
             "is on the page rather than in someone's head.\n\n")
    L.append("| Case | Product | target_days | stage 2 | stage 3 | ordered | fulfilment |\n"
             "|---|---|---|---|---|---|---|\n")
    for o in contract["observed_behaviour"]:
        L.append(f"| {o['case']} | `{o['product_name']}` | {o['target_days']} | "
                 f"{o.get('qty_raw')} | {o.get('qty_finalized')} | "
                 f"{o.get('ordered_quantity')} | {o.get('fulfillment')} |\n")
    L.append("\n### Why each line moved\n\n")
    for o in contract["observed_behaviour"]:
        r = (o.get("pipeline_reasoning") or "").replace("|", "/")
        L.append(f"- **{o['case']}** - {r}\n")
    L.append("\nA `reasoning` string containing **ROP Fallback** means the record was not "
             "enriched enough to take the real path; that is what phase 2 closed. A line "
             "reaching `TRANSFER_FIRST` did not fail - the engine decided the order was "
             "too small to be worth a delivery and expects the network to supply it.\n")

    with open(os.path.join(OUT, "CONTRACT.md"), "w", encoding="utf-8") as f:
        f.write("".join(L))


def check():
    path = os.path.join(OUT, "oasis_engines_config.snapshot.json")
    if not os.path.exists(path):
        print("no snapshot pinned yet - run without --check first")
        return 1
    pinned = json.load(open(path, encoding="utf-8"))
    live = config_snapshot()
    if live is None:
        print("FAIL: live config not found at", CONFIG_SRC)
        return 1
    # ── engine SOURCE, checked before the config ──────────────────────────
    #
    # Checked first because it is the one that actually bit. The config hash
    # can sit unchanged while a .py file moves the answers underneath it.
    pinned_src = (pinned.get("engine_sources") or {})
    live_src = live["engine_sources"]
    src_ok = pinned_src.get("combined_sha256") == live_src["combined_sha256"]
    if not pinned_src:
        print("NOTE: this pin predates engine-source hashing. Re-run "
              "`python pin_contract.py` to pin the source too.")
    elif not src_ok:
        moved, added, removed = [], [], []
        for rel, h in live_src["files"].items():
            was = pinned_src["files"].get(rel, "__absent__")
            if was == "__absent__":
                added.append(rel)
            elif was != h:
                moved.append(rel)
        for rel in pinned_src["files"]:
            if rel not in live_src["files"]:
                removed.append(rel)
        print("DRIFT: the ENGINE SOURCE has changed since the pin.")
        for rel in moved:
            print("  modified:", rel)
        for rel in added:
            print("  newly pinned:", rel)
        for rel in removed:
            print("  no longer present:", rel)
        print("\n  The engines config may be untouched and still say OK - it did,")
        print("  the day a LATA Shield term was added to order_up_to.py and 38")
        print("  golden vectors changed their ordered quantity. Re-run")
        print("  `python golden.py --check` to see what the change did, then")
        print("  re-baseline both deliberately.")
        return 2

    if live["sha256"] == pinned["sha256"]:
        print("OK  engines config and engine source both match the pin")
        print("    config sha256", pinned["sha256"][:16], "|",
              ", ".join(pinned["active_engines"]))
        print("    source sha256", live_src["combined_sha256"][:16], "|",
              f"{len(live_src['files'])} modules")
        return 0
    print("DRIFT: the live engines config no longer matches the pin.")
    print("  pinned sha256 ", pinned["sha256"])
    print("  live   sha256 ", live["sha256"])
    a, b = set(pinned["active_engines"]), set(live["active_engines"])
    if a != b:
        if b - a:
            print("  engines turned ON since the pin :", ", ".join(sorted(b - a)))
        if a - b:
            print("  engines turned OFF since the pin:", ", ".join(sorted(a - b)))
    else:
        print("  active engines unchanged; some other setting moved")
    print("\n  Any sandbox result produced against the old pin is not comparable")
    print("  with one produced now. Re-run pin_contract.py and re-baseline.")
    return 2


def main():
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--check", action="store_true",
                    help="fail if production config has drifted from the pin")
    a = ap.parse_args()
    if a.check:
        raise SystemExit(check())

    contract, snap = build()
    print("wrote", os.path.relpath(OUT, REPO).replace("\\", "/") + "/")
    print("  engine_contract.json")
    print("  oasis_engines_config.snapshot.json")
    print("  CONTRACT.md")
    print()
    for name, d in contract["entry_points"].items():
        print(f"  {name:16s} required {len(d['required_fields']):3d}   "
              f"optional {len(d['optional_fields']):3d}   "
              f"set-by-engine {len(d['fields_the_engine_sets']):3d}")
    if snap:
        print()
        print("  config pinned at sha256", snap["sha256"][:16])
        print("  active engines:", ", ".join(snap["active_engines"]) or "(none)")


if __name__ == "__main__":
    main()
