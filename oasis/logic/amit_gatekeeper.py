"""
AMIT — Assortment & Margin Integration Tool (Pre-Flight Engine)
Chapter 11, Sub-Routine A: The Gatekeeper.

Prevents "Parasite" suppliers and "Cannibal" SKUs from re-infecting the store.
Enforces One-In, One-Out rule based on GMROI ranking per department.

Usage:
    python -m oasis.logic.amit_gatekeeper --data-dir ./oasis/data --nn-path ./neutral_network_export

Output:
    oasis/data/amit_enforcement.json

------------------------------------------------------------------------------
2026-09 REWRITE. See devkit/amit_return.py and devkit/probe_amit_enforcement.py
for the full derivation; this is the summary that matters for anyone reading
this file cold.

THE OLD KEY WAS ALGEBRAICALLY EMPTY
    calculate_gmroi() computed gross_profit / (price * ads * 30 * lata).
    Expand gross_profit = price * ads * T * margin and price and ads cancel
    EXACTLY, leaving (T/30) * margin / lata -- a margin sort wearing a GMROI
    costume, with no velocity and no capital in it anywhere. Worse: nodes.csv's
    own `gross_profit` column was populated on only 3,663 of 23,511 rows, so
    19,848 SKUs (84%) scored exactly 0.0, tied, and Python's stable sort
    resolved every tie by CSV ROW ORDER -- 96.4% of all blacklistings came out
    of that tie block. A ranking whose result depends on input row order is
    not a ranking. (Separately: of the 3,663 rows that DID carry a
    `gross_profit`, 88.5% sit at margin_pct ~= 16.0%, which is this store's
    VAT rate, not a plausible gross margin against a chain median of 25%
    ex-VAT -- a third, independent contamination of that column. See
    resolve_margin() below: node-level margin/gross_profit fields are no
    longer used at all.)

THE FIX: two keys, for two different constraints, never conflated
    annual_gross_profit  KES of gross profit per year. This answers a
                          PER-LINE cap: a facing, a receiving line, a count, a
                          rotation, a planogram slot cost the same whether the
                          SKU sitting on them turns over fast or slow. Category
                          caps in this file are literally SKU-COUNT caps, so
                          annual_gross_profit is what decides which line is
                          "lowest" for the cap trim / One-In-One-Out swap.
    gmroi                 Gross profit per KES-year of inventory the ordering
                          policy commits. This answers a CAPITAL budget
                          constraint. Reported per SKU and per department, but
                          NOT used to rank the cap trim: devkit/amit_return.py
                          found the capital constraint does not bind at this
                          store (portfolio GMROI ~17.85 against a ~26% hurdle
                          of capital + handling + shrink) -- a gate on GMROI
                          only cuts the ~36 lines that lose money outright.
    Margin comes from the VAT-corrected GRN book (oasis/data/margin_from_grn.json,
    built by devkit/build_margin.py) wherever a SKU matches it; the join rate
    is reported in stats.join (T1), and a chain-median fallback is used,
    TAGGED per SKU, for the remainder -- never a silent zero. Inventory comes
    from calling order_up_to.recommend() (measured review cadence, measured
    PO-to-GRN lead time and spread, shelf-life clamp) instead of the private
    "price * ads * 30" proxy -- that proxy is exactly the term that cancelled
    out of the old key.

CAPS NOW REACH THE DATA
    DEFAULT_DEPT_CAPS_BASELINE names ~26 categories by hand; nodes.csv carries
    239 raw department spellings, so most SKUs fell through to the undeclared
    DEFAULT_CAP_FALLBACK_BASELINE = 50 -- which WAS the real assortment policy
    for the bulk of the catalogue, unlabelled as such. norm_dept() (ported
    from devkit/amit_adaptive.py -- this engine may not import devkit) folds
    spelling/alias variants ("STATIONARIES" -> "STATIONERY", "FRESH MILK" ->
    "DAIRY", ...) onto the declared categories so a cap someone actually wrote
    has a chance of applying to the department it was written for. merge_caps()
    reports any collisions the fold produces.

STRUCTURALLY SHORT LINES ARE PROTECTED FROM THE CAP TRIM
    A SKU whose order-up-to level cannot clear its own review-plus-lead window
    without exceeding its shelf life is not failing an assortment test, it is
    failing a supply-terms test (see order_up_to.recommend()'s "feasible"
    flag). Cutting it for having a low GP earned in an exposure window it was
    never allowed to fill is not a merchandising judgement, so these lines
    sort ahead of the cap line regardless of their own economics; see the
    sort key in run_amit().
------------------------------------------------------------------------------
"""

import csv
import json
import os
import random
import logging
import argparse
from collections import defaultdict
from datetime import datetime, timezone
from typing import Dict, List, Any, Optional, Tuple

logger = logging.getLogger("OASIS.AMIT")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# Default department SKU caps (can be overridden via config)
DEFAULT_DEPT_CAPS_BASELINE = {
    "WINES": 120, "SPIRITS": 80, "BEER": 100, "DIWALI ITEMS": 30,
    "PARTY ITEMS": 40, "AIR FRESHNERS": 50, "BISCUITS": 80, "SNACKS": 100,
    "CONFECTIONERY": 120, "BEVERAGES": 150, "CEREALS": 60, "COOKING OIL": 40,
    "RICE": 30, "SUGAR": 20, "FLOUR": 30, "FRESH MILK": 40, "DAIRY": 60,
    "BREAD": 30, "BAKERY": 50, "HOUSEHOLD": 150, "TOILETRIES": 150,
    "DETERGENT": 80, "BABY CARE": 60, "STATIONERY": 50, "COSMETICS": 100,
    "PET FOOD": 30,
}

# Reference constants (will be updated from config in run_amit)
#: A blanket cap for departments nobody has written a number for. OFF unless
#: declared: see the comment at the cap lookup in run_amit().
_FALLBACK_DECLARED = bool(os.getenv("OASIS_AMIT_FALLBACK_CAP"))
#: Share of LIVE-DEMAND revenue this engine may block before it refuses to
#: publish. A gatekeeper that shuts a fifth of the shop is not a gatekeeper,
#: it is an outage, and it should not be able to become one silently by way of
#: a caps table nobody re-read. Override deliberately, never by accident.
MAX_BLOCKED_REVENUE_SHARE = float(os.getenv("OASIS_AMIT_MAX_BLOCK_SHARE", "0.10"))

BASELINE_BUDGET = 10_000_000
MIN_DEPT_CAP_FLOOR = 5
DEFAULT_CAP_FALLBACK_BASELINE = 50

# --- key-fix constants ------------------------------------------------------
MARGIN_CACHE_FILENAME = "margin_from_grn.json"      # devkit/build_margin.py
ADS_CACHE_FILENAME = "corrected_ads_from_pos.json"
# Chain-wide median gross margin, EX-VAT, from the GRN book (build_margin.py's
# natural-experiment finding). Used ONLY when a SKU has no GRN-book match at
# all, so it competes on a plausible number instead of tying at an unlabelled
# zero. node-level margin_pct/gross_profit are deliberately NOT used as an
# intermediate fallback tier: 88.5% of the populated ones sit at ~16.0%, this
# store's VAT rate, not a margin (see module docstring).
FALLBACK_MARGIN_PCT_EX_VAT = 0.25
ASSUMED_VAT_RATE_FOR_FALLBACK = 0.16   # to strip VAT off node["price"] first
# A consumer of amit_enforcement.json should refuse or loudly warn past this.
AMIT_MAX_AGE_DAYS = 14


def _load_amit_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load the whole central config (AMIT reads engines + category rules).

    Resolved via oasis.logic.engines_config, so an install with no tuned
    oasis_engines_config.json picks up the SHIPPED defaults rather than an
    empty dict (deep-analysis finding S1).
    """
    from .engines_config import load_engines_config
    return load_engines_config(data_dir)


def norm(x: Any) -> str:
    """Ported from devkit/amit_adaptive.py (this engine must not import
    devkit): case/whitespace normalization used to key every join below."""
    return " ".join(str(x).upper().split())


# Ported from devkit/amit_adaptive.py -- see module docstring "CAPS NOW REACH
# THE DATA". Keep in sync with that file if the alias table there grows.
_DEPT_ALIAS = {
    "STATIONARIES": "STATIONERY", "STATIONARY": "STATIONERY",
    "SWEETS CHOCOLATES": "CONFECTIONERY", "SWEETS": "CONFECTIONERY",
    "CHOCOLATES": "CONFECTIONERY", "YOGHURT": "DAIRY",
    "FRESH MILK": "DAIRY", "UHT MILK": "DAIRY", "CHEESE": "DAIRY",
    "SHAMPOOS CONDITIONER": "TOILETRIES", "BATH SOAP": "TOILETRIES",
    "MENS SHOWER GEL": "TOILETRIES", "WOMEN UNISEX SHOWER GEL": "TOILETRIES",
    "PET DOG FOOD": "PET FOOD", "PET CAT FOOD": "PET FOOD",
    "PET FISH FOOD": "PET FOOD", "PET ASSESORIES": "PET FOOD",
    "BREAD": "BAKERY", "CAKES": "BAKERY",
}
_DEPT_STRIP_WORDS = r"\b(ITEMS?|PRODUCTS?|MISC|GENERAL|LOCAL|OTHERS?)\b"


def norm_dept(d: Any) -> str:
    """Collapse a raw department spelling onto the canonical category a cap
    was actually written for. Many-to-one: 'FRESH MILK' and 'DAIRY' both
    become 'DAIRY'. See merge_caps() for how caps survive that collapse."""
    import re
    s = norm(d).strip("[]").strip()
    s = re.sub(r"[^A-Z0-9 ]", " ", s)
    s = re.sub(_DEPT_STRIP_WORDS, " ", s)
    s = re.sub(r"\s+", " ", s).strip()
    return _DEPT_ALIAS.get(s, s)


def merge_caps(caps: Dict[str, int]) -> Tuple[Dict[str, int], List[Tuple[str, int, str, int]]]:
    """norm_dept is many-to-one. A dict comprehension over raw keys would keep
    only the LAST write and lose the rest silently. Merge explicitly (keep the
    max) and report every collision so a divergent pair of caps is visible
    rather than quietly resolved."""
    out: Dict[str, int] = {}
    collisions: List[Tuple[str, int, str, int]] = []
    for k, v in caps.items():
        nk = norm_dept(k)
        if nk in out and out[nk] != v:
            collisions.append((nk, out[nk], k, v))
        out[nk] = max(out.get(nk, 0), v)
    return out, collisions


def load_nodes(nn_path: str) -> List[Dict[str, Any]]:
    """Load all SKU nodes from the neural network export."""
    nodes_path = os.path.join(nn_path, "nodes.csv")
    if not os.path.exists(nodes_path):
        logger.error(f"nodes.csv not found at {nodes_path}")
        return []

    nodes = []
    with open(nodes_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("type") != "SKU":
                continue
            # Clean department name (remove [[ ]] wrappers)
            dept = row.get("department", "GENERAL").strip("[]").strip().upper()

            nodes.append({
                "id": row["id"],
                "department": dept,
                "supplier": row.get("supplier", "Unknown").strip("[]").strip().upper(),
                "price": float(row.get("price", 0) or 0),
                "margin_pct": float(row.get("margin_pct", 0) or 0),
                "revenue": float(row.get("revenue", 0) or 0),
                "gross_profit": float(row.get("gross_profit", 0) or 0),
                "sales_rank": float(row.get("sales_rank", 99999) or 99999),
                "velocity_ads": float(row.get("velocity_ads", 0) or 0),
                "total_quantity": float(row.get("total_quantity", 0) or 0),
                "store_fill_rate": float(row.get("store_fill_rate", row.get("rhapta_fill_rate", 0)) or 0),
            })

    logger.info(f"Loaded {len(nodes)} SKU nodes from neural network.")
    return nodes


def load_lata_patterns(data_dir: str) -> Dict[str, float]:
    """Load LATA safety multipliers to adjust GMROI rankings."""
    path = os.path.join(data_dir, "supplier_patterns_2025.json")
    if not os.path.exists(path):
        logger.warning("supplier_patterns_2025.json not found. LATA weighting disabled.")
        return {}

    with open(path, "r", encoding="utf-8") as f:
        patterns = json.load(f)

    return {s: d.get("lata_variance_multiplier", 1.0) for s, d in patterns.items() if isinstance(d, dict)}


def load_margin_book(data_dir: str) -> Dict[str, Dict[str, Any]]:
    """VAT-corrected margin per SKU, from devkit/build_margin.py's cache.
    Keys normalized with norm() so the join is case/whitespace-insensitive.
    Missing file -> {} (every SKU falls to the chain fallback; reported)."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(data_dir)))
    for cand in (os.path.join(data_dir, MARGIN_CACHE_FILENAME),
                 os.path.join(root, "oasis", "data", MARGIN_CACHE_FILENAME)):
        if os.path.exists(cand):
            with open(cand, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return {norm(k): v for k, v in raw.items() if isinstance(v, dict)}
    logger.warning(f"{MARGIN_CACHE_FILENAME} not found -- every SKU falls back "
                   f"to the {FALLBACK_MARGIN_PCT_EX_VAT:.0%} chain-median margin.")
    return {}


def load_ads_book(data_dir: str) -> Dict[str, Dict[str, Any]]:
    """Corrected average-daily-sales per SKU (POS-derived; used here only to
    exercise the ranking machinery, never to validate a real-world figure --
    every POS database in this install is synthetic). Missing file -> {}
    (every SKU falls back to nodes.csv's own velocity_ads; reported)."""
    root = os.path.dirname(os.path.dirname(os.path.abspath(data_dir)))
    for cand in (os.path.join(data_dir, ADS_CACHE_FILENAME),
                 os.path.join(root, "oasis", "data", ADS_CACHE_FILENAME)):
        if os.path.exists(cand):
            with open(cand, "r", encoding="utf-8") as f:
                raw = json.load(f)
            return {norm(k): v for k, v in raw.items() if isinstance(v, dict)}
    logger.warning(f"{ADS_CACHE_FILENAME} not found -- every SKU falls back to "
                   "nodes.csv's own velocity_ads.")
    return {}


def resolve_margin(node: Dict[str, Any], margin_book: Dict[str, Dict[str, Any]],
                    stats: Dict[str, int]) -> Tuple[float, float, str]:
    """(unit_cost, gross_profit_per_unit, provenance). Two tiers only:
      'grn_book'       the VAT-corrected GRN-book observation (preferred).
      'chain_fallback' no GRN-book match: apply the chain median 25% ex-VAT
                       margin to the node's own price (assumed VAT-inclusive,
                       per build_margin.py's finding), so the SKU competes
                       with a labelled estimate instead of a silent zero.
    node-level gross_profit/margin_pct are never used -- see module docstring.
    """
    row = margin_book.get(norm(node["id"]))
    if row and row.get("unit_cost") is not None and row.get("gross_profit_per_unit") is not None:
        stats["grn_book"] = stats.get("grn_book", 0) + 1
        return float(row["unit_cost"]), float(row["gross_profit_per_unit"]), "grn_book"

    stats["chain_fallback"] = stats.get("chain_fallback", 0) + 1
    price = node["price"]
    sp_ex = price / (1.0 + ASSUMED_VAT_RATE_FOR_FALLBACK)
    gp_per_unit = sp_ex * FALLBACK_MARGIN_PCT_EX_VAT
    unit_cost = sp_ex - gp_per_unit
    return unit_cost, gp_per_unit, "chain_fallback_25pct_exvat"


def resolve_ads(node: Dict[str, Any], ads_book: Dict[str, Dict[str, Any]],
                 stats: Dict[str, int]) -> Tuple[float, str]:
    """(avg_daily_sales, provenance). Prefers the POS-corrected figure
    (new_ads, then old_ads); falls back to nodes.csv's own velocity_ads."""
    row = ads_book.get(norm(node["id"]))
    if row:
        d = float(row.get("new_ads") or row.get("old_ads") or 0)
        if d > 0:
            stats["pos_corrected"] = stats.get("pos_corrected", 0) + 1
            return d, "pos_corrected"
    stats["node_velocity_fallback"] = stats.get("node_velocity_fallback", 0) + 1
    return float(node["velocity_ads"] or 0), "node_velocity_fallback"


def calculate_dynamic_caps(baseline_caps: Dict[str, int], total_budget: float, baseline_budget: float, min_floor: int) -> Dict[str, int]:
    """Scale department caps based on total budget vs baseline budget."""
    scaling_factor = (total_budget / baseline_budget) ** 0.5  # Sub-linear scaling

    dynamic_caps = {}
    for dept, cap in baseline_caps.items():
        dynamic_caps[dept] = max(min_floor, int(cap * scaling_factor))

    return dynamic_caps


def run_amit(nn_path: str, data_dir: str, dept_caps: Dict[str, int] = None,
             total_budget: float = None, node_order_seed: Optional[int] = None) -> Dict[str, Any]:
    """Execute the AMIT Gatekeeper logic.

    node_order_seed: if given, shuffle the loaded node list with this seed
    before scoring. Exists so a probe can measure whether the final blacklist
    depends on nodes.csv row order (it must not) -- see
    devkit/probe_amit_enforcement.py's shuffle test.
    """
    nodes = load_nodes(nn_path)
    if not nodes:
        return {"stats": {"total_blacklisted": 0, "departments_over_cap": 0}}

    if node_order_seed is not None:
        random.Random(node_order_seed).shuffle(nodes)

    lata_multipliers = load_lata_patterns(data_dir)
    logger.info(f"Loaded {len(lata_multipliers)} LATA patterns for risk-weighting.")

    margin_book = load_margin_book(data_dir)
    ads_book = load_ads_book(data_dir)
    margin_stats: Dict[str, int] = {}
    ads_stats: Dict[str, int] = {}

    # order_up_to lives in oasis.logic (this package), not devkit -- importing
    # it is fine. Deferred, matching this file's existing style for
    # config-loading imports (avoids relative-import issues if ever run as a
    # bare script rather than `python -m`).
    from . import order_up_to as ou
    repo_root = os.path.dirname(os.path.dirname(os.path.abspath(data_dir)))
    review_schedule = ou.load_review_schedule(repo_root)
    lead_patterns = ou.default_patterns(repo_root)
    ou.load_shelf_life(repo_root)  # populate the module cache shelf_life_for() reads

    # Load Central Config for AMIT settings
    config = _load_amit_config(data_dir)
    amit_conf = config.get("engines", {}).get("amit", {})

    baseline_budget = amit_conf.get("baseline_budget", BASELINE_BUDGET)
    min_floor = amit_conf.get("min_dept_cap_floor", MIN_DEPT_CAP_FLOOR)
    base_caps = amit_conf.get("default_dept_caps_baseline", DEFAULT_DEPT_CAPS_BASELINE).copy()

    category_rules = config.get('category_rules', {})
    for dept, rule in category_rules.items():
        if dept in base_caps:
            boost = rule.get("boost", 1.0)
            base_caps[dept] = int(base_caps[dept] * boost)
            logger.info(f"[AMIT] Config: Boosting {dept} cap by {boost}x.")

    if dept_caps:
        caps = dept_caps
    elif total_budget:
        caps = calculate_dynamic_caps(base_caps, total_budget, baseline_budget, min_floor)
    else:
        caps = base_caps

    # CAPS NOW REACH THE DATA (see module docstring): fold raw department
    # spellings onto the categories these caps actually name, and report any
    # collisions the fold produces (T1-adjacent: a silent merge is a silent
    # policy change).
    # CAPS ARE APPLIED ON THE RAW DEPARTMENT NAME.
    # norm_dept() folds YOGHURT, FRESH MILK, UHT MILK, CHEESE and CREAM onto
    # DAIRY, and BREAD and CAKES onto BAKERY. That is the right move for
    # COMPARING a 16-row caps table against 245 real departments, and the
    # wrong move for ENFORCING it: the 60 somebody wrote next to "DAIRY" was
    # a number for one department, and applying it to the union of five turns
    # a 500-SKU dairy hall into a 60-SKU one. Folding the names made the caps
    # reachable and, in the same stroke, lethal -- 3,724 live lines carrying
    # KES 74.3m of annual revenue newly blocked, most of it yoghurt, cheese,
    # bacon and sausage.
    #
    # So: enforce on the raw name, report on the folded one.
    caps_folded, cap_collisions = merge_caps(caps)
    caps = {" ".join(str(k).upper().split()): v for k, v in caps.items()}
    for nk, old, raw_k, new in cap_collisions:
        logger.warning(f"[AMIT] Cap collision folding '{raw_k}' -> '{nk}': "
                       f"kept max({old}, {new}) = {max(old, new)}.")

    fallback_cap = DEFAULT_CAP_FALLBACK_BASELINE
    if total_budget:
        scaling_factor = (total_budget / baseline_budget) ** 0.5
        fallback_cap = max(min_floor, int(DEFAULT_CAP_FALLBACK_BASELINE * scaling_factor))

    dept_skus: Dict[str, List[Dict]] = defaultdict(list)
    raw_depts_seen = set()
    for node in nodes:
        dept_raw = node["department"]
        raw_depts_seen.add(dept_raw)
        dept = " ".join(str(dept_raw).upper().split())
        supplier = node["supplier"].upper()

        # Get LATA multiplier or default to 1.0 (Neutral)
        multiplier = lata_multipliers.get(supplier, 1.0)

        unit_cost, gp_per_unit, margin_prov = resolve_margin(node, margin_book, margin_stats)
        ads_val, ads_prov = resolve_ads(node, ads_book, ads_stats)

        lp = lead_patterns.get(supplier) or {}
        lead_days = float(lp.get("lead_time_mean", lp.get("lead_time_days", 3.0)) or 3.0)

        structurally_short = False
        if ads_val > 0:
            rec = ou.recommend({
                "avg_daily_sales": ads_val, "supplier_name": supplier,
                "current_stock": 0, "lead_time_days": lead_days,
                "department": dept_raw,
            }, schedule=review_schedule, patterns=lead_patterns)
            if "S" in rec:
                avg_units = rec["S"] - ads_val * (rec["L"] + rec["R"] / 2.0)
                structurally_short = avg_units <= 0
                if structurally_short:
                    avg_units = rec["S"] / 2.0
            else:
                avg_units = 0.0
        else:
            avg_units = 0.0

        inventory_value = max(avg_units * unit_cost, 1e-9)
        annual_gross_profit = gp_per_unit * ads_val * 365.0
        gmroi = annual_gross_profit / inventory_value
        # LATA risk penalty: an erratic supplier traps more capital to hold
        # the same service level, which lowers its effective GMROI.
        if multiplier:
            gmroi = gmroi / multiplier

        node["department"] = dept
        node["department_raw"] = dept_raw
        node["annual_gross_profit"] = annual_gross_profit
        # annualised revenue at the SAME ads and price the GP used, so the
        # blast-radius denominator below is on one basis with the numerator
        node["revenue_year"] = (float(gp_per_unit) + float(unit_cost)) * ads_val * 365.0
        node["gmroi"] = gmroi
        node["structurally_short"] = structurally_short
        node["lata_multiplier"] = multiplier
        node["margin_provenance"] = margin_prov
        node["ads_provenance"] = ads_prov
        dept_skus[dept].append(node)

    blacklist = []
    lowest_gmroi_per_dept = {}
    dept_stats = {}

    for dept, skus in dept_skus.items():
        # AN UNSET CAP IS NOT A CAP OF 50.
        # `fallback_cap` blocked every department nobody had written a number
        # for -- 219 of 245 of them -- and that undeclared literal WAS the
        # assortment policy for 90% of the store. A department absent from the
        # caps table has not been told it has a limit; it has been told
        # nothing, and the engine's own review_period() makes exactly this
        # argument about suppliers absent from the order calendar. Absence of
        # a declared limit is not a declaration.
        #
        # Set OASIS_AMIT_FALLBACK_CAP to reinstate a blanket cap deliberately.
        cap = caps.get(dept)
        if cap is None:
            cap = fallback_cap if _FALLBACK_DECLARED else None
        if cap is None:
            dept_stats[dept] = {"total_skus": len(skus), "cap": None, "over_cap": 0,
                                "uncapped": True}
            if skus:
                skus.sort(key=lambda x: (x["structurally_short"],
                                        x["annual_gross_profit"]), reverse=True)
                worst = skus[-1]
                lowest_gmroi_per_dept[dept] = {
                    "sku": worst["id"],
                    "annual_gross_profit": round(worst.get("annual_gross_profit", 0.0), 2),
                    "gmroi": round(worst.get("gmroi", 0.0), 4),
                    "sales_rank": worst.get("sales_rank", 99999)}
            continue

        # RANKING KEY: annual_gross_profit, not gmroi -- see module docstring
        # ("THE FIX"). A department's SKU-count cap is a per-line constraint,
        # so the shadow price is KES of GP per line, not KES per KES-year of
        # capital. Structurally-short lines (can't clear their own supply
        # window without exceeding shelf life) are protected from the trim
        # regardless of their own GP: (True, ...) sorts before (False, ...)
        # under reverse=True, so they never land in the cut tail unless a
        # department is nothing BUT structurally-short lines.
        skus.sort(key=lambda x: (x["structurally_short"], x["annual_gross_profit"]), reverse=True)

        dept_stats[dept] = {
            "total_skus": len(skus),
            "cap": cap,
            "over_cap": max(0, len(skus) - cap),
        }

        if len(skus) > cap:
            # Keep the top `cap` SKUs, blacklist the rest
            rejects = skus[cap:]

            for rej in rejects:
                multiplier = rej["lata_multiplier"]
                gp = rej["annual_gross_profit"]
                gmroi = rej["gmroi"]

                reason = f"AMIT Trimming: Exceeds {dept} category cap ({len(skus)}/{cap})."

                if multiplier > 1.0:
                    reason += f" [LOGISTICAL RISK: {multiplier}x penalty applied to inventory cost]."
                elif multiplier < 1.0:
                    reason += f" [LOGISTICAL ALPHA: {multiplier}x bonus for high reliability]."

                reason += (f" Annual GP: KES {gp:,.0f} (per-line cap key) - "
                          f"GMROI: {gmroi:.2f} (capital-budget key, not used to rank this cap)"
                          f" - margin src={rej['margin_provenance']} ads src={rej['ads_provenance']}")

                blacklist.append({
                    "sku": rej["id"],
                    "department": dept,
                    "department_raw": rej["department_raw"],
                    "annual_gross_profit": round(gp, 2),
                    "gmroi": round(gmroi, 4),
                    "lata_multiplier": multiplier,
                    "structurally_short": rej["structurally_short"],
                    "margin_provenance": rej["margin_provenance"],
                    "ads_provenance": rej["ads_provenance"],
                    "reason": reason,
                })

            logger.info(f"[AMIT] {dept}: {len(skus)} SKUs -> Cap {cap} -> Blacklisted {len(rejects)} items.")
        else:
            logger.debug(f"[AMIT] {dept}: {len(skus)} SKUs within cap ({cap}).")

        # Record the lowest-priority SKU in every department (for One-In-One-Out
        # swaps). NEVER a silent {} -- if a department has no SKUs this key
        # would simply be absent, which is explicit and checkable, unlike
        # amit_governance.py's old `'lowest_gmroi_per_dept': {}` placeholder.
        if skus:
            # The true "worst" incumbent for a swap is the lowest-ranked
            # non-structurally-short SKU where one exists (see the sort
            # above); only fall back to a short line if the whole department
            # is short.
            worst = skus[-1]
            lowest_gmroi_per_dept[dept] = {
                "sku": worst["id"],
                "annual_gross_profit": round(worst["annual_gross_profit"], 2),
                "gmroi": round(worst["gmroi"], 4),
                "sales_rank": worst["sales_rank"],
                "structurally_short": worst["structurally_short"],
                "ranked_by": "annual_gross_profit",
            }

    # Build the flat blacklist set for O(1) lookups in the engine
    blacklist_set = [item["sku"] for item in blacklist]

    nodes_path = os.path.join(nn_path, "nodes.csv")
    graph_generated_at = None
    if os.path.exists(nodes_path):
        graph_generated_at = datetime.fromtimestamp(
            os.path.getmtime(nodes_path), tz=timezone.utc).isoformat()

    enforcement = {
        "source_engine": "amit_gatekeeper.run_amit",
        "ranking_key": "annual_gross_profit (KES GP/year) -- see module docstring; "
                       "gmroi is reported but does not decide this cap",
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "graph_source": os.path.abspath(nn_path),
        "graph_generated_at": graph_generated_at,
        "max_age_days": AMIT_MAX_AGE_DAYS,
        "blacklist": blacklist_set,
        "blacklist_details": blacklist,
        "lowest_gmroi_per_dept": lowest_gmroi_per_dept,
        "department_caps_applied": {d: caps.get(d) for d in dept_skus},
        "departments_uncapped": sum(1 for v in dept_stats.values() if v.get("uncapped")),
        "fallback_cap_declared": _FALLBACK_DECLARED,
        "cap_collisions": [{"normalized_dept": nk, "kept_cap": max(o, n)}
                          for nk, o, _rk, n in cap_collisions],
        "total_budget_scale": total_budget,
        "join_stats": {
            "margin": margin_stats,
            "ads": ads_stats,
            "raw_departments_seen": len(raw_depts_seen),
            "normalized_departments": len(dept_skus),
        },
        "stats": {
            "total_skus_analyzed": len(nodes),
            "total_departments": len(dept_skus),
            "total_blacklisted": len(blacklist),
            "departments_over_cap": sum(1 for d in dept_stats.values() if d["over_cap"] > 0),
        },
    }

    # Write output
    # BLAST RADIUS. Compute what this enforcement file would cost before it is
    # allowed to exist. The last version of this engine blocked 52% of
    # live-demand lines and 21% of annual revenue, and nothing anywhere said
    # so -- it took a 12-seed simulation showing service at 45% to surface it.
    _live_rev = sum(n.get("revenue_year", 0.0) for n in nodes
                    if n.get("velocity_ads", 0) > 0)
    _blocked = {b["sku"] for b in blacklist}
    _blocked_rev = sum(n.get("revenue_year", 0.0) for n in nodes
                       if n.get("velocity_ads", 0) > 0 and n["id"] in _blocked)
    _share = (_blocked_rev / _live_rev) if _live_rev else 0.0
    enforcement["stats"]["blocked_revenue_share"] = round(_share, 4)
    enforcement["stats"]["blocked_revenue_year"] = round(_blocked_rev)
    if _share > MAX_BLOCKED_REVENUE_SHARE:
        enforcement["blacklist"] = []
        enforcement["blacklist_details"] = []
        enforcement["stats"]["refused"] = True
        enforcement["stats"]["refused_reason"] = (
            f"would block {_share:.1%} of live-demand revenue "
            f"(KES {_blocked_rev:,.0f}/yr), over the "
            f"{MAX_BLOCKED_REVENUE_SHARE:.0%} ceiling. The caps table, not the "
            f"ranking, decides this number: author caps for the departments "
            f"that need them, or raise OASIS_AMIT_MAX_BLOCK_SHARE knowing what "
            f"it costs. Published EMPTY rather than silently shutting the shop.")
        logger.error("[AMIT] REFUSED: %s", enforcement["stats"]["refused_reason"])
    else:
        enforcement["stats"]["refused"] = False

    output_path = os.path.join(data_dir, "amit_enforcement.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enforcement, f, indent=2)

    logger.info(f"[AMIT] Enforcement written to {output_path}")
    logger.info(f"[AMIT] Stats: {enforcement['stats']}")

    return enforcement


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMIT Gatekeeper — Chapter 11 Sub-Routine A")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"), help="Path to oasis/data directory")
    parser.add_argument("--nn-path", default=os.path.join(os.path.dirname(__file__), "..", "..", "neutral_network_export"), help="Path to neural network export directory")
    parser.add_argument("--budget", type=float, help="Total budget for dynamic cap scaling (e.g. 10000000 for 10M KES)")
    parser.add_argument("--shuffle-seed", type=int, default=None, help="Shuffle node order before scoring (order-dependence probe)")
    args = parser.parse_args()

    result = run_amit(args.nn_path, args.data_dir, total_budget=args.budget, node_order_seed=args.shuffle_seed)
    print("\n=== AMIT COMPLETE ===")
    print(f"Total Blacklisted: {result['stats']['total_blacklisted']}")
    print(f"Departments Over Cap: {result['stats']['departments_over_cap']}")
