"""The order quantity, derived rather than tuned.

    S = d(R+L) + z·sqrt((R+L)·sigma_d^2 + d^2·sigma_L^2)
    Q = ceil((S - I - O) / pack) · pack

Four measured inputs (d, sigma_d, L, sigma_L), one policy (R), one economic
ratio (z), three physical clamps. No free constants.

WHY THIS EXISTS ALONGSIDE THE CLASSIC PATH
------------------------------------------
The engine's current form computes a horizon twice, with two different safety
definitions, and multiplies the result by five factors — m_sim, m_vel, b_cat,
m_depth, kappa — plus eleven category constants. Measured against a year of the
client's own purchase history, that stack turns a 7-day cadence into a 24-day
target and buys an implicit ~76% service level nobody chose.

This is the same decision expressed as the periodic-review problem it actually
is. It is NOT switched on: OASIS_ORDER_MODEL selects, and `classic` remains the
default until the comparison says otherwise.

THE THREE THINGS THIS FIXES, in measured order of worth
-------------------------------------------------------
  R      the review period is a POLICY, not the observed order gap. The client's
         own schedule declares a weekday for 940 suppliers; orders actually went
         in 2.21x less often than that. Using the observed gap is worth 2.14x on
         working capital -- the single largest lever in the formula.

  sigma_L lead time is as variable as it is long (sigma_L = 2.22d against a mean
         of 2.29d, over 101,247 deliveries). The classic path models demand
         variance and ignores lead-time variance entirely, though the latter is
         the larger of the two terms for any line that moves.

  z      one service level, from the cost ratio, replacing a product of five
         multipliers that approximated it without ever naming it.

WHAT IT DOES NOT CHANGE
-----------------------
The reorder TRIGGER. A true periodic review orders every R days regardless of
position; this keeps the existing ROP trigger so the comparison isolates the
quantity decision. Changing both at once would leave any difference
unattributable.
"""

from __future__ import annotations

import json
import logging
import math
import os
import re
from typing import Tuple, Any, Dict, Optional

logger = logging.getLogger("OASIS.OrderUpTo")

#: Review period when a supplier has no declared order day. Weekly is the
#: client's own chain-wide policy (935 of 940 suppliers sit on a single
#: weekday), so it is a measured default rather than a guess.
DEFAULT_REVIEW_DAYS = 7.0

#: Lead-time spread when a supplier has too little history to measure one.
#: 2.22 days is the chain-wide figure across 101,247 deliveries — the honest
#: fallback is the population value, not zero. Zero would silently delete the
#: larger half of the safety term.
DEFAULT_SIGMA_LEAD = 2.22

#: Service level -> z. The critical ratio c_s/(c_s+c_h) belongs here once margin
#: and cost of capital are wired in; until then it is one explicit dial rather
#: than five implicit ones.
Z_FOR_SERVICE = {
    0.50: 0.00, 0.80: 0.84, 0.85: 1.04, 0.90: 1.28,
    0.95: 1.64, 0.975: 1.96, 0.99: 2.33,
}
DEFAULT_SERVICE_LEVEL = 0.90

SCHEDULE_FILE = "supplier_weekly_schedule.json"

#: Supplier codes come in two shapes on this book: SA0209 and SDD067. A pattern
#: that matches only the first silently discards a third of the file.
_CODE = re.compile(r"^[A-Z]{2,4}\d{3,5}\s*-")
_EMPTY_CODE = re.compile(r"^[A-Z]{2,4}\d{3,5}\s*-\s*$")
#: Truncation markers from the report this file was parsed out of.
_ARTEFACT = re.compile(r"^\.\.\.\s*AND\s+\d+\s+MORE$|^\(.*\)$|^\d+$", re.I)


def model_name() -> str:
    """Which quantity model is selected. `classic` unless asked otherwise."""
    return (os.getenv("OASIS_ORDER_MODEL") or "classic").strip().lower()


def is_enabled(default: str = "classic") -> bool:
    """Whether the derived order-up-to model drives the quantity.

    Reads OASIS_ORDER_MODEL first, then global_settings.order_model in the
    central config. Until 2026-09 only the environment variable existed and
    nothing anywhere set it, so the module that carries the derivation was off
    in every install while the classic multiplicative path ran instead.
    """
    v = (os.getenv("OASIS_ORDER_MODEL") or "").strip().lower()
    if not v:
        try:
            from .engines_config import load_engines_config
            cfg = load_engines_config(None) or {}
            v = str((cfg.get("global_settings") or {}).get("order_model")
                    or default).strip().lower()
        except Exception:
            v = default
    return v in ("order_up_to", "newsvendor")

def service_level() -> float:
    try:
        v = float(os.getenv("OASIS_SERVICE_LEVEL") or DEFAULT_SERVICE_LEVEL)
    except (TypeError, ValueError):
        return DEFAULT_SERVICE_LEVEL
    return v if 0.0 < v < 1.0 else DEFAULT_SERVICE_LEVEL


def z_score(service: Optional[float] = None) -> float:
    """z for a service level, interpolated between the tabulated points."""
    s = service if service is not None else service_level()
    if s in Z_FOR_SERVICE:
        return Z_FOR_SERVICE[s]
    pts = sorted(Z_FOR_SERVICE.items())
    if s <= pts[0][0]:
        return pts[0][1]
    if s >= pts[-1][0]:
        return pts[-1][1]
    for (s0, z0), (s1, z1) in zip(pts, pts[1:]):
        if s0 <= s <= s1:
            return z0 + (z1 - z0) * (s - s0) / (s1 - s0)
    return Z_FOR_SERVICE[DEFAULT_SERVICE_LEVEL]


# ── R: the review period, from the client's declared schedule ─────────────
def load_review_schedule(root: str) -> Dict[str, float]:
    """Supplier -> review period in days, from the declared order calendar.

    The schedule lists suppliers under each weekday. A supplier on one weekday
    is reviewed every 7 days; on two, every 3.5. This is a POLICY statement —
    how often the buyer gets a chance to order — which is exactly the quantity
    the periodic-review model wants and the observed order gap is not.
    """
    # SEARCH, do not assume. The schedule is a policy file kept at the repo
    # root while data_dir points at oasis/data, so a single os.path.join is
    # off by a level — and the failure is silent: every supplier quietly falls
    # back to R=7 and the file that was supposed to supply the answer is never
    # read. The same trap the calendar loader already works around.
    tried = []
    # Deliberately NOT os.getcwd(): a caller pointed at an empty directory
    # would silently pick up whatever schedule happened to be beside the
    # process, which makes the answer depend on where you were standing.
    # Two levels up from data_dir reaches the repo root and stops there.
    for cand in (os.path.join(root, SCHEDULE_FILE),
                 os.path.join(root, "..", SCHEDULE_FILE),
                 os.path.join(root, "..", "..", SCHEDULE_FILE)):
        path = os.path.abspath(cand)
        tried.append(path)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                sched = json.load(f) or {}
            break
        except (OSError, ValueError) as e:
            logger.warning("order schedule at %s is unreadable (%s)", path, e)
            return {}
    else:
        logger.info("no declared order schedule found (looked in %s) — every "
                    "supplier falls back to R=%.0fd",
                    ", ".join(sorted(set(os.path.dirname(t) for t in tried))),
                    DEFAULT_REVIEW_DAYS)
        return {}

    # The file is not a data export; it was parsed out of a rendered report,
    # and it shows. Three shapes arrive:
    #
    #   "SB0009 - BROOKSIDE DAIRY"   a supplier, code and name
    #   "SB0179 -" then "BRANDACTIV KENYA SR"
    #                                ONE supplier whose name contained a comma,
    #                                split across two entries
    #   "...AND 123 MORE", "(BI-WK)" display truncation markers, not suppliers
    #
    # The old guard was `len(key) > 3`, which admitted every marker. It let 262
    # non-suppliers into the schedule and reported 940 "suppliers with a
    # declared order day" — the figure the derivation quotes for R — when 678
    # of them carry a supplier code. The markers were harmless in themselves,
    # since nothing looks them up; the count was not, and the twelve split
    # suppliers keyed to neither half of their own name and fell back to the
    # default review period without a word.
    days_of: Dict[str, set] = {}
    rejected = 0
    repaired = 0
    for day, names in sched.items():
        if not isinstance(names, (list, tuple)):
            continue
        entries = [str(n or "").strip() for n in names]
        for i, raw in enumerate(entries):
            if not raw:
                continue
            if _EMPTY_CODE.fullmatch(raw):
                # A code with no name: the name is the next entry. Only rejoin
                # when the previous entry is unambiguously a bare code — a
                # general "glue fragments together" rule would be guesswork.
                nxt = entries[i + 1] if i + 1 < len(entries) else ""
                if nxt and not _CODE.match(nxt) and not _ARTEFACT.match(nxt):
                    key = " ".join(nxt.upper().split())
                    if len(key) > 3:
                        days_of.setdefault(key, set()).add(day)
                        repaired += 1
                        continue
                rejected += 1
                continue
            if not _CODE.match(raw):
                # No supplier code. Either a truncation marker or the tail of a
                # name split further up; neither is a supplier on its own.
                rejected += 1
                continue
            name = raw.split(" - ", 1)[1] if " - " in raw else raw
            key = " ".join(name.upper().split())
            if len(key) > 3:
                days_of.setdefault(key, set()).add(day)
            else:
                rejected += 1

    out = {k: (7.0 / len(v)) for k, v in days_of.items() if v}
    logger.info("review schedule: %d suppliers with a declared order day "
                "(%d entries rejected as parse artefacts, %d comma-split names "
                "repaired)", len(out), rejected, repaired)
    return out


PATTERNS_FILE = "supplier_lead_patterns.json"

#: Second-choice source. This is the file `order_engine.py` loads into
#: `databases['supplier_patterns']` and the ORIGINAL wiring bug handed
#: straight to `sigma_lead()` -- it carries no `lead_time_stdev` key, only
#: `lata_stdev_days`, so that lookup always missed and every supplier fell
#: back to the 2.22 constant. It is not a bad file, it is the WRONG PRIMARY:
#: same underlying statistic (Brookside: 0.56d here vs 0.555d in the receipt
#: file), computed by a second pipeline, over a different vendor set (599
#: vs. 472). Kept as a gap-filler for the ~127 vendors it has that the
#: receipt file doesn't, never as an override.
SECONDARY_PATTERNS_FILE = "supplier_patterns_2025.json"


def _load_secondary_patterns(root: str) -> Dict[str, dict]:
    """Gap-filler patterns from SECONDARY_PATTERNS_FILE's `lata_stdev_days`.

    Returns only entries with that field present; every value is tagged
    `provenance: "observed_secondary"` so a caller can tell a receipt-file
    measurement from a table one -- "prefer measured over asserted, and
    label which is which."
    """
    for cand in (os.path.join(root, "oasis", "data", SECONDARY_PATTERNS_FILE),
                 os.path.join(root, "data", SECONDARY_PATTERNS_FILE),
                 os.path.join(root, SECONDARY_PATTERNS_FILE)):
        path = os.path.abspath(cand)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f) or {}
        except (OSError, ValueError) as e:
            logger.warning("secondary lead patterns at %s are unreadable "
                           "(%s)", path, e)
            return {}
        out = {}
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            sd = v.get("lata_stdev_days")
            if sd is None:
                continue
            try:
                sd = float(sd)
            except (TypeError, ValueError):
                continue
            if sd < 0:
                continue
            key = " ".join(str(k).upper().split())
            out[key] = {"lead_time_stdev": sd,
                       "samples": v.get("lata_sample_size"),
                       "provenance": "observed_secondary",
                       "vendor": k}
        logger.info("secondary lead patterns: %d vendors from %s "
                    "(gap-filler only)", len(out),
                    os.path.basename(path))
        return out
    return {}


def load_lead_patterns(root: str) -> Dict[str, dict]:
    """Supplier -> measured lead time and spread, from the receipt history.

    `sigma_lead()` falls back to a chain-wide 2.22 days for any supplier it has
    not measured, which is the right default and the wrong answer for the 472
    suppliers the fulfilment export CAN measure: their median spread is 1.45
    days, so the constant overstates safety stock for the typical line by about
    a third.

    Keyed on the supplier NAME — the part after the code — because that is what
    `recommend()` looks up and what `load_review_schedule` produces. Written
    with both spellings so either matches; a patterns file that loads, parses
    and never matches is the worst of the three outcomes.

    Produced by `devkit/probe_lead_time.py --write`. Absent, the engine keeps
    the default and says so.
    """
    for cand in (os.path.join(root, "oasis", "data", PATTERNS_FILE),
                 os.path.join(root, "data", PATTERNS_FILE),
                 os.path.join(root, PATTERNS_FILE)):
        path = os.path.abspath(cand)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f) or {}
        except (OSError, ValueError) as e:
            logger.warning("lead patterns at %s are unreadable (%s)", path, e)
            return {}
        out = {" ".join(str(k).upper().split()): v
               for k, v in data.items() if isinstance(v, dict)}
        measured = len({v.get("vendor") for v in out.values()})
        logger.info("lead patterns: %d suppliers measured (%d keys); every "
                    "other supplier keeps sigma_L=%.2f", measured, len(out),
                    DEFAULT_SIGMA_LEAD)
        return out
    logger.info("no measured lead patterns found — every supplier uses "
                "sigma_L=%.2f", DEFAULT_SIGMA_LEAD)
    return {}


CADENCE_FILE = "supplier_cadence_from_grn.json"
MIN_CADENCE_DELIVERIES = 10
#: How far the receipt book has to contradict the order calendar before the
#: calendar is treated as falsified rather than authoritative. 2.0 means a
#: supplier declared weekly and observed at 3.5 days or less loses the
#: declaration. Set deliberately loose: a supplier delivering a little more
#: often than promised has not broken its promise.
CADENCE_OVERRIDE_FACTOR = 2.0
#: and only on enough evidence to call it a cadence rather than a run of luck.
CADENCE_OVERRIDE_MIN_RECEIPTS = 30


def load_cadence(root: str) -> Dict[str, float]:
    """Supplier -> the delivery interval the receipt history actually shows.

    lata_derived.json carries review_days = 7.0 for all 944 suppliers. That is
    not a measurement, it is the default wearing a measurement's clothes. The
    GRN book knows better: every receipt carries a vendor and a date, so the
    DISTINCT delivery dates per vendor measure the cadence the supplier runs.
    The dairies come out at a median gap of 1 day against an assumed 7.

    This is a delivery interval, not a declared order day, so it ranks BELOW
    the calendar: a supplier with a declared day keeps it, because that is a
    commitment somebody made. It ranks ABOVE the blanket default, because a
    measured 1-day cadence is evidence and 7.0 is an assumption.

    Only vendors with at least MIN_CADENCE_DELIVERIES receipts are used: a
    vendor seen twice has a gap, not a cadence.

    Produced by `devkit/probe_fresh_cover.py`.
    """
    for cand in (os.path.join(root, "oasis", "data", CADENCE_FILE),
                 os.path.join(root, "data", CADENCE_FILE),
                 os.path.join(root, CADENCE_FILE)):
        path = os.path.abspath(cand)
        if not os.path.exists(path):
            continue
        try:
            with open(path, encoding="utf-8") as f:
                data = json.load(f) or {}
        except (OSError, ValueError) as e:
            logger.warning("cadence at %s is unreadable (%s)", path, e)
            return {}
        out, counts = {}, {}
        for k, v in data.items():
            if not isinstance(v, dict):
                continue
            g = v.get("median_gap")
            if g is None or (v.get("deliveries") or 0) < MIN_CADENCE_DELIVERIES:
                continue
            kk = " ".join(str(k).upper().split())
            out[kk] = max(1.0, float(g))
            counts[kk] = int(v.get("deliveries") or 0)
        out["__counts__"] = counts
        logger.info("measured cadence: %d suppliers with >=%d receipts",
                    len(out) - 1, MIN_CADENCE_DELIVERIES)
        return out
    logger.info("no measured cadence found - unscheduled suppliers keep the "
                "mode default")
    return {}


_CADENCE_CACHE: Optional[Dict[str, float]] = None


def default_cadence(root: Optional[str] = None) -> Dict[str, float]:
    global _CADENCE_CACHE
    if _CADENCE_CACHE is None:
        base = root or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".."))
        _CADENCE_CACHE = load_cadence(base)
    return _CADENCE_CACHE


SHELF_LIFE_FILE = "shelf_life_days.json"
_SHELF_CACHE: Optional[Dict[str, float]] = None


def load_shelf_life(root: Optional[str] = None) -> Dict[str, float]:
    """Department -> days the product survives on a shelf.

    `clamp_level()` has always accepted `shelf_life_days` and NOTHING has ever
    populated it, so the clamp has never once fired. The engine was asking for
    10.7 days of cover on fresh milk with a day and a bit of life.

    These numbers are ASSERTED, not measured: they come from the category, not
    from the data, and they should be replaced by code-date evidence when any
    exists. They are a ceiling, so an asserted ceiling that is roughly right
    beats no ceiling at all.
    """
    global _SHELF_CACHE
    if _SHELF_CACHE is not None:
        return _SHELF_CACHE
    base = root or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", ".."))
    for cand in (os.path.join(base, "oasis", "data", SHELF_LIFE_FILE),
                 os.path.join(base, "data", SHELF_LIFE_FILE)):
        if os.path.exists(cand):
            try:
                with open(cand, encoding="utf-8") as f:
                    data = json.load(f) or {}
                _SHELF_CACHE = {" ".join(str(k).upper().split()): float(v)
                                for k, v in data.items()
                                if isinstance(v, (int, float))}
                logger.info("shelf life: %d departments", len(_SHELF_CACHE))
                return _SHELF_CACHE
            except (OSError, ValueError, TypeError):
                break
    _SHELF_CACHE = {}
    return _SHELF_CACHE


PER_SKU_SHELF_FILE = "shelf_life_per_sku.json"
_PER_SKU_SHELF: Optional[Dict[str, dict]] = None


def load_shelf_life_per_sku(root: Optional[str] = None) -> Dict[str, dict]:
    """Per-SKU effective shelf life, MEASURED where the book can measure it.

    Receipt-to-expiry from the purchase-return book, joined to the GRN that
    delivered the stock -- 8,311 dated returns over 2,095 SKUs. The department
    table stays as the fallback, but it is asserted and it was wrong in both
    directions: YOGHURT measured 7 days against 14 asserted, ICE-CREAM 45
    against 90, BREAD 3 against 1.2. And it covered no dry goods at all, while
    the returns show FLOUR at 26 days, BISCUITS 54, CRISPS 65.

    Per-SKU is also the only form that survives a change of universe. The same
    derivation in a pharmacy produces drug-level expiry without anyone
    rewriting a department list.

    Produced by devkit/derive_shelf_life.py --write.
    """
    global _PER_SKU_SHELF
    if _PER_SKU_SHELF is not None:
        return _PER_SKU_SHELF
    base = root or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", ".."))
    for cand in (os.path.join(base, "oasis", "data", PER_SKU_SHELF_FILE),
                 os.path.join(base, "data", PER_SKU_SHELF_FILE)):
        if os.path.exists(cand):
            try:
                with open(cand, encoding="utf-8") as f:
                    data = json.load(f) or {}
                _PER_SKU_SHELF = {" ".join(str(k).upper().split()): v
                                  for k, v in data.items() if isinstance(v, dict)}
                logger.info("shelf life: %d SKUs, %d of them measured",
                            len(_PER_SKU_SHELF),
                            sum(1 for v in _PER_SKU_SHELF.values()
                                if str(v.get("provenance", "")).startswith("observed_return")))
                return _PER_SKU_SHELF
            except (OSError, ValueError, TypeError):
                break
    _PER_SKU_SHELF = {}
    return _PER_SKU_SHELF


LONG_LIFE_CONFIG = "oasis_engines_config.json"
_LONG_LIFE: Optional[Tuple[frozenset, tuple]] = None


def load_long_life(root: Optional[str] = None):
    """The operator-maintained long-life list, as (products, name_tokens).

    Lines that sit in a FRESH department and keep for months. The config block
    has existed and been inert: intelligence_mixin carried its own hardcoded
    ('UHT','ESL','LONG LIFE') in four places and never read the file, and the
    order-up-to path never consulted it at all -- so BROOKSIDE 500ML DAIRY
    BEST, an ESL pouch, took FRESH MILK's 1.2-day ceiling, and TUZO 500ML
    FINO 180DAYS took it too. A product whose own name says 180 days was being
    ordered as though it died tomorrow.

    The list is deliberately curated rather than pattern-matched. The config's
    own note says why: LONGLIFE also appears on a battery, and the long-life
    cap is tighter than the dry-goods cap, so a broad token would quietly
    REDUCE cover on non-dairy stock. Awkward cases go in `products`.
    """
    global _LONG_LIFE
    if _LONG_LIFE is not None:
        return _LONG_LIFE
    base = root or os.path.abspath(
        os.path.join(os.path.dirname(__file__), "..", ".."))
    prods, toks = frozenset(), ()
    for cand in (os.path.join(base, "oasis", "data", LONG_LIFE_CONFIG),
                 os.path.join(base, "data", LONG_LIFE_CONFIG)):
        if os.path.exists(cand):
            try:
                with open(cand, encoding="utf-8") as f:
                    ll = (json.load(f) or {}).get("long_life") or {}
                prods = frozenset(" ".join(str(x).upper().split())
                                  for x in (ll.get("products") or []))
                toks = tuple(" ".join(str(x).upper().split())
                             for x in (ll.get("name_tokens") or []))
                logger.info("long life: %d named products, %d name tokens",
                            len(prods), len(toks))
            except (OSError, ValueError, TypeError):
                pass
            break
    _LONG_LIFE = (prods, toks)
    return _LONG_LIFE


def is_long_life(sku: Any, root: Optional[str] = None) -> bool:
    """Named product, or a name token on a WORD BOUNDARY.

    A naive substring test is wrong and quietly so: 'ESL' is inside DESLY and
    inside MUESLI, so bread crumbs and muesli bread both came back long-life
    on the first pass and were handed an unclamped ceiling. The config's own
    note worries about VARTA LONGLIFE POWER BATT -- a whole-word false
    positive -- and misses that the shortest token fires inside ordinary
    words. Both are the same failure and a boundary fixes both.
    """
    k = " ".join(str(sku or "").upper().split())
    if not k:
        return False
    prods, toks = load_long_life(root)
    if k in prods:
        return True
    for t in toks:
        if t and re.search(r"(?<![A-Z0-9])" + re.escape(t) + r"(?![A-Z0-9])", k):
            return True
    return False


def shelf_life_for(department: str, root: Optional[str] = None,
                   sku: Optional[str] = None) -> float:
    """Effective shelf life in days: the SKU's own where it is measured, else
    its department's asserted number, else unclamped."""
    # THE MEASURED FIGURE IS AN UPPER BOUND, NOT THE LIFE.
    # It is days from receipt to WRITE-OFF, and a write-off is processed when
    # somebody gets to it. FESTIVE 800G WHITE MILKY BREAD measures 16 days;
    # bread does not live 16 days, the paperwork does. So where a department
    # asserts a physical limit, the ceiling is the SMALLER of the two -- a
    # ceiling that errs long is worse than one that errs short, because the
    # first fills a shelf with stock that dies on it.
    #
    # Where no department figure exists -- FLOUR, BISCUITS, CRISPS, RICE, all
    # of which the asserted table never covered and all of which the return
    # book shows expiring -- the measured number stands alone. Some clamp
    # beats none.
    dept = " ".join(str(department or "").upper().split())
    v = (load_shelf_life_per_sku(root).get(" ".join(str(sku).upper().split()))
         if sku else None)
    # A caller that knows the SKU but not its department should still get the
    # clamp. The per-SKU file records the department the SKU was seen in, so
    # the asserted ceiling is recoverable without the caller supplying it --
    # otherwise a product record missing one field silently loses its shelf
    # life entirely, which is how a 500ml milk pouch ends up sized at three
    # and a third days of cover.
    if not dept and v:
        dept = " ".join(str(v.get("department") or "").upper().split())
    # LONG LIFE OUTRANKS THE DEPARTMENT.
    # A department ceiling is an assertion about the typical product in that
    # aisle. An ESL or UHT pouch in the fresh-milk chiller is the exception the
    # operator has already written down, and applying FRESH MILK's 1.2 days to
    # it removes any ability to hold stock against a supply disruption on a
    # product that keeps for months. The SKU's own measured life still applies
    # if the return book has one; otherwise it is unclamped, like dry goods.
    if is_long_life(sku, root):
        sku_v = float(v.get("shelf_life_days") or 0) if v else 0.0
        return sku_v if sku_v > 0 else 0.0
    dept_v = load_shelf_life(root).get(dept, 0.0)
    if sku:
        sku_v = float(v.get("shelf_life_days") or 0) if v else 0.0
        if sku_v > 0:
            return min(sku_v, dept_v) if dept_v > 0 else sku_v
    return dept_v


#: Under on-demand ordering there is no cycle to wait for: the next chance to
#: buy is the next working day. R collapses to that, not to a week.
ON_DEMAND_REVIEW_DAYS = 1.0

MODES = ("scheduled", "on_demand")


#: Vendor strings arrive in two spellings and always have. The GRN book and
#: most product records carry "SB0009 - BROOKSIDE DAIRY LIMITED"; the order
#: calendar, the cadence file and the lead patterns are keyed on the NAME
#: alone. Every lookup in this module missed on the code-prefixed form and
#: fell to the blanket default -- so a dairy the receipt book measures at a
#: one-day cadence was reviewed as if weekly, R went 1 -> 7, and the entire
#: LATA gain evaporated in production while every offline probe (which strips
#: the code before looking up) reported it working. A join that succeeds in
#: the harness and fails in the engine is the worst kind, because the harness
#: keeps saying yes.
_CODE_PREFIX = re.compile(r"^[A-Z]{1,3}\d{3,6}\s*-\s*")


def supplier_key(name: Any) -> str:
    """The one spelling every supplier lookup in this module uses."""
    s = " ".join(str(name or "").upper().split()).strip("[]").strip()
    return _CODE_PREFIX.sub("", s).strip()


def ordering_mode(default: str = "scheduled") -> str:
    """Whether ordering waits for a declared day, or can happen any day.

    The distinction matters more than it looks. `R` is the review period — how
    long until the buyer next gets a chance to order — and that is a property of
    the ORDERING PROCESS, not of the supplier. A calendar day is needed when
    ordering is scheduled or autonomous. When a buyer can raise an order today,
    there is no cycle to cover.
    """
    v = (os.getenv("OASIS_ORDERING_MODE") or default).strip().lower()
    return v if v in MODES else default


def review_period(supplier: str, schedule: Optional[Dict[str, float]] = None,
                  default: float = DEFAULT_REVIEW_DAYS,
                  mode: Optional[str] = None,
                  cadence: Optional[Dict[str, float]] = None) -> float:
    """Days until the next chance to order this supplier.

    A supplier absent from the calendar has NOT told us it can only be ordered
    weekly — it has told us nothing. Which of those two the engine assumes is
    worth a week of cover on 48% of the book:

      scheduled   ordering waits for declared days, and a supplier with no
                  declared day is reviewed on the chain's default cycle.
      on_demand   a buyer can raise an order any working day, so an unscheduled
                  supplier's review period is one day, not seven.

    A supplier that IS on the calendar keeps its declared cadence in both modes:
    that is a commitment somebody made, and it holds whether or not the engine
    could have ordered sooner.
    """
    key = supplier_key(supplier)
    cad = default_cadence() if cadence is None else cadence
    measured = cad.get(key) if cad else None
    declared = schedule.get(key) if schedule else None

    # A calendar entry is ASSERTED: somebody wrote a weekday down. A cadence
    # built from 300 distinct receipt dates is MEASURED. Where the two agree,
    # or disagree mildly, the declaration stands -- it is a commitment, and a
    # commitment can be honoured more often than it promises without ceasing
    # to be the thing you can rely on.
    #
    # Where the receipt book contradicts the declaration by a FACTOR, the
    # declaration is falsified, not authoritative. The dairies are on the
    # calendar for one weekday and appear in the GRN book at a median gap of
    # one day. Believing the calendar there costs a week of cover on product
    # that lives a day and a bit.
    if declared is not None:
        if (measured is not None
                and measured <= declared / CADENCE_OVERRIDE_FACTOR
                and (cad.get("__counts__", {}) or {}).get(key, MIN_CADENCE_DELIVERIES)
                >= CADENCE_OVERRIDE_MIN_RECEIPTS):
            logger.debug("cadence overrides calendar for %s: declared %.1f d, "
                         "measured %.1f d", key, declared, measured)
            return measured
        return declared
    if measured is not None:
        return measured
    mode = (mode or ordering_mode()).lower()
    if mode == "on_demand":
        return ON_DEMAND_REVIEW_DAYS
    return default


# ── sigma_L: how much the supplier's lead time actually moves ─────────────
def _r_source(supplier, schedule, cadence=None) -> str:
    key = supplier_key(supplier)
    cad = default_cadence() if cadence is None else cadence
    m = cad.get(key) if cad else None
    d = schedule.get(key) if schedule else None
    if d is not None:
        if m is not None and m <= d / CADENCE_OVERRIDE_FACTOR and \
                (cad.get("__counts__", {}) or {}).get(key, 0) >= CADENCE_OVERRIDE_MIN_RECEIPTS:
            return "cadence_overrides_calendar"
        return "calendar"
    return "cadence" if m is not None else "default"


#: Chain-wide fraction of `sigma_lead()` calls (i.e. order lines actually
#: sized) that must resolve a MEASURED spread before the fallback constant is
#: trusted blind. Set from the observed join rate of the wiring bug this
#: guards against: before the fix (patterns dict with no recognised key at
#: all) the rate was ~0%; the measured file alone covers ~50% of declared
#: suppliers by vendor count, more by order-line volume since it is weighted
#: toward the suppliers who actually ship. 0.30 is well below either measured
#: figure and well above the ~0% a silent re-break would produce -- see
#: devkit/probe_lata_wiring.py for the number this floor is calibrated against.
JOIN_RATE_FLOOR = 0.30


class _JoinStats:
    """Hits/misses for `sigma_lead()` -- the T1-style guard task A asks for.

    A patterns dict that loads, parses and matches nothing is worse than no
    patterns dict: it LOOKS wired up. This is the difference between "the
    file is missing" (logged once, loudly, at load time) and "the file loads
    fine but the join silently fails" (invisible unless someone counts).
    """

    def __init__(self) -> None:
        self.hits = 0
        self.total = 0

    def reset(self) -> None:
        self.hits = 0
        self.total = 0

    def record(self, matched: bool) -> None:
        self.total += 1
        if matched:
            self.hits += 1

    @property
    def rate(self) -> float:
        return (self.hits / self.total) if self.total else 0.0


_JOIN_STATS = _JoinStats()


def check_join_rate(floor: float = JOIN_RATE_FLOOR, raise_on_fail: bool = False,
                    reset: bool = True) -> float:
    """Chain-wide fraction of scored order lines that hit a MEASURED sigma_L.

    Call this after a batch of `recommend()` calls (simulation_bridge does,
    once per scan). Logs a warning below `floor`; raises instead when a
    caller wants a hard stop rather than a log line nobody reads. This is the
    guard against the exact failure this package fixes: a patterns dict that
    resolves, but to the wrong file, so every lookup misses and the join rate
    silently sits at (or near) zero while nothing else says so.
    """
    stats = _JOIN_STATS
    rate = stats.rate
    if stats.total == 0:
        logger.info("sigma_L join rate: no order lines scored yet")
        return rate
    msg = (f"sigma_L join rate: {stats.hits}/{stats.total} order lines "
          f"resolved a measured lead-time spread ({rate:.1%}), floor "
          f"{floor:.0%}")
    if rate < floor:
        logger.warning(msg + " -- patterns dict is loaded but barely "
                       "matching; check the key format/source before "
                       "trusting the fallback constant (T1: silent join "
                       "failure)")
        if raise_on_fail:
            raise RuntimeError(msg)
    else:
        logger.info(msg)
    if reset:
        stats.reset()
    return rate


#: Receipts needed before a measured lead-time spread is believed. Below this
#: the figure is noise: the book's sigma_L tops out at 71 days unfiltered and
#: at 13.35 days once 30 receipts are required.
MIN_SIGMA_SAMPLES = 30
#: How far above its own shelf life a line may be forced by the protection
#: interval before the engine treats it as a supply-terms failure rather than
#: an order.
MAX_SHELF_MULTIPLE = 2.0
#: DAILY-REPLENISHED FRESH IS CAPPED, AND THAT IS THE POLICY, NOT A DEFECT.
#: Milk and bread cycle every day. The operator's rule is 1.2 days, never
#: above 2.0, and the protection-interval floor must respect it: d*(R+L) came
#: to 2.28 days on the dairies because the measured lead time is 1.28, and
#: 2.28 > 2.0. The floor exists to stop a ceiling manufacturing a stockout,
#: not to overrule a merchandising limit that is there for spoilage. Where the
#: two disagree the cap wins and the line is reported as infeasible, which
#: points at the real fix: a shorter lead time, not a deeper shelf.
#: Long-life and UHT are exempt -- they are not on this cycle and keep their
#: longer buffer.
FRESH_CYCLE_SHELF_DAYS = 3.0     # at or under this, a line is daily-cycle fresh
FRESH_COVER_CEILING_DAYS = 2.0   # and never carries more than this
#: Percentile of the measured population used for suppliers we cannot measure.
#: NOT the median: an unmeasured supplier is an unknown, and an unknown should
#: not be given the typical supplier's reliability. p75 is conservative and
#: SOURCED, which the hardcoded 2.22 was not -- that constant sat 54% above the
#: measured median of 1.44 with nothing behind it.
CHAIN_SIGMA_PERCENTILE = 0.75
_CHAIN_SIGMA: Optional[float] = None


def chain_sigma_lead(patterns: Optional[Dict[str, dict]] = None) -> float:
    """The fallback sigma_L, derived from the suppliers we CAN measure.

    A constant nobody can point at is a parameter nobody can argue with. This
    one is the p75 of `lead_time_stdev` across every supplier with at least
    MIN_SIGMA_SAMPLES receipts, so it moves when the book moves and can be
    checked against the file it came from.
    """
    global _CHAIN_SIGMA
    if _CHAIN_SIGMA is not None:
        return _CHAIN_SIGMA
    pats = patterns if patterns is not None else default_patterns()
    vals = []
    for v in (pats or {}).values():
        if not isinstance(v, dict):
            continue
        try:
            if int(v.get("samples") or 0) < MIN_SIGMA_SAMPLES:
                continue
            x = float(v.get("lead_time_stdev", v.get("lata_stdev_days")))
        except (TypeError, ValueError):
            continue
        if x >= 0:
            vals.append(x)
    if len(vals) < 50:
        _CHAIN_SIGMA = DEFAULT_SIGMA_LEAD
        logger.info("chain sigma_L: too few measured suppliers (%d), keeping "
                    "the %.2f default", len(vals), DEFAULT_SIGMA_LEAD)
        return _CHAIN_SIGMA
    vals.sort()
    _CHAIN_SIGMA = vals[int(CHAIN_SIGMA_PERCENTILE * len(vals))]
    logger.info("chain sigma_L: p%d of %d measured suppliers = %.2f d "
                "(was a hardcoded %.2f)", int(100 * CHAIN_SIGMA_PERCENTILE),
                len(vals), _CHAIN_SIGMA, DEFAULT_SIGMA_LEAD)
    return _CHAIN_SIGMA


def sigma_lead(pattern: Optional[dict],
               default: Optional[float] = None,
               record: bool = True) -> float:
    """Lead-time standard deviation for a supplier.

    Prefers a measured value on the supplier's pattern; falls back to the
    chain-wide figure. Never zero: a supplier we have not measured is not a
    supplier who always delivers on time, and treating it as one deletes the
    larger half of the safety term.

    Four key spellings, not three: `lata_stdev_days` is the SAME measured
    quantity under the name the older supplier_patterns_2025.json table uses
    (Brookside: 0.56 there against 0.555 here -- one statistic, two
    pipelines). Recognising it here means a caller who is handed that table
    by mistake degrades gracefully instead of silently landing on the 2.22
    constant for every supplier, which is exactly the wiring bug this
    package exists to fix (see simulation_bridge.py's old
    `patterns=self.engine.databases.get('supplier_patterns', {})`).

    `record=False` is for callers that look up sigma_L for every line they
    ever SCAN (e.g. the classic path's trigger, computed before a line is
    known to need an order at all) rather than for a line actually being
    SIZED -- passing False keeps that traffic out of `check_join_rate()`,
    whose contract ("fraction of order lines") is about lines the derived
    model actually sizes, not every SKU touched by a pass over the catalogue.
    """
    # SAMPLE FLOOR. A standard deviation from six receipts is not a measurement
    # of anything. Unfiltered, the book contains suppliers at sigma_L = 71 days
    # -- which is not lead-time variance, it is a sporadic vendor seen twice a
    # year -- and 71 days of sigma drives 91 days of safety stock through the
    # quadrature. Above 30 samples the whole book tops out at 13.35 days with a
    # p99 of 5.54, which is a spread you can actually plan against.
    #
    # A supplier this thin is not thereby RELIABLE, so the fallback is the
    # chain-wide constant, not zero -- same reasoning as the missing-supplier
    # case above. And a supplier whose lead time genuinely swings by weeks is a
    # supplier to renegotiate or drop (MANDE's job), not one to absorb with
    # stock: safety stock is priced for noise, not for a broken relationship.
    if default is None:
        default = chain_sigma_lead()
    if isinstance(pattern, dict):
        try:
            _n = int(pattern.get("samples") or pattern.get("n") or 0)
        except (TypeError, ValueError):
            _n = 0
        if _n and _n < MIN_SIGMA_SAMPLES:
            if record:
                _JOIN_STATS.record(False)
            return default
        for key in ("lead_time_stdev", "lead_time_std", "lead_stdev",
                   "lata_stdev_days"):
            v = pattern.get(key)
            if v is not None:
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if v >= 0:
                    if record:
                        _JOIN_STATS.record(True)
                    return v
    if record:
        _JOIN_STATS.record(False)
    return default


#: DEMAND VARIABILITY, DERIVED RATHER THAN BANDED.
#:
#: intelligence_mixin scales the whole target by a step function of ADS --
#: 1.4 above 10/day down to 0.8 at or below 1/day -- as its spike and
#: bulk-event handling. Two things are wrong with it and one is right.
#:
#: WRONG, placement. `target_days *= v(d)` scales CYCLE stock, which is not a
#: risk quantity at all -- it is the demand that will certainly arrive over the
#: protection interval -- and it scales the safety term a second time when
#: sigma_P is already proportional to d.
#:
#: WRONG, direction. Counting arrivals in a fixed window is Poisson to first
#: order, and for Poisson
#:        sigma = sqrt(d)   =>   cv = sigma/d = 1/sqrt(d)
#: so relative variability FALLS as velocity rises. A SKU selling 0.2 a day has
#: a cv of 2.2; one selling 60 a day has 0.13. The band table runs the other
#: way: it gives fast movers 1.4x and slow movers 0.8x, which is the reverse of
#: the statistics, and it is why the long tail -- 13,553 of 15,037 SKUs sell a
#: unit a day or less -- carries almost no safety stock and produces the
#: stockouts.
#:
#: RIGHT, the instinct. A bulk-shopping day does land harder on a fast mover,
#: and that is real overdispersion on top of Poisson. So the model is
#:        cv(d) = sqrt( 1/d + phi^2 )
#: one Poisson term that needs no parameter, plus ONE overdispersion parameter
#: phi that survives at high velocity and carries the basket and payday effects.
#: phi replaces five hand-set bands and the flat 0.40, and it is the single
#: number to fit when real till data exists -- every POS database in this
#: install is synthetic, so it cannot be fitted today.
#:
#: phi defaults to the chain's old flat assumption, so nothing changes for a
#: fast mover and the slow tail gets the protection the arithmetic says it
#: always needed.
DEMAND_OVERDISPERSION = float(os.getenv("OASIS_DEMAND_OVERDISPERSION", "0.40"))
CV_CAP = 2.5        # a cv above this is a dead line, not a variable one


def demand_cv(avg_daily_sales: float, phi: Optional[float] = None) -> float:
    """cv of daily demand = sqrt(1/d + phi^2). Poisson plus overdispersion.

    THE CAP MAY REMOVE OVERDISPERSION. IT MAY NOT REMOVE COUNTING NOISE.
    Var >= mean is a floor for a count, not a modelling preference, and
    cv = 1/sqrt(d) is that floor written as a ratio. A flat cap of 2.5 binds
    whenever 1/sqrt(d) > 2.5 -- that is, on every line under 0.16/day -- and
    below that point it was returning a cv implying LESS variance than Poisson:

        d = 0.114 (this book's median ordered line)  ->  0.71x the floor
        d = 0.05                                     ->  0.31x
        d = 0.02                                     ->  0.13x

    Measured consequence: the level the engine computed delivered a median
    cycle service of 0.862 against a 0.900 target, because the safety term on
    the slowest half of the catalogue was built from a variance that cannot
    occur. Pack rounding hid it -- realised service came to 0.951 -- which is
    luck, not design, and it is why this went unnoticed.

    So the cap now applies only to the part it was written for. Its own comment
    says "a cv above this is a dead line, not a variable one": that is an
    argument about runaway overdispersion, never about the irreducible
    variance of counting. Floor first, cap second.
    """
    d = float(avg_daily_sales or 0)
    p = DEMAND_OVERDISPERSION if phi is None else float(phi)
    if d <= 0:
        # No rate, so no Poisson floor to respect; unchanged on purpose.
        return min(CV_CAP, math.sqrt(1.0 + p * p))
    poisson_floor = math.sqrt(1.0 / d)
    return max(poisson_floor, min(CV_CAP, math.sqrt(1.0 / d + p * p)))


# ── the formula ───────────────────────────────────────────────────────────
def demand_sigma_over(interval_days: float, d: float, sigma_d: float,
                      sigma_lead_days: float) -> float:
    """sqrt(P·sigma_d^2 + d^2·sigma_L^2).

    Both halves matter. The first is demand varying over a fixed horizon; the
    second is the horizon itself varying, which the classic path omits and
    which is the larger term wherever the line actually moves.
    """
    P = max(0.0, float(interval_days))
    return math.sqrt(P * float(sigma_d) ** 2
                     + (float(d) ** 2) * (float(sigma_lead_days) ** 2))


#: Above this mean demand over the protection interval, the normal
#: approximation and the exact discrete quantile agree to well under one unit,
#: and the normal form is cheaper and smoother. Below it they do not agree at
#: all -- which is where 86.7% of this book's ordered lines live.
DISCRETE_QUANTILE_MAX_LAMBDA = float(
    os.getenv("OASIS_DISCRETE_QUANTILE_MAX_LAMBDA", "40"))

#: Whether S comes from the exact discrete quantile or from d*P + z*sigma_P.
#: Off by default until the measurement that justifies flipping it.
_DISCRETE_QUANTILE = (os.getenv("OASIS_DISCRETE_QUANTILE", "")
                      .strip().lower() in ("1", "true", "yes", "on"))


def use_discrete_quantile() -> bool:
    """Read at call time, so a harness can toggle it between runs."""
    v = os.getenv("OASIS_DISCRETE_QUANTILE")
    if v is None:
        return _DISCRETE_QUANTILE
    return v.strip().lower() in ("1", "true", "yes", "on")


def demand_quantile_over(interval_days: float, d: float, sigma_d: float,
                         sigma_lead_days: float, service: float) -> float:
    """Smallest stock level s with P(demand over P <= s) >= service.

    WHY NOT d*P + z*sigma_P. That form asks a continuous, symmetric
    distribution for a quantile of a count. It is an excellent approximation
    when the mean is large and a poor one when it is small, and on this book
    the mean is small: lambda = d*(R+L) is under 1 unit on 38.1% of ordered
    lines, under 3 on 68.2%, and under 10 on 86.7%. Measured against the exact
    law, the normal-derived S delivers a median cycle service of 0.862 against
    a 0.900 target -- it under-protects, systematically, on the tail where most
    of the catalogue lives.

    WHICH LAW. The cv model already fixes it. sigma_d^2 = d + d^2*phi^2, so

        sigma_P^2 = P*sigma_d^2 + d^2*sigma_L^2
                  = lambda + lambda*d*phi^2 + d^2*sigma_L^2

    The variance is the mean plus non-negative terms, always. So the family is
    Poisson exactly when overdispersion and lead-time variance both vanish,
    and negative binomial -- the standard mean/variance-matched discrete
    alternative -- whenever they do not. There is no parameter region where
    this is undefined, which is why it can replace the normal outright rather
    than only in a special case.

    RETURNS A LEVEL, NOT A SAFETY TERM. The caller still clamps it for shelf
    life and display, and still rounds the resulting ORDER to a pack. Those
    two are what currently rescue the normal form: rounding up to a whole pack
    lifts median realised service from 0.862 to 0.951. This makes the level
    right on its own instead of relying on that accident, which is why it also
    tightens the over-served median rather than only lifting the short tail.
    """
    d = float(d or 0)
    P = max(0.0, float(interval_days))
    if d <= 0 or P <= 0:
        return 0.0
    s = min(max(float(service), 0.0), 0.999999)
    mu = d * P
    var = demand_sigma_over(P, d, sigma_d, sigma_lead_days) ** 2
    if s <= 0.0:
        return 0.0

    # Never walk a distribution with a huge mean one unit at a time: past the
    # threshold the two forms agree anyway.
    if mu > DISCRETE_QUANTILE_MAX_LAMBDA:
        return mu + z_score(s) * math.sqrt(var)

    # var >= mu by construction (see above), but guard the boundary: floating
    # point can put them a hair apart, and NB is undefined at var == mu.
    if var <= mu * (1.0 + 1e-9):
        log_pmf = -mu                       # Poisson: log P(X = 0)
        step = lambda k, lp: lp + math.log(mu) - math.log(k)
    else:
        p = mu / var                        # 0 < p < 1
        r = mu * mu / (var - mu)            # size
        log_pmf = r * math.log(p)           # NB: log P(X = 0)
        log1mp = math.log1p(-p)
        step = lambda k, lp: lp + math.log(k - 1 + r) - math.log(k) + log1mp

    # Walk up until the cdf reaches the target. The cap is generous but finite
    # so a pathological variance cannot spin here.
    cap = int(mu + 12.0 * math.sqrt(var)) + 12
    cdf = math.exp(log_pmf)
    k = 0
    while cdf < s and k < cap:
        k += 1
        log_pmf = step(k, log_pmf)
        cdf += math.exp(log_pmf)
    return float(k)


def order_up_to_level(d: float, sigma_d: float, lead_days: float,
                      review_days: float, sigma_lead_days: float,
                      z: float) -> float:
    """S = d·P + z·sigma_{D_P}, with P = R + L.

    Or, when the discrete quantile is enabled, the smallest level that meets
    the SAME service level z encodes -- see demand_quantile_over. z is
    converted back to a probability with Phi(z) rather than the caller being
    asked for a new parameter: z was only ever a way of writing a service
    target, and the service target is what both forms are answering.
    """
    if d <= 0:
        return 0.0
    P = max(0.0, float(review_days)) + max(0.0, float(lead_days))
    if use_discrete_quantile():
        service = 0.5 * (1.0 + math.erf(float(z) / math.sqrt(2.0)))
        return demand_quantile_over(P, d, sigma_d, sigma_lead_days, service)
    cycle = float(d) * P
    safety = float(z) * demand_sigma_over(P, d, sigma_d, sigma_lead_days)
    return cycle + safety


def clamp_level(S: float, d: float, shelf_life_days: float = 0.0,
                min_display: float = 0.0, min_protection: float = 0.0) -> float:
    """Physical limits are CLAMPS, not multipliers.

    A shelf life is a ceiling on what can be held at all, and a facing is a
    floor below which the shelf looks broken. Expressing either as a scaling
    factor — as the category boosts do — lets them compound with everything
    else and stop meaning what they say.
    """
    out = float(S)
    if shelf_life_days and shelf_life_days > 0 and d > 0:
        shelf_cap = float(d) * float(shelf_life_days)
        # THE CLAMP MUST NOT BIND BELOW THE PROTECTION INTERVAL.
        # If the shelf life is shorter than R + L, no order-up-to level
        # satisfies both, and clamping anyway does not avoid the waste -- it
        # converts it into a guaranteed stockout instead. On FRESH MILK and
        # BREAD, where P = 2.28 days against a 1.2-day life, that trade cost
        # 4x and 2.9x the stockout-days respectively in a 6-seed run, on the
        # exact 1,173 lines where measuring the review cadence had just lifted
        # service from 45% to 92%. The clamp was handing back most of LATA's
        # only real gain.
        #
        # So the floor is d*P and the excess over the shelf life is FORCED
        # WASTE -- a number a buyer can see and act on, by shortening the lead
        # time or changing the delivery terms. Pass min_protection=0 to get the
        # old behaviour, which is the right choice only if a stockout is
        # genuinely cheaper than spoilage on that line.
        # DEFENCE IN DEPTH. The floor is d*P, and P is only as good as R. When
        # a supplier lookup missed and R fell back to the blanket 7 days, this
        # floor turned a 1.2-day milk clamp into 8.28 days of milk -- it
        # AMPLIFIED the error instead of protecting service. A line forced to
        # hold more than MAX_SHELF_MULTIPLE times its own shelf life is not a
        # replenishment decision, it is a broken supply term, and it should be
        # capped and flagged rather than quietly ordered.
        floor = min(float(d) * float(min_protection or 0.0),
                    shelf_cap * MAX_SHELF_MULTIPLE)
        if float(shelf_life_days) <= FRESH_CYCLE_SHELF_DAYS:
            # daily-cycle fresh: the merchandising ceiling outranks the floor
            floor = min(floor, float(d) * FRESH_COVER_CEILING_DAYS)
            out = min(out, float(d) * FRESH_COVER_CEILING_DAYS)
        out = max(min(out, shelf_cap), min(floor, out))
    if min_display and min_display > 0:
        out = max(out, float(min_display))
    return max(0.0, out)


#: A line where ONE pack is more cover than this should not be bought
#: automatically. You cannot order 0.02 of a microwave, so the smallest
#: orderable quantity is months of stock -- 479 lines in the book round to
#: exactly one unit at a median 92 days of cover, and RAMTONS RM/459 lands at
#: 185. The arithmetic is not wrong about them; buying them on a replenishment
#: rule is. They belong to a special-order or transfer decision a person
#: makes, and the engine should say so instead of quietly issuing the PO.
MAX_AUTO_ORDER_COVER_DAYS = float(os.getenv("OASIS_MAX_AUTO_COVER", "60"))


def order_quantity(S: float, on_hand: float, on_order: float,
                   pack_size: float = 1.0) -> float:
    """Q = ceil((S - I - O)/pack)·pack, never negative."""
    net = float(S) - (float(on_hand) + float(on_order))
    if net <= 0:
        return 0.0
    pack = float(pack_size) if pack_size and pack_size > 0 else 1.0
    return math.ceil(net / pack) * pack


_PATTERNS_CACHE: Optional[Dict[str, dict]] = None


def default_patterns(root: Optional[str] = None) -> Dict[str, dict]:
    """The measured lead patterns, loaded once. THE canonical source for
    `sigma_lead()` -- callers should pass this, not `order_engine`'s
    `databases['supplier_patterns']` (supplier_patterns_2025.json raw),
    which is the dict that caused the original wiring bug: it loads,
    parses, and matches nothing `sigma_lead()` recognised.

    `recommend()` used to fall back to an empty dict when a caller passed no
    patterns, which meant every supplier silently took the chain-wide
    sigma_L — including the 472 the receipt history can measure. A file nobody
    loads is indistinguishable from a file that does not exist.

    Merges in `_load_secondary_patterns()` as a gap-filler UNDER the primary
    receipt-measured file -- never overriding it -- then reports the
    supplier-level join rate against the declared order schedule so a
    silent regression (a rename, a moved file) shows up as a log line
    instead of a quiet return to the 2.22 constant for everyone.
    """
    global _PATTERNS_CACHE
    if _PATTERNS_CACHE is None:
        base = root or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".."))
        primary = load_lead_patterns(base)
        secondary = _load_secondary_patterns(base)
        merged = dict(secondary)
        merged.update(primary)          # measured file wins where both exist
        _PATTERNS_CACHE = merged

        # T1-style static guard: what fraction of the DECLARED supplier book
        # (the order schedule, not just the patterns file's own vendor list)
        # would resolve a measured sigma_L. This catches "the file loads but
        # keys nothing real" at load time, before a single order line is
        # scored; check_join_rate() catches the same failure at run time,
        # per actual order-line traffic.
        try:
            schedule = load_review_schedule(base)
        except Exception:
            schedule = {}
        if schedule:
            universe = [k for k in schedule if k != "__counts__"]
            hits = sum(1 for s in universe if s in merged)
            rate = hits / len(universe) if universe else 0.0
            msg = (f"sigma_L patterns join (static, vs declared schedule): "
                  f"{hits}/{len(universe)} suppliers ({rate:.1%})")
            if rate < JOIN_RATE_FLOOR:
                logger.warning(msg + f" — below the {JOIN_RATE_FLOOR:.0%} "
                              "floor; a patterns file that loads and "
                              "matches almost nothing is worse than none "
                              "(T1: silent join failure)")
            else:
                logger.info(msg)
    return _PATTERNS_CACHE


def recommend(product: Dict[str, Any],
              schedule: Optional[Dict[str, float]] = None,
              patterns: Optional[Dict[str, dict]] = None,
              z: Optional[float] = None,
              mode: Optional[str] = None) -> Dict[str, Any]:
    """One line's order, with every term it was built from.

    Returns the terms as well as the quantity: a number nobody can decompose is
    a number nobody can argue with, and the whole point of this form is that
    each part is separately checkable.
    """
    d = float(product.get("avg_daily_sales") or 0)
    if d <= 0:
        return {"quantity": 0.0, "reason": "no measured sales rate"}

    supplier = supplier_key(product.get("supplier_name"))
    # sigma_d IS A DAILY STANDARD DEVIATION, so the cv that builds it must be
    # a daily one.
    #
    # This used to read `product['demand_cv'] or demand_cv(d)`, and the field
    # it preferred is written by enrichment as
    # _calculate_cv(sales_data['monthly_sales']) -- stdev/mean over MONTHLY
    # TOTALS. Feeding a monthly cv into sigma_d = cv * d states that a day
    # varies as little as a month does, which is the aggregation working
    # backwards: totals over 30 days are far steadier in relative terms than
    # the days inside them.
    #
    # It was not a corner case. The supplied value won on 14,525 of 15,037
    # lines (96.6%), was smaller on 99.3% of them (median ratio 0.244 against
    # the daily figure), and left the safety term at 0.334x of what the
    # formula asks for -- about a third. It also made the velocity model inert
    # on all but 512 lines, which is why perturbing phi across its whole
    # plausible range moved not one line of the order book.
    #
    # A genuinely DAILY measurement should still win when one exists; nothing
    # produces one today, and the name has to say which interval it means.
    # The monthly figure remains useful as a trend signal and is left where it
    # is for the surfaces that read it -- it is simply not this input.
    cv = float(product.get("demand_cv_daily") or 0) or demand_cv(d)
    sigma_d = cv * d
    L = max(1.0, float(product.get("lead_time_days")
                       or product.get("estimated_delivery_days") or 3))
    R = review_period(supplier, schedule, mode=mode,
                      cadence=product.get("_cadence"))
    pats = default_patterns() if patterns is None else patterns
    sL = sigma_lead(pats.get(supplier))
    zz = z_score() if z is None else z

    S_raw = order_up_to_level(d, sigma_d, L, R, sL, zz)
    # A shelf life given on the line wins; otherwise the department's.
    _sl = float(product.get("shelf_life_days") or 0) \
        or shelf_life_for(product.get("department") or "",
                          sku=product.get("sku") or product.get("id")
                              or product.get("product_name"))
    S = clamp_level(S_raw, d,
                    shelf_life_days=_sl, min_protection=(R + L),
                    min_display=float(product.get("min_presentation_stock") or 0))
    I = float(product.get("current_stock")
              if product.get("current_stock") is not None
              else product.get("current_stocks") or 0)
    O = float(product.get("on_order_qty") or 0)
    Q = order_quantity(S, I, O, float(product.get("pack_size") or 1))
    # TRIED AND REJECTED, on measurement: max(60, 2 * (R + L)) instead of the
    # flat 60. The argument was good -- a line reviewed monthly is exposed 37
    # days by construction, so a pack covering 70 of them is under two cycles
    # rather than parked capital -- and it is why this cap makes the engine
    # size-dependent at all, since the rule is in days but the PACK is
    # absolute. It does not survive contact with the book: only 90 lines carry
    # a measured cadence above 28 days, and they barely overlap the population
    # the cap refuses. Perturbing the multiplier from 2 to 6 moved 13 lines at
    # a 0.3x store against 19 at 2.5x -- it WIDENED the size gap by 6 lines,
    # the opposite of its purpose.
    #
    # What does move that gap, measured the same way: the supplier MOT (+188
    # lines at 0.3x), the per-SKU MOP (+144 fresh), and the service level
    # (+72 at 0.95). Those are absolute shillings and an explicit target --
    # policy, not this cap. Raising the cap itself to 365 gives +81/-54, so it
    # is a real lever, but a blunt one that mostly just buys more stock.
    #
    # THE CAP JUDGES A ROUNDED NUMBER, SO ROUND TOWARDS IT, DON'T REFUSE.
    # Measured on the whole book: on all 476 lines this rule fires, S/d is
    # already INSIDE the cap. It never rejects the level the engine computed
    # -- only that level rounded up to a whole sellable pack. Two cases hide
    # inside that, and they deserve different answers:
    #
    #   374 lines  pack/d > cap. One pack IS more cover than the cap allows,
    #              so NO orderable quantity satisfies it. Refusing is the only
    #              option and the suppression message is true.
    #
    #   102 lines  pack/d <= cap. A quantity does fit -- one pack fewer -- and
    #              the engine was ordering nothing instead. JW 145G TUNA:
    #              d=0.147, pack=1, S=8.2 units (56 days, inside the cap);
    #              ceil gives 9 units = 61 days, so it bought zero. Choosing
    #              between 54 days and 61 days, it took 0.
    #
    # Ordering the largest whole pack that fits misses S by less than one
    # pack; refusing misses it by all of S. The second is strictly worse on
    # any loss function, and the cap is still honoured exactly -- the position
    # after this branch can never exceed MAX_AUTO_ORDER_COVER_DAYS.
    _dead = False
    if Q > 0 and d > 0 and (I + Q) / d > MAX_AUTO_ORDER_COVER_DAYS:
        _pack = float(product.get("pack_size") or 1) or 1.0
        _room = MAX_AUTO_ORDER_COVER_DAYS * d - I
        Q = math.floor(_room / _pack) * _pack if _room > 0 else 0.0
        if Q <= 0:
            _dead = True
            Q = 0.0

    P = R + L

    # A CLAMP BELOW THE PROTECTION INTERVAL IS NOT A POLICY, IT IS A PLANNED
    # STOCKOUT. If the product dies before the next delivery can arrive, no
    # order-up-to level exists that both respects the shelf life and covers
    # the exposure window. The engine must say so rather than return a number
    # that looks like an answer: 188 of 311 clamped fresh lines sit here, and
    # fresh milk lands on a 2.1% fill rate against a 90% target.
    #
    # The resolution is never arithmetic. It is one of:
    #   shorten L      same-day drop, so R + L falls under the shelf life
    #   accept waste   hold d*P and write off the difference
    #   accept gaps    hold d*shelf_life and be empty part of the cycle
    # and which one is right is a buyer's decision, not a formula's.
    _cycle = d * P
    _sigma_P = demand_sigma_over(P, d, sigma_d, sL)
    _infeasible = bool(_sl and S < _cycle - 1e-9)
    _service = None
    if _sigma_P > 0:
        _service = 0.5 * (1.0 + math.erf((S - _cycle) / (_sigma_P * math.sqrt(2.0))))
    return {
        "feasible": not _infeasible,
        "auto_order_suppressed": _dead,
        "suppress_reason": ("one pack exceeds "
                            f"{MAX_AUTO_ORDER_COVER_DAYS:.0f} days of cover -- "
                            "special order or transfer, not replenishment")
        if _dead else None,
        "forced_waste_units_per_cycle": max(0.0, S - (d * _sl)) if _sl else 0.0,
        "binding": ("shelf_life" if _infeasible else
                    "shelf_life_slack" if (_sl and abs(S - S_raw) > 1e-9) else "service"),
        "implied_service": _service,
        "min_feasible_cover_days": P,
        "quantity": Q, "S": S, "S_unclamped": S_raw,
        "R": R, "L": L, "P": P, "d": d, "sigma_d": sigma_d,
        "sigma_lead": sL, "z": zz,
        "mode": (mode or ordering_mode()),
        "scheduled": bool(schedule and " ".join(supplier.split()) in schedule),
        "cycle_stock": d * P,
        "safety_stock": zz * demand_sigma_over(P, d, sigma_d, sL),
        "clamped": abs(S - S_raw) > 1e-9, "shelf_life_days": _sl,
        "R_source": _r_source(supplier, schedule, product.get("_cadence")),
        "cover_days": ((I + Q) / d) if d > 0 else 0.0,
    }


def describe(terms: Dict[str, Any]) -> str:
    """The order in words, term by term — for the review queue."""
    if not terms.get("quantity"):
        return "No order: position already covers the protection interval."
    return (
        "Reviewed every {R:.0f}d, delivered in {L:.0f}d, so this line has to "
        "survive {P:.0f} days. At {d:.2f}/day that is {cyc:.0f} units, plus "
        "{saf:.0f} for variability ({z:.2f} sigma, of which the supplier's own "
        "lead-time spread of {sl:.1f}d is the larger part). Order-up-to "
        "{S:.0f}; on hand and on order come to {have:.0f}."
    ).format(R=terms["R"], L=terms["L"], P=terms["P"], d=terms["d"],
             cyc=terms["cycle_stock"], saf=terms["safety_stock"],
             z=terms["z"], sl=terms["sigma_lead"], S=terms["S"],
             have=terms["S"] - terms["quantity"])
