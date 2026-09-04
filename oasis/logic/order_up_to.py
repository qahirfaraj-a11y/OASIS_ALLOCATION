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
from typing import Any, Dict, Optional

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


def is_enabled() -> bool:
    return model_name() in ("order_up_to", "order-up-to", "newsvendor")


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


#: Under on-demand ordering there is no cycle to wait for: the next chance to
#: buy is the next working day. R collapses to that, not to a week.
ON_DEMAND_REVIEW_DAYS = 1.0

MODES = ("scheduled", "on_demand")


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
                  mode: Optional[str] = None) -> float:
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
    key = " ".join(str(supplier or "").upper().split())
    if schedule and key in schedule:
        return schedule[key]
    mode = (mode or ordering_mode()).lower()
    if mode == "on_demand":
        return ON_DEMAND_REVIEW_DAYS
    return default


# ── sigma_L: how much the supplier's lead time actually moves ─────────────
def sigma_lead(pattern: Optional[dict],
               default: float = DEFAULT_SIGMA_LEAD) -> float:
    """Lead-time standard deviation for a supplier.

    Prefers a measured value on the supplier's pattern; falls back to the
    chain-wide figure. Never zero: a supplier we have not measured is not a
    supplier who always delivers on time, and treating it as one deletes the
    larger half of the safety term.
    """
    if isinstance(pattern, dict):
        for key in ("lead_time_stdev", "lead_time_std", "lead_stdev"):
            v = pattern.get(key)
            if v is not None:
                try:
                    v = float(v)
                except (TypeError, ValueError):
                    continue
                if v >= 0:
                    return v
    return default


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


def order_up_to_level(d: float, sigma_d: float, lead_days: float,
                      review_days: float, sigma_lead_days: float,
                      z: float) -> float:
    """S = d·P + z·sigma_{D_P}, with P = R + L."""
    if d <= 0:
        return 0.0
    P = max(0.0, float(review_days)) + max(0.0, float(lead_days))
    cycle = float(d) * P
    safety = float(z) * demand_sigma_over(P, d, sigma_d, sigma_lead_days)
    return cycle + safety


def clamp_level(S: float, d: float, shelf_life_days: float = 0.0,
                min_display: float = 0.0) -> float:
    """Physical limits are CLAMPS, not multipliers.

    A shelf life is a ceiling on what can be held at all, and a facing is a
    floor below which the shelf looks broken. Expressing either as a scaling
    factor — as the category boosts do — lets them compound with everything
    else and stop meaning what they say.
    """
    out = float(S)
    if shelf_life_days and shelf_life_days > 0 and d > 0:
        out = min(out, float(d) * float(shelf_life_days))
    if min_display and min_display > 0:
        out = max(out, float(min_display))
    return max(0.0, out)


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
    """The measured lead patterns, loaded once.

    `recommend()` used to fall back to an empty dict when a caller passed no
    patterns, which meant every supplier silently took the chain-wide
    sigma_L — including the 472 the receipt history can measure. A file nobody
    loads is indistinguishable from a file that does not exist.
    """
    global _PATTERNS_CACHE
    if _PATTERNS_CACHE is None:
        base = root or os.path.abspath(
            os.path.join(os.path.dirname(__file__), "..", ".."))
        _PATTERNS_CACHE = load_lead_patterns(base)
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

    supplier = str(product.get("supplier_name") or "").upper().strip()
    cv = float(product.get("demand_cv") or 0.4)
    sigma_d = cv * d
    L = max(1.0, float(product.get("lead_time_days")
                       or product.get("estimated_delivery_days") or 3))
    R = review_period(supplier, schedule, mode=mode)
    pats = default_patterns() if patterns is None else patterns
    sL = sigma_lead(pats.get(supplier))
    zz = z_score() if z is None else z

    S_raw = order_up_to_level(d, sigma_d, L, R, sL, zz)
    S = clamp_level(S_raw, d,
                    shelf_life_days=float(product.get("shelf_life_days") or 0),
                    min_display=float(product.get("min_presentation_stock") or 0))
    I = float(product.get("current_stock")
              if product.get("current_stock") is not None
              else product.get("current_stocks") or 0)
    O = float(product.get("on_order_qty") or 0)
    Q = order_quantity(S, I, O, float(product.get("pack_size") or 1))

    P = R + L
    return {
        "quantity": Q, "S": S, "S_unclamped": S_raw,
        "R": R, "L": L, "P": P, "d": d, "sigma_d": sigma_d,
        "sigma_lead": sL, "z": zz,
        "mode": (mode or ordering_mode()),
        "scheduled": bool(schedule and " ".join(supplier.split()) in schedule),
        "cycle_stock": d * P,
        "safety_stock": zz * demand_sigma_over(P, d, sigma_d, sL),
        "clamped": abs(S - S_raw) > 1e-9,
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
