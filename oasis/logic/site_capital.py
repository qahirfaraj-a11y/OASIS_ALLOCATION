"""
From a candidate location to a proposed opening capital — calibrated on the
client's OWN estate.

THE PROBLEM THIS SOLVES
    ``site_scoring`` answers "what share of this catchment could a store here
    take?". That is a fraction, and a fraction cannot open a store. The two
    steps that turn it into a decision were missing, and the one that existed
    was circular:

    * ``recommend_format(capture_pct)`` claimed to recommend a store size, but
      capture is computed FROM the size the operator typed in. Measured on a
      fixed location with a fixed competitor set, the recommendation tracked the
      input one-for-one — 3,000 sqft returned "Unsuitable", 80,000 sqft returned
      "Hyper / Flagship". It restated the question as its answer.
    * Nothing produced a capital figure. ``StoreProfileManager`` runs the other
      way round: it takes a budget and returns a tier. So the chain stopped at a
      string, and the operator still had to guess the number that mattered.

THE IDENTITY THIS IS BUILT ON
    Huff gives the share; revenue is the share times the size of the prize:

        revenue_i = capture_i * catchment_demand_i

    Every term on the left is observable for a store that already trades. So
    the demand around an existing store is not assumed, it is *measured*:

        catchment_demand_i = revenue_i / capture_i

    That is the whole idea. The estate is a labelled dataset — the only one in
    this platform built from outcomes rather than from a formula. Each existing
    store contributes one observation: where it is, how big it is, what it
    actually sold, and what stock it actually carries.

WHAT IS ASSUMED, STATED PLAINLY
    That a candidate's catchment resembles the estate's. OASIS has no
    population, income or footfall data, so it CANNOT know that an underserved
    suburb differs from an empty field. This module therefore never returns a
    point estimate on its own: it returns a range built from the observed
    spread of the client's own stores, so the operator can see how much that
    assumption is worth on their estate. A chain whose stores vary tenfold gets
    a tenfold band, which is the honest answer.

THE GATE
    The capture model is allowed to produce a calibrated number only if it
    beats two dumber predictors under leave-one-out cross-validation on the
    estate:

        (a) revenue proportional to floor area alone
        (b) the estate's median revenue, ignoring the site entirely

    This is the same discipline the store-GNN work uses, applied from the start
    rather than retrofitted. If the geography adds nothing over "big stores
    sell more", it does not get to set a budget; the caller falls back to the
    productivity basis and is told so. Nothing here silently upgrades itself.

WHY THE SIZE RECOMMENDATION IS NO LONGER CIRCULAR
    Huff share saturates: doubling the floor area less than doubles the share,
    because the site is competing against a fixed field. So predicted revenue
    per square foot FALLS as the store grows, and there is a real crossing
    point against an external anchor — the productivity the client's own stores
    actually achieve. The recommendation is that crossing, not the input.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional, Sequence

from .site_scoring import CATCHMENT_KM, DEFAULT_SIZE_SQFT, score_site

logger = logging.getLogger("OASIS.SiteCapital")

#: Below this many usable observations the capture model cannot be
#: cross-validated at all — leave-one-out on three stores is noise, not
#: evidence. The caller falls back to the productivity basis and is told.
MIN_CALIBRATION_STORES = 4

#: A capture this small makes ``revenue / capture`` explode. A store taking
#: under half a percent of its catchment is either mis-placed on the map or
#: sitting inside a competitor's shadow; either way it cannot calibrate demand.
MIN_USABLE_CAPTURE = 0.005

#: Floor areas considered when recommending a size, smallest first (sq ft).
SIZE_LADDER = (2_500.0, 5_000.0, 10_000.0, 20_000.0, 35_000.0, 60_000.0)

#: Names for those rungs, so the operator sees a format and not just a number.
SIZE_FORMATS = {
    2_500.0: "Kiosk / Duka",
    5_000.0: "Express / Neighbourhood",
    10_000.0: "Mini-Mart",
    20_000.0: "Medium Anchor",
    35_000.0: "Supermarket",
    60_000.0: "Hyper / Flagship",
}

#: A candidate may out-earn the estate's best store, but a site that models
#: more than this multiple of the estate's median implied demand is almost
#: always a mapping error (a store placed on the wrong side of the world, or a
#: candidate in an empty quadrant). Cap the claim and say why.
MAX_DEMAND_MULTIPLE = 3.0


# ── small pure statistics (no numpy: this runs in the client's console) ──────
def _median(xs: Sequence[float]) -> float:
    vals = sorted(float(x) for x in xs)
    n = len(vals)
    if n == 0:
        return 0.0
    mid = n // 2
    return vals[mid] if n % 2 else (vals[mid - 1] + vals[mid]) / 2.0


def _quantile(xs: Sequence[float], q: float) -> float:
    vals = sorted(float(x) for x in xs)
    if not vals:
        return 0.0
    if len(vals) == 1:
        return vals[0]
    pos = q * (len(vals) - 1)
    lo = int(pos)
    hi = min(lo + 1, len(vals) - 1)
    return vals[lo] + (vals[hi] - vals[lo]) * (pos - lo)


def _mape(pairs: Sequence[tuple]) -> float:
    """Median absolute percentage error over (actual, predicted) pairs."""
    errs = [abs(p - a) / a for a, p in pairs if a and a > 0]
    return round(_median(errs), 4) if errs else 1.0


# ── step 1: measure the estate ──────────────────────────────────────────────
def estate_observations(stores: Sequence[Dict[str, Any]],
                        competitors: Sequence[Dict[str, Any]] = (),
                        revenue_by_org: Optional[Dict[str, float]] = None,
                        stock_value_by_org: Optional[Dict[str, float]] = None,
                        catchment_km: float = CATCHMENT_KM) -> List[Dict[str, Any]]:
    """One observation per located, trading store — the labelled dataset.

    Each store is scored **leave-one-out**: as if it were a candidate site,
    against the OTHER stores and the competitors. Scoring it against itself
    would put a store on top of itself at the distance floor and hand it a
    near-total share, which is the failure mode the ring sampling in
    ``site_scoring`` exists to avoid.

    Its floor area is a measurement here, not an assumption, so using it
    introduces none of the circularity that broke ``recommend_format``.
    """
    revenue_by_org = revenue_by_org or {}
    stock_value_by_org = stock_value_by_org or {}
    out: List[Dict[str, Any]] = []

    for i, s in enumerate(stores or []):
        org = str(s.get("org_cd") or s.get("ORG_CD") or f"#{i}")
        rev = float(revenue_by_org.get(org, 0) or 0)
        if rev <= 0:
            continue                      # no outcome -> not an observation
        lat, lon = s.get("lat"), s.get("lon")
        if lat is None or lon is None:
            continue                      # not placed -> cannot be scored
        size = float(s.get("size_sqft") or DEFAULT_SIZE_SQFT)

        others = [o for j, o in enumerate(stores) if j != i]
        sc = score_site(float(lat), float(lon), others, competitors,
                        size_sqft=size, catchment_km=catchment_km)
        capture = sc["capture_pct"] / 100.0
        if capture < MIN_USABLE_CAPTURE:
            continue                      # demand estimate would explode

        out.append({
            "org_cd": org,
            "name": s.get("name") or org,
            "size_sqft": size,
            "capture": capture,
            "revenue": rev,
            "stock_value": float(stock_value_by_org.get(org, 0) or 0),
            # The measurement this module exists for: how big is the prize
            # around a store that we KNOW trades this well?
            "implied_demand": rev / capture,
            "revenue_per_sqft": rev / size if size > 0 else 0.0,
        })
    return out


def calibrate(observations: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Reduce the estate observations to the constants a candidate needs."""
    obs = list(observations or [])
    n = len(obs)
    if n == 0:
        return {"n": 0, "usable": False,
                "reason": "no store has both a location and recorded sales"}

    demands = [o["implied_demand"] for o in obs]
    rps = [o["revenue_per_sqft"] for o in obs]
    covers = [o["stock_value"] / o["revenue"]
              for o in obs if o["revenue"] > 0 and o["stock_value"] > 0]

    med_demand = _median(demands)
    spread = (_quantile(demands, 0.75) / _quantile(demands, 0.25)
              if _quantile(demands, 0.25) > 0 else 0.0)

    return {
        "n": n,
        "usable": True,
        "median_demand": med_demand,
        "demand_p25": _quantile(demands, 0.25),
        "demand_p75": _quantile(demands, 0.75),
        # How far apart the client's own catchments are. This is the honest
        # width of any claim made about a site we have never traded in.
        "demand_spread_ratio": round(spread, 2),
        "median_revenue_per_sqft": _median(rps),
        "median_revenue": _median([o["revenue"] for o in obs]),
        # Working capital intensity, measured — NOT a rule of thumb. This is
        # the chain's own stock-to-sales ratio over the same period.
        "cover_ratio": _median(covers) if covers else 0.0,
        "cover_measured": bool(covers),
    }


# ── step 2: the gate ────────────────────────────────────────────────────────
def loo_validate(observations: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Leave-one-out: does the geography beat two dumber predictors?

    Predictors, each refitted on the n-1 remaining stores so no fold sees its
    own answer:

      capture   revenue = capture_i * median(implied_demand of the others)
      sqft      revenue = size_i    * median(revenue_per_sqft of the others)
      mean      revenue = median(revenue of the others)

    The capture model earns the right to set a budget only by beating BOTH. A
    tie is a loss: the simpler predictor wins by default, because it needs no
    map, no competitor file and no assumption about catchments.
    """
    obs = list(observations or [])
    n = len(obs)
    if n < MIN_CALIBRATION_STORES:
        return {"n": n, "validated": False,
                "reason": (f"{n} usable store(s); leave-one-out needs at least "
                           f"{MIN_CALIBRATION_STORES} to mean anything")}

    cap_pairs, sqft_pairs, mean_pairs = [], [], []
    for i, o in enumerate(obs):
        rest = [x for j, x in enumerate(obs) if j != i]
        d_hat = _median([r["implied_demand"] for r in rest])
        rps_hat = _median([r["revenue_per_sqft"] for r in rest])
        mean_hat = _median([r["revenue"] for r in rest])
        cap_pairs.append((o["revenue"], o["capture"] * d_hat))
        sqft_pairs.append((o["revenue"], o["size_sqft"] * rps_hat))
        mean_pairs.append((o["revenue"], mean_hat))

    e_cap, e_sqft, e_mean = _mape(cap_pairs), _mape(sqft_pairs), _mape(mean_pairs)
    validated = e_cap < e_sqft and e_cap < e_mean

    return {
        "n": n,
        "validated": validated,
        "mape_capture": e_cap,
        "mape_sqft_only": e_sqft,
        "mape_estate_median": e_mean,
        "beaten_by": (None if validated
                      else ("floor area alone" if e_sqft <= e_cap
                            else "the estate median")),
        "reason": (None if validated else
                   "the location adds nothing over the simpler predictor on "
                   "this estate - capital is proposed from productivity "
                   "instead, and the site score stays a ranking tool"),
    }


# ── step 3: the proposal ────────────────────────────────────────────────────
def propose_capital(capture_pct: float, size_sqft: float,
                    calibration: Dict[str, Any],
                    validation: Optional[Dict[str, Any]] = None,
                    isolated: bool = False) -> Dict[str, Any]:
    """Opening stock capital for a candidate, with the basis it rests on.

    Returns a ``basis`` the caller must show, because the three bases carry
    very different weight:

      ``estate-calibrated``   geography beat the baselines; capture x measured
                              catchment demand, banded by the estate's spread.
      ``estate-productivity`` geography did NOT beat floor area; the number is
                              this chain's own revenue per square foot and the
                              site does not enter it.
      ``insufficient-data``   no number. Says what is missing.

    ``isolated`` sites are refused outright: a full share of an empty catchment
    is arithmetically 100% and means nothing, and dressing that as a flagship
    budget is exactly the mistake this module was written to stop.
    """
    size = max(0.0, float(size_sqft or 0))
    capture = max(0.0, float(capture_pct or 0)) / 100.0
    cal = calibration or {}
    val = validation or {}

    def _shell(basis: str, note: str) -> Dict[str, Any]:
        return {"basis": basis, "expected_revenue": None, "opening_capital": None,
                "capital_low": None, "capital_high": None,
                "cover_ratio": cal.get("cover_ratio") or 0.0,
                "cover_measured": bool(cal.get("cover_measured")),
                "note": note}

    if isolated:
        return _shell("insufficient-data",
                      "Nothing within the catchment - a full share of an empty "
                      "area is still a full share, and carries no evidence of "
                      "demand. Bring a catchment estimate before budgeting.")
    if not cal.get("usable"):
        return _shell("insufficient-data",
                      cal.get("reason") or "the estate carries no usable "
                      "observation: place your stores and connect their sales.")

    cover = float(cal.get("cover_ratio") or 0.0)
    cover_measured = bool(cal.get("cover_measured"))
    if not cover_measured:
        # Without stock values there is no measured stock-to-sales ratio, and
        # inventing one would be the sort of unlabelled constant this whole
        # exercise exists to remove. Revenue is still reportable.
        cover = 0.0

    if val.get("validated"):
        d_med = float(cal.get("median_demand") or 0.0)
        d_lo = float(cal.get("demand_p25") or d_med)
        d_hi = min(float(cal.get("demand_p75") or d_med),
                   d_med * MAX_DEMAND_MULTIPLE)
        rev = capture * d_med
        rev_lo, rev_hi = capture * d_lo, capture * d_hi
        basis = "estate-calibrated"
        note = (f"Capture x the demand implied by {cal['n']} of your own "
                f"stores. Their catchments differ by "
                f"{cal.get('demand_spread_ratio', 0):.1f}x, which is the width "
                f"of this band.")
    else:
        rps = float(cal.get("median_revenue_per_sqft") or 0.0)
        rev = size * rps
        rev_lo, rev_hi = rev * 0.6, rev * 1.5
        basis = "estate-productivity"
        note = ((val.get("reason") or
                 "not enough located, trading stores to validate the "
                 "geography") +
                f" - sized from your median {rps:,.0f} per sq ft.")

    return {
        "basis": basis,
        "expected_revenue": round(rev, 0),
        "revenue_low": round(rev_lo, 0),
        "revenue_high": round(rev_hi, 0),
        "opening_capital": round(rev * cover, 0) if cover > 0 else None,
        "capital_low": round(rev_lo * cover, 0) if cover > 0 else None,
        "capital_high": round(rev_hi * cover, 0) if cover > 0 else None,
        "cover_ratio": round(cover, 4),
        "cover_measured": cover_measured,
        "note": note if cover > 0 else
                note + " Stock values are missing, so no capital figure can be "
                       "derived - only expected revenue.",
    }


# ── step 4: the size recommendation that is not circular ────────────────────
def recommend_size(score_fn: Callable[[float], float],
                   calibration: Dict[str, Any],
                   validation: Optional[Dict[str, Any]] = None,
                   ladder: Sequence[float] = SIZE_LADDER) -> Dict[str, Any]:
    """Largest floor area that still clears the estate's own productivity.

    ``score_fn(size_sqft) -> capture_pct`` re-scores the SAME location at each
    rung, so the saturation of the Huff share does the work: a bigger store
    takes a larger share of a fixed catchment, but less than proportionally, so
    predicted revenue per square foot falls monotonically. The recommendation
    is the last rung at or above the productivity the client's own stores
    achieve — an external anchor, which is precisely what the old
    ``recommend_format`` lacked.
    """
    cal = calibration or {}
    if not cal.get("usable"):
        return {"recommended_sqft": None, "format": None, "rungs": [],
                "note": "no estate productivity to measure a site against."}

    anchor = float(cal.get("median_revenue_per_sqft") or 0.0)
    d_med = float(cal.get("median_demand") or 0.0)
    calibrated = bool((validation or {}).get("validated"))

    rungs = []
    best = None
    for size in ladder:
        cap = max(0.0, float(score_fn(size))) / 100.0
        rev = cap * d_med
        rps = rev / size if size > 0 else 0.0
        clears = rps >= anchor and anchor > 0
        rungs.append({"size_sqft": size,
                      "format": SIZE_FORMATS.get(size, f"{size:,.0f} sq ft"),
                      "capture_pct": round(cap * 100, 2),
                      "revenue_per_sqft": round(rps, 2),
                      "clears_estate": clears})
        if clears:
            best = size

    if not calibrated:
        note = ("Indicative only - the geography did not beat floor area on "
                "your estate, so treat this as a ranking, not a size decision.")
    elif best is None:
        note = (f"No format clears your median {anchor:,.0f} per sq ft here. "
                "The catchment is too contested to carry a store at your "
                "chain's usual productivity.")
    else:
        note = (f"Largest format whose modelled revenue still clears your own "
                f"median of {anchor:,.0f} per sq ft.")

    return {
        "recommended_sqft": best,
        "format": SIZE_FORMATS.get(best) if best else "None at this location",
        "productivity_anchor": round(anchor, 2),
        "rungs": rungs,
        "calibrated": calibrated,
        "note": note,
    }


# ── reporting ───────────────────────────────────────────────────────────────
def format_report(cal: Dict[str, Any], val: Dict[str, Any],
                  proposal: Optional[Dict[str, Any]] = None) -> str:
    """ASCII only — this prints to a customer's Windows console."""
    w = ["", "O.A.S.I.S. - site capital calibration", "=" * 62]
    w.append(f"  stores used            {cal.get('n', 0):>12,}")
    if not cal.get("usable"):
        w.append(f"  UNUSABLE: {cal.get('reason', '')}")
        return "\n".join(w)
    w.append(f"  median implied demand  {cal.get('median_demand', 0):>12,.0f}")
    w.append(f"  catchment spread       {cal.get('demand_spread_ratio', 0):>12.2f}x")
    w.append(f"  median revenue/sq ft   {cal.get('median_revenue_per_sqft', 0):>12,.2f}")
    w.append(f"  stock-to-sales ratio   {cal.get('cover_ratio', 0):>12.4f}"
             + ("" if cal.get("cover_measured") else "   (not measured)"))
    w.append("")
    w.append("  leave-one-out validation")
    if not val.get("validated"):
        w.append(f"     NOT VALIDATED - {val.get('reason', '')}")
    if "mape_capture" in val:
        w.append(f"     capture model      {val['mape_capture']:>8.1%}  median abs error")
        w.append(f"     floor area alone   {val['mape_sqft_only']:>8.1%}")
        w.append(f"     estate median      {val['mape_estate_median']:>8.1%}")
        w.append(f"     -> geography earns a budget: "
                 f"{'YES' if val.get('validated') else 'NO'}")
    if proposal:
        w.append("")
        w.append(f"  proposal basis         {proposal.get('basis')}")
        if proposal.get("expected_revenue") is not None:
            w.append(f"  expected revenue       {proposal['expected_revenue']:>12,.0f}"
                     f"   ({proposal['revenue_low']:,.0f} - {proposal['revenue_high']:,.0f})")
        if proposal.get("opening_capital") is not None:
            w.append(f"  OPENING CAPITAL        {proposal['opening_capital']:>12,.0f}"
                     f"   ({proposal['capital_low']:,.0f} - {proposal['capital_high']:,.0f})")
        w.append(f"     {proposal.get('note', '')}")
    w.append("")
    return "\n".join(w)
