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

from .site_scoring import (BETA_RANGE, CATCHMENT_KM, DEFAULT_SIZE_SQFT,
                           DISTANCE_DECAY, build_field, build_geometry,
                           score_site)

logger = logging.getLogger("OASIS.SiteCapital")

#: Below this many usable observations the capture model cannot be
#: cross-validated at all — leave-one-out on three stores is noise, not
#: evidence. The caller falls back to the productivity basis and is told.
MIN_CALIBRATION_STORES = 4

#: A capture this small makes ``revenue / capture`` explode. A store taking
#: under half a percent of its catchment is either mis-placed on the map or
#: sitting inside a competitor's shadow; either way it cannot calibrate demand.
MIN_USABLE_CAPTURE = 0.005

#: How much better than the best dumb predictor a geographic model must be
#: before it may set a budget, as a RELATIVE reduction in median error.
#:
#: The gate used to be a strict inequality: any margin at all counted as a win.
#: Measured, that is not a gate. ``loo_validate`` compares MEDIANS of five or
#: six absolute percentage errors, and with n=5 the median IS the third value —
#: one store moving flips it. On one estate the population model "beat" floor
#: area by 4% against 5%, and on another capture and floor area tied at 5% and a
#: winner was still declared. Those are ties being read as wins.
#:
#: 10% relative is a modest bar deliberately: it is not a significance test and
#: does not pretend to be. The significance question is answered separately by
#: the paired sign test below, which on an estate this small will usually say
#: "not conclusive" — and saying so is the point.
MIN_GATE_MARGIN = 0.10

#: Above this sign-test p-value the win is reported as PROVISIONAL. A geographic
#: model that beats the baseline on 4 of 6 stores has a 34% chance of doing that
#: by coin flip, and the operator should be told so rather than shown a verdict.
PROVISIONAL_P = 0.10


class Observations(list):
    """The estate observations, carrying what was LEFT OUT and why.

    A plain list was the defect. ``estate_observations`` skips a store on three
    separate conditions — no recorded sales, no location, capture below the
    floor — and returned no account of any of them. Measured on an eight-store
    estate, widening the catchment to 20 km silently dropped two stores below
    the capture floor and took n from 8 to 6, with nothing in the return value,
    the report or the console saying so. The operator reads "stores used: 6" as
    their estate.

    Subclasses ``list`` so every existing caller keeps working unchanged; the
    accounting rides along with the number instead of being separated from it,
    which is the whole failure being fixed.
    """

    __slots__ = ("skipped",)

    def __init__(self, rows=(), skipped=None):
        super().__init__(rows)
        self.skipped = list(skipped or [])

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


def _errors(pairs: Sequence[tuple]) -> List[float]:
    """Per-fold absolute percentage errors over (actual, predicted) pairs."""
    return [abs(p - a) / a for a, p in pairs if a and a > 0]


def _mape(pairs: Sequence[tuple]) -> float:
    """Median absolute percentage error over (actual, predicted) pairs."""
    errs = _errors(pairs)
    return round(_median(errs), 4) if errs else 1.0


def _sign_test(a: Sequence[float], b: Sequence[float]) -> Dict[str, Any]:
    """Paired sign test: does ``a`` beat ``b`` more often than a coin would?

    The folds are PAIRED — the same held-out store, predicted two ways — so the
    question "did the geography win?" has a per-store answer, and counting those
    answers asks something a median cannot: is the margin bigger than the noise?

    Reported, never hidden. On five or six stores this will usually say the win
    could be chance, and that is the honest reading of five stores.
    """
    wins = sum(1 for x, y in zip(a, b) if x < y)
    ties = sum(1 for x, y in zip(a, b) if x == y)
    n = len(a) - ties            # ties carry no evidence either way
    if n <= 0:
        return {"folds": 0, "wins": wins, "p": 1.0}
    # Two-sided binomial tail at p=0.5, computed exactly — no scipy on a
    # client console.
    def _c(n_, k_):
        r = 1
        for i in range(k_):
            r = r * (n_ - i) // (i + 1)
        return r
    tail = sum(_c(n, k) for k in range(wins, n + 1)) / (2.0 ** n)
    return {"folds": n, "wins": wins, "p": round(min(1.0, 2.0 * tail), 4)}


# ── step 1: measure the estate ──────────────────────────────────────────────
def estate_observations(stores: Sequence[Dict[str, Any]],
                        competitors: Sequence[Dict[str, Any]] = (),
                        revenue_by_org: Optional[Dict[str, float]] = None,
                        stock_value_by_org: Optional[Dict[str, float]] = None,
                        catchment_km: float = CATCHMENT_KM,
                        population: Any = None,
                        affluence: Any = None,
                        beta: float = DISTANCE_DECAY,
                        geometries: Optional[Dict[int, Any]] = None
                        ) -> "Observations":
    """One observation per located, trading store — the labelled dataset.

    Each store is scored **leave-one-out**: as if it were a candidate site,
    against the OTHER stores and the competitors. Scoring it against itself
    would put a store on top of itself at the distance floor and hand it a
    near-total share, which is the failure mode the ring sampling in
    ``site_scoring`` exists to avoid.

    Its floor area is a measurement here, not an assumption, so using it
    introduces none of the circularity that broke ``recommend_format``.

    ``beta`` must match the exponent a candidate will be priced at. Spend per
    person is ``revenue / captured_population``, and captured_population is a
    function of beta — so a constant measured at one exponent cannot price a
    headcount computed at another. ``observations_by_beta`` is the usual way in.

    ``geometries`` is an optional store-index -> CatchmentGeometry cache, so a
    sweep over several betas pays for the distance matrix once.
    """
    revenue_by_org = revenue_by_org or {}
    stock_value_by_org = stock_value_by_org or {}
    out: List[Dict[str, Any]] = []
    skipped: List[Dict[str, Any]] = []

    def _skip(org, name, reason, detail=""):
        skipped.append({"org_cd": org, "name": name, "reason": reason,
                        "detail": detail})

    for i, s in enumerate(stores or []):
        org = str(s.get("org_cd") or s.get("ORG_CD") or f"#{i}")
        name = str(s.get("name") or org)
        rev = float(revenue_by_org.get(org, 0) or 0)
        if rev <= 0:
            _skip(org, name, "no-sales",
                  "no recorded sales in the period, so there is no outcome to "
                  "calibrate against")
            continue
        lat, lon = s.get("lat"), s.get("lon")
        if lat is None or lon is None:
            _skip(org, name, "not-placed",
                  "no location on file, so it cannot be scored")
            continue
        size = float(s.get("size_sqft") or DEFAULT_SIZE_SQFT)

        others = [o for j, o in enumerate(stores) if j != i]
        geo = None if geometries is None else geometries.get(i)
        if geo is None:
            geo = build_geometry(float(lat), float(lon), others, competitors,
                                 catchment_km, population)
            if geometries is not None:
                geometries[i] = geo
        sc = score_site(float(lat), float(lon), others, competitors,
                        size_sqft=size,
                        field=build_field(float(lat), float(lon), others,
                                          competitors, beta=float(beta),
                                          geometry=geo))
        capture = sc["capture_pct"] / 100.0
        if capture < MIN_USABLE_CAPTURE:
            # revenue / capture would explode. Worth naming loudly: it usually
            # means a mis-typed coordinate or a store sitting inside a rival's
            # shadow, and both are things the operator can act on.
            # ASCII only: these strings reach format_report, which prints to a
            # customer's Windows console.
            _skip(org, name, "capture-too-low",
                  f"takes only {capture:.2%} of its catchment, under the "
                  f"{MIN_USABLE_CAPTURE:.1%} floor, so the demand it implies "
                  "would be unusable. Check its coordinates.")
            continue

        # With a population grid the store's trade area is a headcount, so the
        # constant we calibrate becomes SPEND PER PERSON — an economic quantity
        # that transfers between locations. Without one we can only calibrate
        # revenue per unit of share, which does not: it silently carries the
        # density of whichever catchment it was measured in.
        people = sc.get("captured_population")
        # Catchment covariate for the spend model. None, never zero, when it
        # cannot be read — nothing downstream may treat "unknown" as "poor".
        catch = None
        if affluence is not None and population is not None:
            catch = affluence.index_at(float(lat), float(lon),
                                       population).get("index")
        out.append({
            "org_cd": org,
            "name": s.get("name") or org,
            "size_sqft": size,
            "capture": capture,
            "revenue": rev,
            "stock_value": float(stock_value_by_org.get(org, 0) or 0),
            "captured_population": people,
            "catchment_population": sc.get("catchment_population"),
            "affluence_index": catch,
            "spend_per_person": (rev / people
                                 if people and people > 0 else None),
            # The measurement this module exists for: how big is the prize
            # around a store that we KNOW trades this well?
            "implied_demand": rev / capture,
            "revenue_per_sqft": rev / size if size > 0 else 0.0,
        })
    return Observations(out, skipped)


def calibrate_by_beta(stores: Sequence[Dict[str, Any]],
                      competitors: Sequence[Dict[str, Any]] = (),
                      revenue_by_org: Optional[Dict[str, float]] = None,
                      stock_value_by_org: Optional[Dict[str, float]] = None,
                      betas: Sequence[float] = BETA_RANGE,
                      **kwargs) -> Dict[float, Dict[str, Any]]:
    """Calibrate and validate the estate at EVERY exponent in the band.

    WHY THIS EXISTS. A candidate's band edges are captured population at beta
    1.5 and 3.0. Pricing them needed a spend per person, and there was only
    one — measured at beta 2.0. But spend per person IS ``revenue /
    captured_population``, and captured_population is a function of beta, so
    multiplying people(beta=3) by spend(beta=2) counts the exponent twice.

    Measured, the error was smaller than it sounds: on a validated seven-store
    estate both band edges came out 1.9% to 7.5% low. That is because the same
    cancellation that makes the catchment radius nearly a unit works here too —
    the estate's headcount and the candidate's move together with beta, so most
    of the ratio survives. The WIDTH moved both ways depending on the site
    (1.98x to 1.90x on one candidate, 1.17x to 1.22x on another), so this was
    never a uniform narrowing.

    Small and mostly conservative is not a reason to publish a number built two
    ways, and the fix costs one distance matrix per store: the whole four-
    exponent sweep measured 0.42s on a six-store estate.

    Returns ``{beta: {"observations", "calibration", "validation"}}``.
    """
    geometries: Dict[int, Any] = {}
    out: Dict[float, Dict[str, Any]] = {}
    for b in betas:
        obs = estate_observations(stores, competitors, revenue_by_org,
                                  stock_value_by_org, beta=float(b),
                                  geometries=geometries, **kwargs)
        out[float(b)] = {"observations": obs,
                         "calibration": calibrate(obs),
                         "validation": loo_validate(obs)}
    return out


def basis_holds_across(by_beta: Dict[float, Dict[str, Any]],
                       central: float = DISTANCE_DECAY) -> Dict[str, Any]:
    """Does the gate reach the same verdict at every exponent in the band?

    The basis is a claim about the ESTATE, so it is decided once, at the
    central exponent. But if it would flip at 1.5 or 3.0 then the verdict is
    partly a statement about a parameter nobody has fitted, and the operator is
    entitled to know that before spending against it — the same reason the
    capture figures are published as a band rather than a point.
    """
    verdicts = {b: (v.get("validation") or {}).get("basis")
                for b, v in by_beta.items()}
    ref = verdicts.get(float(central))
    stable = all(x == ref for x in verdicts.values())
    # Two different sizes of instability, and conflating them overstates the
    # milder one. A basis that moves between "population" and "affluence" still
    # earns a budget at every exponent — the geography won each time, by a
    # different route. A basis that becomes None somewhere does not.
    earns_everywhere = all(x is not None for x in verdicts.values())
    if stable:
        note = None
    elif earns_everywhere:
        note = ("the geography earns a budget at every distance-decay exponent "
                "in the range, but which measure wins changes across it, so the "
                "figure is better read as its band than as its midpoint")
    else:
        note = ("the gate reaches a different verdict at the ends of the "
                "distance-decay range, so whether the location earns a budget "
                "at all rests partly on an exponent this estate cannot fit")
    return {
        "basis": ref,
        "basis_stable_across_beta": stable,
        "earns_budget_at_every_beta": earns_everywhere,
        "basis_by_beta": {str(b): verdicts[b] for b in sorted(verdicts)},
        "note": note,
    }


def calibrate(observations: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Reduce the estate observations to the constants a candidate needs."""
    obs = list(observations or [])
    n = len(obs)
    # The accounting travels with the observations. Read it here so it reaches
    # every surface that shows "stores used", because a count without the
    # stores it left out is the thing that misleads.
    skipped = list(getattr(observations, "skipped", ()) or ())
    excluded = {"stores_skipped": skipped,
                "skipped_count": len(skipped),
                "considered": n + len(skipped)}
    if n == 0:
        return dict(excluded, n=0, usable=False,
                    reason=("no store has both a location and recorded sales"
                            if not skipped else
                            f"none of the {len(skipped)} store(s) could be "
                            "used — see stores_skipped for why"))

    demands = [o["implied_demand"] for o in obs]
    rps = [o["revenue_per_sqft"] for o in obs]
    covers = [o["stock_value"] / o["revenue"]
              for o in obs if o["revenue"] > 0 and o["stock_value"] > 0]
    spends = [o["spend_per_person"] for o in obs
              if o.get("spend_per_person") and o["spend_per_person"] > 0]

    med_demand = _median(demands)
    spread = (_quantile(demands, 0.75) / _quantile(demands, 0.25)
              if _quantile(demands, 0.25) > 0 else 0.0)
    spend_spread = (_quantile(spends, 0.75) / _quantile(spends, 0.25)
                    if spends and _quantile(spends, 0.25) > 0 else 0.0)

    return dict(
        excluded,
        n=n,
        usable=True,
        **{
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
        # Population basis. Annual spend captured per person in catchment —
        # the one constant here that transfers between locations, because it
        # no longer carries the density of where it was measured.
        "has_population": bool(spends),
        "population_stores": len(spends),
        "median_spend_per_person": _median(spends) if spends else 0.0,
        "spend_p25": _quantile(spends, 0.25) if spends else 0.0,
        "spend_p75": _quantile(spends, 0.75) if spends else 0.0,
        "spend_spread_ratio": round(spend_spread, 2),
        # Spend as a FUNCTION of the catchment. Fitted here, but it only sets
        # a budget if loo_validate says it beat the dumb predictors.
        "spend_model": _fit_spend(obs),
        })


def _fit_spend(obs: Sequence[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    """Log-linear spend-vs-catchment fit, or None when unsupportable."""
    try:
        from .affluence import fit_spend_model
    except ImportError:
        return None
    return fit_spend_model([(o.get("affluence_index"), o.get("spend_per_person"))
                            for o in obs])


# ── step 2: the gate ────────────────────────────────────────────────────────
def loo_validate(observations: Sequence[Dict[str, Any]]) -> Dict[str, Any]:
    """Leave-one-out: does the geography beat two dumber predictors?

    Predictors, each refitted on the n-1 remaining stores so no fold sees its
    own answer:

      population  revenue = captured_population_i * median(spend_per_person)
      capture     revenue = capture_i * median(implied_demand of the others)
      sqft        revenue = size_i    * median(revenue_per_sqft of the others)
      mean        revenue = median(revenue of the others)

    A geographic model earns the right to set a budget only by beating BOTH
    dumb predictors. A tie is a loss: the simpler one wins by default, because
    it needs no map, no competitor file and no assumption about catchments.

    Population is tested SEPARATELY rather than assumed better. Buying a
    population grid does not automatically make a forecast good — if spend per
    person varies more across this estate than floor area does, the headcount
    is adding noise, and the operator should be told that rather than sold it.
    """
    obs = list(observations or [])
    n = len(obs)
    if n < MIN_CALIBRATION_STORES:
        return {"n": n, "validated": False, "basis": None,
                "reason": (f"{n} usable store(s); leave-one-out needs at least "
                           f"{MIN_CALIBRATION_STORES} to mean anything")}

    cap_pairs, sqft_pairs, mean_pairs, pop_pairs, aff_pairs = [], [], [], [], []
    for i, o in enumerate(obs):
        rest = [x for j, x in enumerate(obs) if j != i]
        d_hat = _median([r["implied_demand"] for r in rest])
        rps_hat = _median([r["revenue_per_sqft"] for r in rest])
        mean_hat = _median([r["revenue"] for r in rest])
        cap_pairs.append((o["revenue"], o["capture"] * d_hat))
        sqft_pairs.append((o["revenue"], o["size_sqft"] * rps_hat))
        mean_pairs.append((o["revenue"], mean_hat))

        spends = [r["spend_per_person"] for r in rest
                  if r.get("spend_per_person") and r["spend_per_person"] > 0]
        people = o.get("captured_population")
        if spends and people and people > 0:
            pop_pairs.append((o["revenue"], people * _median(spends)))

            # Fifth predictor: spend per person as a FUNCTION of the
            # catchment rather than one number for the chain. Refit on the
            # n-1 remaining stores so the held-out fold never sees itself.
            from .affluence import fit_spend_model, predict_spend
            model = fit_spend_model(
                [(r.get("affluence_index"), r.get("spend_per_person"))
                 for r in rest])
            if model is not None and o.get("affluence_index") is not None:
                aff_pairs.append((o["revenue"], people * predict_spend(
                    model, o["affluence_index"], _median(spends))))

    e_cap = _mape(cap_pairs)
    e_sqft = _mape(sqft_pairs)
    e_mean = _mape(mean_pairs)
    # Only meaningful if every fold could be predicted this way.
    e_pop = _mape(pop_pairs) if len(pop_pairs) == n else None
    e_aff = _mape(aff_pairs) if len(aff_pairs) == n else None

    baseline = min(e_sqft, e_mean)
    # Per-fold errors, kept so the win can be tested rather than merely
    # observed. The baseline is whichever dumb predictor did better overall.
    base_errs = (_errors(sqft_pairs) if e_sqft <= e_mean
                 else _errors(mean_pairs))

    def _beats(err: Optional[float], errs: Sequence[float]) -> bool:
        """Better than the baseline on BOTH counts, or it does not pass.

        A margin on the median is not enough on its own. Measured: a population
        model once cleared the margin by 29.8% while beating the baseline on
        only 2 of 5 held-out stores — the median was being carried by one or two
        outliers, and the model was WORSE on the majority of the estate. A
        predictor that loses on most of your stores must not set a budget.
        """
        if err is None or baseline <= 0:
            return False
        if (baseline - err) / baseline < MIN_GATE_MARGIN:
            return False
        s = _sign_test(errs, base_errs)
        return s["folds"] > 0 and s["wins"] * 2 > s["folds"]

    aff_wins = (_beats(e_aff, _errors(aff_pairs))
                and e_aff <= (e_pop if e_pop is not None else e_aff)
                and e_aff <= e_cap)
    pop_wins = (not aff_wins and _beats(e_pop, _errors(pop_pairs))
                and e_pop <= e_cap)
    cap_wins = (not aff_wins and not pop_wins
                and _beats(e_cap, _errors(cap_pairs)))

    if aff_wins:
        basis, validated = "affluence", True
        win_errs = _errors(aff_pairs)
        best = e_aff
    elif pop_wins:
        basis, validated = "population", True
        win_errs = _errors(pop_pairs)
        best = e_pop
    elif cap_wins:
        basis, validated = "capture", True
        win_errs = _errors(cap_pairs)
        best = e_cap
    else:
        basis, validated = None, False
        # Report the CLOSEST geographic predictor even in defeat, so the
        # operator can see whether it lost narrowly or was never in the race.
        best = min(x for x in (e_cap, e_pop, e_aff) if x is not None)
        win_errs = (_errors(aff_pairs) if best == e_aff and e_aff is not None
                    else _errors(pop_pairs) if best == e_pop and e_pop is not None
                    else _errors(cap_pairs))

    margin = (baseline - best) / baseline if baseline > 0 else 0.0
    sign = (_sign_test(win_errs, base_errs)
            if len(win_errs) == len(base_errs) and win_errs
            else {"folds": 0, "wins": 0, "p": 1.0})
    # A win the sign test cannot separate from a coin flip is still reported as
    # a win — the margin cleared — but it is labelled, and the caller must show
    # the label. Five stores rarely establish anything, and pretending
    # otherwise is what the old bare inequality did.
    provisional = validated and sign["p"] > PROVISIONAL_P

    out = {
        "n": n,
        "validated": validated,
        "provisional": provisional,
        "basis": basis,
        "margin": round(margin, 4),
        "margin_required": MIN_GATE_MARGIN,
        "folds_won": sign["wins"],
        "folds_compared": sign["folds"],
        "sign_p": sign["p"],
        "mape_capture": e_cap,
        "mape_population": e_pop,
        "mape_affluence": e_aff,
        "mape_sqft_only": e_sqft,
        "mape_estate_median": e_mean,
        "beaten_by": (None if validated
                      else ("floor area alone" if e_sqft <= e_mean
                            else "the estate median")),
        "confidence_note": (
            None if not validated else
            (f"won on {sign['wins']} of {sign['folds']} stores; a coin flip "
             f"would do that {sign['p']:.0%} of the time, so treat this as "
             "provisional until the estate is larger")
            if provisional else
            f"won on {sign['wins']} of {sign['folds']} stores (p={sign['p']:.2f})"),
        "reason": (None if validated else
                   (f"the location beat the simpler predictor by {margin:.0%} "
                    f"on the median but lost on {sign['folds'] - sign['wins']} "
                    f"of {sign['folds']} of your stores, so the win rests on "
                    "one or two outliers"
                    if margin >= MIN_GATE_MARGIN else
                    f"the location beat the simpler predictor by only "
                    f"{margin:.0%}, under the {MIN_GATE_MARGIN:.0%} margin "
                    "required - too close to call on this estate"
                    if margin > 0 else
                    "the location adds nothing over the simpler predictor on "
                    "this estate") +
                   " - capital is proposed from productivity instead, and the "
                   "site score stays a ranking tool"),
    }
    if e_pop is not None and not pop_wins:
        # Say it plainly: they may have paid for this data.
        out["population_note"] = (
            f"population is loaded but did not improve the forecast "
            f"({e_pop:.0%} median error against {min(e_cap, baseline):.0%} for "
            f"the best alternative) - spend per person varies too much across "
            f"this estate to carry a budget")
    elif pop_wins:
        out["population_note"] = (
            f"population improved the forecast to {e_pop:.0%} median error, "
            f"from {min(e_cap, baseline):.0%} without it")
    return out


# ── step 3: the proposal ────────────────────────────────────────────────────
def propose_capital(capture_pct: float, size_sqft: float,
                    calibration: Dict[str, Any],
                    validation: Optional[Dict[str, Any]] = None,
                    isolated: bool = False,
                    captured_population: Optional[float] = None,
                    affluence_index: Optional[float] = None) -> Dict[str, Any]:
    """Opening stock capital for a candidate, with the basis it rests on.

    Returns a ``basis`` the caller must show, because the four bases carry very
    different weight:

      ``population-calibrated`` strongest: people captured x this chain's own
                              measured spend per person. Transfers between
                              locations because it carries no density.
      ``estate-calibrated``   geography beat the baselines but without a
                              headcount; capture x measured catchment demand.
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

    if val.get("basis") == "affluence" and captured_population:
        # The strongest basis: a headcount times a spend per person that
        # varies with the catchment, both measured on this chain's own books.
        from .affluence import predict_spend
        people = max(0.0, float(captured_population))
        model = cal.get("spend_model")
        fallback = float(cal.get("median_spend_per_person") or 0.0)
        s_mid = predict_spend(model, affluence_index, fallback)
        # The band is the fit's own residual spread, not an invented margin.
        r2 = float((model or {}).get("r2") or 0.0)
        width = 1.0 + max(0.15, 1.0 - r2)
        rev = people * s_mid
        rev_lo, rev_hi = rev / width, rev * width
        basis = "affluence-calibrated"
        note = (f"~{people:,.0f} people x {s_mid:,.0f} spend per person for a "
                f"catchment like this one (fitted on {cal.get('n', 0)} of your "
                f"stores, R2 {r2:.2f}). Spend is modelled from the catchment, "
                f"not averaged across the chain.")
    elif val.get("basis") == "population" and captured_population:
        # The strongest basis available: an absolute headcount times a spend
        # per person measured on this chain's own trading. Nothing here carries
        # the density of the catchment it was calibrated in.
        people = max(0.0, float(captured_population))
        s_med = float(cal.get("median_spend_per_person") or 0.0)
        s_lo = float(cal.get("spend_p25") or s_med)
        s_hi = float(cal.get("spend_p75") or s_med)
        rev = people * s_med
        rev_lo, rev_hi = people * s_lo, people * s_hi
        basis = "population-calibrated"
        note = (f"~{people:,.0f} people captured x "
                f"{s_med:,.0f} spend per person, measured on "
                f"{cal.get('population_stores', 0)} of your own stores "
                f"(spend varies {cal.get('spend_spread_ratio', 0):.1f}x "
                f"across them, which is this band).")
    elif val.get("validated"):
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
        "captured_population": (None if captured_population is None
                                else round(float(captured_population), 0)),
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

    ``score_fn(size_sqft)`` re-scores the SAME location at each rung. It may
    return a capture percentage, or the whole ``score_site`` result — the
    latter lets the sweep price each rung on captured PEOPLE where a population
    grid is loaded, which is the more honest curve because a bigger store pulls
    people from further out rather than simply taking more of a fixed share.

    Either way the saturation of the Huff model does the work: a bigger store
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
    spend = float(cal.get("median_spend_per_person") or 0.0)
    use_people = bool((validation or {}).get("basis") == "population" and spend)
    calibrated = bool((validation or {}).get("validated"))

    rungs = []
    best = None
    for size in ladder:
        scored = score_fn(size)
        if isinstance(scored, dict):
            cap = max(0.0, float(scored.get("adjusted_capture_pct") or 0)) / 100.0
            people = scored.get("captured_population")
        else:
            cap, people = max(0.0, float(scored)) / 100.0, None
        if use_people and people:
            rev = float(people) * spend
        else:
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
        # Reachable at last. score_sites used to skip this call entirely
        # whenever the gate failed - the common case - so the one branch
        # written for an unvalidated estate could never run, and a retailer
        # whose geography had not earned a budget saw no size guidance at all
        # rather than guidance with a caveat on it.
        #
        # The caveat now names WHICH way the gate failed, because the reasons
        # are not equivalent: "beat the median but lost on most of your stores"
        # is a different warning from "did not beat floor area at all", and the
        # operator can act on the difference.
        why = ((validation or {}).get("reason") or
               "the geography did not beat floor area on your estate")
        note = ("Indicative only - " + why.split(" - ")[0].strip() +
                ". Treat this as a ranking, not a size decision.")
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
    considered = cal.get("considered", cal.get("n", 0))
    w.append(f"  stores used            {cal.get('n', 0):>12,}"
             + (f"   of {considered:,} on file" if considered != cal.get("n", 0)
                else ""))
    # Never print a count without what it left out. A store that silently
    # dropped below the capture floor is usually a mis-typed coordinate, and
    # the operator is the only one who can tell.
    for s in (cal.get("stores_skipped") or []):
        w.append(f"     EXCLUDED {s.get('org_cd', ''):<10} {s.get('detail', '')}")
    if not cal.get("usable"):
        w.append(f"  UNUSABLE: {cal.get('reason', '')}")
        return "\n".join(w)
    w.append(f"  median implied demand  {cal.get('median_demand', 0):>12,.0f}")
    w.append(f"  catchment spread       {cal.get('demand_spread_ratio', 0):>12.2f}x")
    w.append(f"  median revenue/sq ft   {cal.get('median_revenue_per_sqft', 0):>12,.2f}")
    w.append(f"  stock-to-sales ratio   {cal.get('cover_ratio', 0):>12.4f}"
             + ("" if cal.get("cover_measured") else "   (not measured)"))
    if cal.get("has_population"):
        w.append(f"  spend per person       {cal.get('median_spend_per_person', 0):>12,.2f}"
                 f"   over {cal.get('population_stores', 0)} stores")
        w.append(f"  spend spread           {cal.get('spend_spread_ratio', 0):>12.2f}x")
    else:
        w.append("  spend per person             (no population grid loaded)")
    w.append("")
    w.append("  leave-one-out validation")
    if not val.get("validated"):
        w.append(f"     NOT VALIDATED - {val.get('reason', '')}")
    if "mape_capture" in val:
        if val.get("mape_population") is not None:
            w.append(f"     population model   {val['mape_population']:>8.1%}  median abs error")
        w.append(f"     capture model      {val['mape_capture']:>8.1%}"
                 + ("" if val.get("mape_population") is not None
                    else "  median abs error"))
        w.append(f"     floor area alone   {val['mape_sqft_only']:>8.1%}")
        w.append(f"     estate median      {val['mape_estate_median']:>8.1%}")
        w.append(f"     margin over baseline  {val.get('margin', 0):>8.1%}"
                 f"   (needs {val.get('margin_required', 0):.0%})")
        w.append(f"     -> geography earns a budget: "
                 + ("YES (" + str(val.get("basis")) + ")"
                    + (" - PROVISIONAL" if val.get("provisional") else "")
                    if val.get("validated") else "NO"))
        if val.get("confidence_note"):
            w.append(f"     {val['confidence_note']}")
    if val.get("population_note"):
        w.append(f"     {val['population_note']}")
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
