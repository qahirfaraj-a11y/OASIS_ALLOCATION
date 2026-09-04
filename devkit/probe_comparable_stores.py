"""Probe: do the siting constants have any support in realised store performance?

THE CLAIMS UNDER TEST
    claim.siting.size-exponent-unfitted        SIZE_EXPONENT     = 1.0
    claim.siting.distance-decay-unfitted       DISTANCE_DECAY    = 2.0
    claim.siting.catchment-km-unfitted         CATCHMENT_KM      = 10.0
    claim.siting.cannibalisation-km-unfitted   CANNIBALISATION_KM= 3.0
    claim.siting.estate-has-a-usable-revenue-label

THE METHOD
    Score every store in the EXISTING estate at its own location, with itself
    removed from the competitive field — leave-one-out, because a store that
    competes with itself is not a comparable. Correlate predicted capture
    against realised performance, and sweep alpha x beta x catchment to see
    which values the data prefers.

    This is the only held-out test siting can have: no store has ever been
    opened on this model's recommendation, so there is no outcome to attribute.
    Stores that already exist are the substitute.

THE PART THAT COMES FIRST
    A correlation is only worth as much as its label. So before any fit, this
    probe interrogates the label itself:

      * Is it a deterministic function of the model's own inputs? Then the fit
        is circular and proves nothing (trap T5).
      * Do the available labels agree with each other? If two candidate
        measures of "how well is this store doing" rank the estate differently,
        at most one of them can be the truth, and the fit must not proceed on a
        coin toss.

    Reporting "no defensible label" is a real result. Manufacturing a fit
    against a synthetic ladder is how the GNN ended up trained on labels
    generated from its own inputs.

EMITS one JSON verdict object per line, per the probe harness contract.
"""
from __future__ import annotations

import argparse
import csv
import json
import math
import re
import sqlite3
import sys
from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Optional, Sequence, Tuple

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

from devkit.methodology.traps import (join_match_rate, like_for_like,  # noqa: E402
                                      non_circular, ratio, run_all)
from oasis.logic import site_scoring as ss                             # noqa: E402

ESTATE = ROOT / "stores_network.json"
COMPETITORS = ROOT / "oasis" / "data" / "competitor_network.csv"
POS_DB = ROOT / "oasis" / "data" / "variant_network.db"

OWN_CHAIN = "chandarana"
CATCHMENT_RANGE = (5.0, 7.5, 10.0, 12.5, 15.0)


# ----------------------------------------------------------------- statistics
def _ranks(xs: Sequence[float]) -> List[float]:
    order = sorted(range(len(xs)), key=lambda i: xs[i])
    r = [0.0] * len(xs)
    i = 0
    while i < len(order):
        j = i
        while j + 1 < len(order) and xs[order[j + 1]] == xs[order[i]]:
            j += 1
        avg = (i + j) / 2.0 + 1.0
        for k in range(i, j + 1):
            r[order[k]] = avg
        i = j + 1
    return r


def pearson(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    n = len(xs)
    if n < 3:
        return None
    mx, my = sum(xs) / n, sum(ys) / n
    sxy = sum((a - mx) * (b - my) for a, b in zip(xs, ys))
    sxx = sum((a - mx) ** 2 for a in xs)
    syy = sum((b - my) ** 2 for b in ys)
    if sxx <= 0 or syy <= 0:
        return None
    return sxy / math.sqrt(sxx * syy)


def spearman(xs: Sequence[float], ys: Sequence[float]) -> Optional[float]:
    if len(xs) < 3:
        return None
    return pearson(_ranks(xs), _ranks(ys))


def _norm(name: str) -> str:
    s = re.sub(r"[^a-z0-9 ]", " ", str(name).lower())
    s = s.replace(OWN_CHAIN, " ").replace("foodplus", " ").replace("branch", " ")
    return re.sub(r"\s+", " ", s).strip()


# --------------------------------------------------------------------- inputs
def load_estate() -> List[Dict[str, Any]]:
    raw = json.loads(ESTATE.read_text(encoding="utf-8"))
    out = []
    for s in raw.get("stores", []):
        if s.get("latitude") is None or s.get("longitude") is None:
            continue
        out.append({
            "id": s.get("store_id"), "name": s.get("name", ""),
            "key": _norm(s.get("name", "")),
            "lat": float(s["latitude"]), "lon": float(s["longitude"]),
            "size_sqft": float(s.get("floor_area_sqft") or ss.DEFAULT_SIZE_SQFT),
            "chain": OWN_CHAIN,
            "declared_revenue": float(s.get("avg_monthly_revenue") or 0.0),
            "footfall_rank": float(s.get("footfall_rank") or 0.0),
            "sales_rank": float(s.get("sales_rank") or 0.0),
            "affluence": float(s.get("catchment_affluence_index") or 0.0),
        })
    return out


def load_competitors() -> List[Dict[str, Any]]:
    if not COMPETITORS.exists():
        return []
    rows = list(csv.DictReader(COMPETITORS.open(encoding="utf-8")))
    return [{"lat": float(r["Latitude"]), "lon": float(r["Longitude"]),
             "chain": r.get("Chain", ""), "name": r.get("Store_Name", "")}
            for r in rows
            if r.get("Latitude") and r.get("Longitude")
            and OWN_CHAIN not in str(r.get("Chain", "")).lower()]


def load_pos_label() -> Dict[str, Dict[str, float]]:
    """Realised till takings per store, from POS. Observed, not declared."""
    if not POS_DB.exists():
        return {}
    c = sqlite3.connect(str(POS_DB))
    c.row_factory = sqlite3.Row
    try:
        rows = list(c.execute("""
            select h.ORG_CD, o.ORG_NAME,
                   count(*) bills, sum(h.NET_AMT) net,
                   min(h.BILL_DT) d0, max(h.BILL_DT) d1
            from POS_SALES_HDR h
            left join ORGANIZATION_MST o on o.ORG_CD = h.ORG_CD
            where coalesce(h.VOID_FLAG, 'N') <> 'Y'
            group by h.ORG_CD"""))
    except sqlite3.Error:
        return {}
    finally:
        c.close()
    return {_norm(r["ORG_NAME"] or r["ORG_CD"]):
            {"net": float(r["net"] or 0.0), "bills": float(r["bills"]),
             "d0": r["d0"], "d1": r["d1"]} for r in rows}


# ------------------------------------------------------- label interrogation
def _grid_step(values: Sequence[float]) -> Optional[float]:
    """The common divisor of a set of values, if they sit on a fixed ladder."""
    vs = sorted({round(v) for v in values if v})
    if len(vs) < 4:
        return None
    g = 0
    for v in vs:
        g = math.gcd(g, int(v))
    return float(g) if g > 1 and g >= max(vs) / (len(vs) * 8) else None


def interrogate_label(estate: List[Dict[str, Any]],
                      pos: Dict[str, Dict[str, float]]) -> Dict[str, Any]:
    """Before any fit: is there a label worth fitting against?"""
    declared = [s["declared_revenue"] for s in estate]
    footfall = [s["footfall_rank"] for s in estate]
    size = [s["size_sqft"] for s in estate]

    rho_ff = spearman(declared, footfall)
    rho_sz = spearman(declared, size)
    step = _grid_step(declared)

    matched = [s for s in estate if s["key"] in pos]
    rho_agree = (spearman([s["declared_revenue"] for s in matched],
                          [pos[s["key"]]["net"] for s in matched])
                 if len(matched) >= 3 else None)

    checks = [
        join_match_rate([s["key"] for s in estate], list(pos),
                        "estate ↔ POS organisations", floor=0.50),
        non_circular("declared avg_monthly_revenue",
                     inputs=["footfall_rank", "floor_area_sqft"],
                     downstream_of_behaviour=["footfall_rank", "floor_area_sqft"]),
    ]
    if matched:
        checks.append(like_for_like(
            ("declared (monthly)", 1.0),
            ("POS net (period)", 1.0), "revenue label basis"))
    run_all(checks)

    synthetic = bool(step) or (rho_ff is not None and abs(rho_ff) > 0.97)
    disagree = rho_agree is not None and rho_agree < 0.5

    return {
        "declared_is_ladder": bool(step), "ladder_step": step,
        "declared_vs_footfall_rho": rho_ff,
        "declared_vs_size_rho": rho_sz,
        "declared_vs_pos_rho": rho_agree,
        "matched_stores": len(matched),
        "pos_window": (pos[matched[0]["key"]]["d0"], pos[matched[0]["key"]]["d1"])
                      if matched else None,
        "synthetic": synthetic, "labels_disagree": disagree,
        "usable": bool(matched) and not disagree,
    }


# -------------------------------------------------------------------- the fit
def sweep(estate: List[Dict[str, Any]], competitors: List[Dict[str, Any]],
          label: Dict[str, float],
          alphas=ss.ALPHA_RANGE, betas=ss.BETA_RANGE,
          catchments=CATCHMENT_RANGE) -> List[Dict[str, Any]]:
    """Leave-one-out capture vs the label, at every combination."""
    subjects = [s for s in estate if s["key"] in label]
    out = []
    for a in alphas:
        for b in betas:
            for ck in catchments:
                preds, actuals = [], []
                for s in subjects:
                    others = [o for o in estate if o["id"] != s["id"]]
                    try:
                        r = ss.score_site(
                            s["lat"], s["lon"], own_stores=others,
                            competitors=competitors, size_sqft=s["size_sqft"],
                            catchment_km=ck, beta=b, alpha=a)
                    except Exception:
                        continue
                    preds.append(float(r.get("capture_pct") or 0.0))
                    actuals.append(float(label[s["key"]]))
                if len(preds) < 3:
                    continue
                out.append({"alpha": a, "beta": b, "catchment_km": ck,
                            "n": len(preds),
                            "spearman": spearman(preds, actuals),
                            "pearson": pearson(preds, actuals),
                            "mean_capture_pct": sum(preds) / len(preds)})
    return out


def beta_identified(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Is beta identified, or does the fit just run monotone to an edge?

    `score_band`'s own docstring says the fit runs monotone to zero on this
    estate. If that is still true, the sweep is not choosing an exponent — it is
    walking to the boundary of the range it was given, which is not a fit.
    """
    by_beta: Dict[float, List[float]] = {}
    for r in results:
        if r["spearman"] is not None:
            by_beta.setdefault(r["beta"], []).append(r["spearman"])
    means = {b: sum(v) / len(v) for b, v in by_beta.items() if v}
    if len(means) < 3:
        return {"identified": False, "reason": "too few betas scored"}
    ordered = [means[b] for b in sorted(means)]
    monotone = (all(x <= y for x, y in zip(ordered, ordered[1:]))
                or all(x >= y for x, y in zip(ordered, ordered[1:])))
    best = max(means, key=lambda b: means[b])
    interior = min(means) < best < max(means)
    return {"identified": bool(interior and not monotone),
            "best_beta": best, "by_beta": means, "monotone": monotone,
            "reason": ("optimum is at the edge of the swept range — the data is "
                       "not choosing an exponent" if not interior else
                       "interior optimum")}


# ----------------------------------------------------------------------- main
def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--label", choices=("pos", "declared"), default="pos")
    ap.add_argument("--json-only", action="store_true")
    a = ap.parse_args(argv)

    estate = load_estate()
    competitors = load_competitors()
    pos = load_pos_label()
    print(f"  estate {len(estate)} stores · competitors {len(competitors)} "
          f"· POS orgs {len(pos)}")

    lab = interrogate_label(estate, pos)
    print(f"  label: declared_is_ladder={lab['declared_is_ladder']} "
          f"(step {lab['ladder_step']}) · declared~footfall rho="
          f"{lab['declared_vs_footfall_rho']} · declared~POS rho="
          f"{lab['declared_vs_pos_rho']} on {lab['matched_stores']} stores")

    label = ({k: v["net"] for k, v in pos.items()} if a.label == "pos"
             else {s["key"]: s["declared_revenue"] for s in estate})
    results = sweep(estate, competitors, label)
    results.sort(key=lambda r: -(r["spearman"] or -9))
    ident = beta_identified(results)

    if results:
        print("  top combinations by rank correlation:")
        for r in results[:5]:
            print(f"    alpha={r['alpha']} beta={r['beta']} catch={r['catchment_km']:>4}"
                  f"  rho={r['spearman']: .3f}  n={r['n']}"
                  f"  mean capture {r['mean_capture_pct']:.2f}%")
        print(f"  beta identified: {ident['identified']} — {ident['reason']}")

    best = results[0] if results else {}
    metric = {"label_source": a.label, "label": lab, "best": best,
              "beta_identification": ident,
              "swept": len(results), "top": results[:8]}

    # -- the blocking claim, judged first -------------------------------------
    usable = lab["usable"] and not lab["synthetic"]
    print(json.dumps({
        "claim": "claim.siting.estate-has-a-usable-revenue-label",
        "verdict": "supports" if usable else "contradicts",
        "metric": metric, "held_out": False,
        "baseline": "stores_network.json avg_monthly_revenue",
        "beat_baseline": None, "traps": ["T1", "T4", "T5"],
        "notes": (
            "Declared avg_monthly_revenue sits on a fixed ladder "
            f"(step {lab['ladder_step']:,.0f}) and tracks footfall_rank at rho="
            f"{lab['declared_vs_footfall_rho']}, so it is a function of the "
            "model's own inputs. Against POS takings on "
            f"{lab['matched_stores']} matched stores it ranks the estate at "
            f"rho={lab['declared_vs_pos_rho']}. Two labels that disagree cannot "
            "both be the truth, and a fit against either is not held out."
            if not usable else
            f"POS takings available for {lab['matched_stores']} stores; "
            "declared and observed labels agree.")}))

    # -- the four constants ---------------------------------------------------
    verdict = "inconclusive" if not usable else (
        "contradicts" if (best.get("spearman") or 0) > 0.5 else "inconclusive")
    for claim, current, swept in (
            ("claim.siting.size-exponent-unfitted", ss.SIZE_EXPONENT, "alpha"),
            ("claim.siting.distance-decay-unfitted", ss.DISTANCE_DECAY, "beta"),
            ("claim.siting.catchment-km-unfitted", ss.CATCHMENT_KM, "catchment_km"),
    ):
        print(json.dumps({
            "claim": claim, "verdict": verdict,
            "metric": {"current": current, "preferred": best.get(swept),
                       "spearman_at_preferred": best.get("spearman"),
                       "n": best.get("n"), "label_source": a.label,
                       "label_usable": usable,
                       "beta_identification": ident},
            "held_out": bool(usable), "beat_baseline": None,
            "traps": ["T1", "T4", "T5"],
            "notes": (
                f"Swept {len(results)} combinations leave-one-out. The label is "
                "not defensible, so no value is fitted: the sweep is reported as "
                "a diagnostic only."
                if not usable else
                f"Leave-one-out over {best.get('n')} stores prefers "
                f"{swept}={best.get(swept)} against current {current}.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
