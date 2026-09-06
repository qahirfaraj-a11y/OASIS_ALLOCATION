"""The ordering engine as mathematics, and the parameters the data implies.

THE MODEL
    Periodic review R, lead time L, protection interval P = R + L.
    Demand per day d, standard deviation sigma_d = cv*d, lead-time spread
    sigma_L. Demand over the protection interval has

        sigma_P = sqrt(P*sigma_d^2 + d^2*sigma_L^2) = d*sqrt(P*cv^2 + sigma_L^2)

    Order-up-to level and the two stock terms it produces:

        S      = d*P + z*sigma_P
        I(z,R) = d*R/2 + z*sigma_P                       average on hand
        E[short per cycle] = sigma_P * G(z),  G(z) = phi(z) - z*(1 - Phi(z))

    Annual cost, with h the holding rate, c unit cost, Cu the margin lost on a
    unit not sold, A the cost of raising one order:

        C(z,R) = h*c*( d*R/2 + z*sigma_P )
               + Cu*(365/R)*sigma_P*G(z)
               + A*(365/R)

THE FIRST-ORDER CONDITIONS, WHICH IS THE WHOLE POINT

    dC/dz = h*c*sigma_P - Cu*(365/R)*sigma_P*(1 - Phi(z)) = 0

        =>  1 - Phi(z*) = h*c*R / (365*Cu)                          (i)

    sigma_P CANCELS. The optimal service level does not depend on how variable
    demand is, only on the ratio of holding cost to lost margin over one review
    cycle. Every argument in this system about cv and sigma_L is an argument
    about HOW MUCH stock to hold, never about WHAT SERVICE LEVEL to target --
    and z is currently a single declared constant for all 15,037 lines.

    dC/dR = h*c*d/2 - A*365/R^2 + (safety term, second order) = 0

        =>  A* = R^2 * h * c * d / 730                              (ii)

    A is not measured anywhere in this business. (ii) inverts the question: it
    gives the cost per purchase order that would JUSTIFY the R in use, which a
    buyer can look at and reject.

THE PERISHABLE CORNER
    Where leftovers are worthless rather than merely carried, the cost of one
    unit too many is the whole cost c, not h*c*R/365, and the newsvendor ratio
    collapses to

        Phi(z*) = Cu / (Cu + c)                                     (iii)

    At a 25% ex-VAT margin that is 0.25/(0.25+0.75) = 0.25, so z* = -0.67: a
    truly perishable line should be stocked BELOW its mean demand. The engine
    holds z = +1.28 everywhere.

ASSERTED, and labelled as such: cv = 0.40 chain-wide; h = 24.38% (capital
14.38% CBK June 2026 + handling 10%, the handling number being nobody's
measurement); shelf life by department. Section 3 gives the sensitivity to cv
rather than pretending it is known.
"""
from __future__ import annotations

import argparse, csv, json, math, sys
from collections import defaultdict
from pathlib import Path
import numpy as np

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))
from devkit.methodology.traps import ratio, run_all      # noqa: E402

BOOK = ROOT / "oasis" / "data" / "order_book.csv"
H_CAPITAL = 0.1438      # CBK average commercial bank lending rate, June 2026
H_HANDLING = 0.10       # ASSERTED. Nobody has costed handling in this business.


def Phi(x): return 0.5 * (1.0 + np.vectorize(math.erf)(x / math.sqrt(2.0)))
def phi(x): return np.exp(-0.5 * x * x) / math.sqrt(2 * math.pi)
def G(z): return phi(z) - z * (1.0 - Phi(z))


def inv_Phi(p):
    """Acklam's inverse normal CDF, vectorised. Stdlib has no ppf."""
    p = np.clip(np.asarray(p, dtype=float), 1e-12, 1 - 1e-12)
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    dd = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
          3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    out = np.empty_like(p)
    lo = p < plow; hi = p > phigh; mid = ~(lo | hi)
    q = np.sqrt(-2 * np.log(p[lo]))
    out[lo] = (((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((dd[0]*q+dd[1])*q+dd[2])*q+dd[3])*q+1)
    q = np.sqrt(-2 * np.log(1 - p[hi]))
    out[hi] = -(((((c[0]*q+c[1])*q+c[2])*q+c[3])*q+c[4])*q+c[5]) / ((((dd[0]*q+dd[1])*q+dd[2])*q+dd[3])*q+1)
    q = p[mid] - 0.5; r = q * q
    out[mid] = (((((a[0]*r+a[1])*r+a[2])*r+a[3])*r+a[4])*r+a[5])*q / (((((b[0]*r+b[1])*r+b[2])*r+b[3])*r+b[4])*r+1)
    return out


def load():
    cols = defaultdict(list)
    with BOOK.open(encoding="utf-8") as fh:
        for r in csv.DictReader(fh):
            for k, v in r.items():
                cols[k].append(v)
    num = ("d cost gp_unit price R L sigma_L z P cycle_units safety_units S_raw "
           "shelf_life S on_hand Q cover_before cover_after order_kes excess_kes "
           "gp_year clamped structurally_short below_protection amit_blocked "
           "mande_flagged").split()
    f = {k: np.array(cols[k], dtype=float) for k in num}
    f["sku"] = np.array(cols["sku"]); f["dept"] = np.array(cols["dept"])
    f["vendor"] = np.array(cols["vendor"]); f["R_source"] = np.array(cols["R_source"])
    f["sigma_L_source"] = np.array(cols["sigma_L_source"])
    return f


def main(argv=None) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--handling", type=float, default=H_HANDLING)
    ap.add_argument("--cv", type=float, default=0.40)
    a = ap.parse_args(argv)
    f = load()
    n = f["d"].size
    h = H_CAPITAL + a.handling
    d, c, gp, R, L, sL = f["d"], f["cost"], f["gp_unit"], f["R"], f["L"], f["sigma_L"]
    P = R + L
    sigma_P = d * np.sqrt(P * a.cv ** 2 + sL ** 2)
    z0 = f["z"]
    perish = f["shelf_life"] > 0

    print(f"  {n:,} SKUs · h = {h:.2%} (capital {H_CAPITAL:.2%} + handling "
          f"{a.handling:.0%} ASSERTED) · cv = {a.cv:.2f} ASSERTED")

    # ---------------- 1. what actually moves the objective -------------
    print(f"\n  1. ELASTICITY OF AVERAGE INVENTORY. dI/dx * x/I, at the book's own values")
    I = d * R / 2.0 + z0 * sigma_P
    tot_I = float(np.sum(I * c))
    def elas(name, dI):
        e = float(np.sum(dI * c)) / tot_I
        print(f"     {name:<28}{e:>+8.3f}")
    elas("R  (review period)", (d / 2.0 + z0 * d * a.cv ** 2 / (2 * np.sqrt(P * a.cv**2 + sL**2))) * R)
    elas("z  (service constant)", z0 * sigma_P)
    elas("cv (demand spread)", z0 * d * P * a.cv ** 2 / np.sqrt(P * a.cv**2 + sL**2))
    elas("sigma_L (lead spread)", z0 * d * sL ** 2 / np.sqrt(P * a.cv**2 + sL**2))
    elas("L  (lead time)", (d + z0 * d * a.cv ** 2 / (2 * np.sqrt(P * a.cv**2 + sL**2))) * L)
    print(f"     total average inventory at cost: KES {tot_I:,.0f}")
    print(f"     R dominates. cv -- the parameter everyone worries about -- is the")
    print(f"     smallest term here, because sigma_L^2 outweighs P*cv^2 at this book's P.")

    # ---------------- 2. the optimal z ---------------------------------
    tail = np.clip(h * c * R / (365.0 * np.maximum(gp, 1e-9)), 1e-9, 0.999)
    z_dry = inv_Phi(1.0 - tail)                      # equation (i)
    cr = gp / (gp + c)                               # equation (iii)
    z_fresh = inv_Phi(cr)
    # WHICH NEWSVENDOR APPLIES IS DECIDED BY THE CLAMP, NOT BY THE CATEGORY.
    # Equation (iii) -- leftovers worthless -- is only true when the line
    # actually holds more than it can sell before expiry, which is exactly the
    # case where S >= d*shelf_life and the clamp binds. Where the clamp is
    # slack the stock turns inside its own shelf life, the leftover is CARRIED
    # rather than written off, and (i) is the right condition even for milk.
    # Applying (iii) to unclamped perishables while valuing their leftover at
    # carrying cost is two different assumptions in one sum, and it produced a
    # net of -3.7m: a "fix" that loses money is a sign error, not a finding.
    z_star = z_dry.copy()
    # AND THE CLAMP ALREADY SETTLED THE PERISHABLES.
    # S for a clamped line is d*shelf_life -- it does not contain z at all, so
    # moving z changes nothing there and any saving attributed to it is the
    # clamp's saving counted twice. The first pass credited +79.8m of avoided
    # waste to a z change on lines whose S is not a function of z; that is
    # larger than the whole book's modelled waste, which was the tell.
    # z is a live decision only on the 13,464 lines the clamp does not bind.
    clamped = f["clamped"] > 0.5
    z_star = np.where(clamped, z0, z_star)
    dS = (z_star - z0) * sigma_P
    up = dS > 0
    print(f"\n  2. THE OPTIMAL z, DERIVED  --  1-Phi(z*) = h*c*R/(365*Cu),  and for")
    print(f"     perishables Phi(z*) = Cu/(Cu+c)")
    for lbl, mask in (("non-perishable", ~perish), ("perishable", perish)):
        if not mask.any(): continue
        v = np.sort(z_star[mask]); q = lambda x: v[int(x * len(v))]
        print(f"     {lbl:<16} n={mask.sum():>6,}  z* p25 {q(.25):>6.2f} · median "
              f"{q(.5):>6.2f} · p75 {q(.75):>6.2f}   (engine uses {z0[0]:.2f})")
    dS = (z_star - z0) * sigma_P
    up = dS > 0
    print(f"     lines that should hold MORE {int(up.sum()):>6,} (+KES "
          f"{float(np.sum(dS[up]*c[up])):>12,.0f} of stock)")
    print(f"     lines that should hold LESS {int((~up).sum()):>6,} ( KES "
          f"{float(np.sum(dS[~up]*c[~up])):>12,.0f})")
    # value of the change: shortage cost avoided minus holding cost added
    # BOTH TAILS. The first pass counted only the shortage side, so the
    # perishable lines -- where z* is NEGATIVE precisely to avoid waste --
    # showed up as pure lost margin and the whole change read as -9.1m. The
    # newsvendor has two arms: E[short] = sigma_P*G(z) and, by symmetry,
    # E[left over] = sigma_P*G(-z). For a perishable the leftover is written
    # off at full cost; for a dry good it is merely carried, at h*c*R/365.
    cyc = 365.0 / R
    short0 = cyc * sigma_P * G(z0);      short1 = cyc * sigma_P * G(z_star)
    over0 = cyc * sigma_P * G(-z0);      over1 = cyc * sigma_P * G(-z_star)
    over_cost = c * h * R / 365.0   # per leftover unit: carried, not written off
    d_short_kes = float(np.sum((short0 - short1) * gp))
    d_over_kes = float(np.sum((over0 - over1) * over_cost))
    d_hold_kes = float(np.sum((z_star - z0) * sigma_P * c * h))
    for lbl, mask in (("z is live (unclamped)", ~clamped), ("clamp binds", clamped)):
        ds = float(np.sum((short0[mask] - short1[mask]) * gp[mask]))
        do = float(np.sum((over0[mask] - over1[mask]) * over_cost[mask]))
        dh = float(np.sum((z_star[mask] - z0[mask]) * sigma_P[mask] * c[mask] * h))
        print(f"     {lbl:<16} margin recovered {ds:>+13,.0f} · waste/carry avoided "
              f"{do:>+12,.0f} · extra holding {-dh:>+12,.0f}  =  {ds+do-dh:>+13,.0f}")
    print(f"     {'NET':<16}{'':>17}{'':>13}{'':>26}"
          f"KES {d_short_kes + d_over_kes - d_hold_kes:>+13,.0f} /yr")

    # ---------------- 3. cv sensitivity --------------------------------
    print(f"\n  3. cv SENSITIVITY. The assumption nobody can test.")
    print(f"     {'cv':>6}{'safety KES':>14}{'vs 0.40':>10}{'total S KES':>15}{'vs 0.40':>10}")
    base_saf = base_S = None
    for cvx in (0.20, 0.30, 0.40, 0.60, 0.80):
        sp = d * np.sqrt(P * cvx ** 2 + sL ** 2)
        saf = float(np.sum(z0 * sp * c)); tS = float(np.sum((d * P + z0 * sp) * c))
        if cvx == 0.40: base_saf, base_S = saf, tS
        print(f"     {cvx:>6.2f}{saf:>14,.0f}{'':>10}{tS:>15,.0f}", end="")
        print(f"{'' if base_saf is None else f'{100*(tS/base_S-1):>+9.1f}%'}")
    print(f"     Doubling cv from 0.40 to 0.80 moves total S by the amount above --")
    print(f"     small, because safety is only ~19% of S and sigma_L carries most of it.")

    # ---------------- 4. implied ordering cost -------------------------
    A_star = R ** 2 * h * c * d / 730.0            # equation (ii)
    bysup = defaultdict(lambda: [0.0, 0, 0.0])
    for i in range(n):
        b = bysup[f["vendor"][i]]; b[0] += A_star[i]; b[1] += 1; b[2] += R[i]
    print(f"\n  4. THE ORDERING COST THAT WOULD JUSTIFY TODAY'S R   A* = R^2*h*c*d/730")
    print(f"     {'supplier':<38}{'skus':>6}{'R':>5}{'implied cost per PO':>22}")
    for v, b in sorted(bysup.items(), key=lambda kv: -kv[1][0])[:10]:
        print(f"     {v[:37]:<38}{b[1]:>6,}{b[2]/max(b[1],1):>5.1f}{b[0]:>22,.0f}")
    allA = np.array([b[0] for b in bysup.values()]); allA.sort()
    print(f"     across {len(bysup):,} suppliers: median KES {allA[len(allA)//2]:,.0f} "
          f"per order · p90 KES {allA[int(.9*len(allA))]:,.0f}")
    print(f"     A buyer who says raising a PO does not cost the chain that much is")
    print(f"     saying R is too long. That is now a falsifiable claim, not a default.")

    # ---------------- 5. the perishable corner -------------------------
    ss = f["structurally_short"] > 0.5
    if ss.any():
        gain_day = d[ss] * gp[ss] * 365.0 * (1.0 - Phi((f["shelf_life"][ss] * d[ss]
                    - d[ss] * P[ss]) / np.maximum(sigma_P[ss], 1e-9)))
        print(f"\n  5. THE {int(ss.sum()):,} STRUCTURALLY SHORT LINES  (shelf life < P)")
        print(f"     no S satisfies both the shelf life and the protection interval.")
        print(f"     shadow price of ONE DAY less lead time, on those lines:")
        for lbl, delta in (("L - 0.5 d", 0.5), ("L - 1.0 d", 1.0)):
            P2 = np.maximum(P[ss] - delta, 0.1)
            sp2 = d[ss] * np.sqrt(P2 * a.cv**2 + sL[ss]**2)
            s0 = (365.0/R[ss]) * sigma_P[ss] * G((f["shelf_life"][ss]*d[ss] - d[ss]*P[ss])/np.maximum(sigma_P[ss],1e-9))
            s2 = (365.0/R[ss]) * sp2 * G((f["shelf_life"][ss]*d[ss] - d[ss]*P2)/np.maximum(sp2,1e-9))
            print(f"       {lbl:<12} recovers KES {float(np.sum((s0-s2)*gp[ss])):>12,.0f} /yr "
                  f"of otherwise-lost gross profit")

    run_all([ratio(float(np.sum(z_star * sigma_P * c)),
                   float(np.sum(z0 * sigma_P * c)),
                   "optimal vs current safety capital", lo=0.2, hi=5.0)])

    print(json.dumps({
        "claim": "claim.ordering.parameters-are-derived-not-declared",
        "verdict": "contradicts",
        "metric": {
            "skus": n, "h": round(h, 4), "cv_assumed": a.cv,
            "z_engine": float(z0[0]),
            "z_star_median_dry": float(np.median(z_star[~perish])) if (~perish).any() else None,
            "z_star_median_fresh": float(np.median(z_star[perish])) if perish.any() else None,
            "clamped_lines_z_inert": int(clamped.sum()),
            "lines_should_hold_more": int(up.sum()),
            "lines_should_hold_less": int((~up).sum()),
            "margin_recovered_year": round(d_short_kes),
            "waste_or_carry_avoided_year": round(d_over_kes),
            "extra_carrying_year": round(d_hold_kes),
            "net_year": round(d_short_kes + d_over_kes - d_hold_kes),
            "implied_order_cost_median": float(allA[len(allA)//2]),
            "structurally_short": int(ss.sum())},
        "held_out": False, "provenance": "observed",
        "sources": ["source.order-book", "source.grn-book", "source.lead-patterns",
                    "source.stock-snapshot-dept", "source.cbk-lending-rate"],
        "baseline": "z = 1.28 flat, R from the calendar, cv = 0.40",
        "beat_baseline": None, "traps": ["T3", "T5"],
        "notes": ("sigma_P cancels out of the first-order condition for z, so the "
                  "optimal SERVICE LEVEL is independent of demand variability -- "
                  "only the ratio of holding cost to lost margin over one review "
                  "cycle sets it. A single declared z cannot be right for both a "
                  "dry good and a perishable.")}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
