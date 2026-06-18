"""
Newsvendor risk baseline (Risk-Scoring Redesign, P3).

The first *real* risk number — and the yardstick every ML model (P4) and the
GNN (P6) must beat on a held-out backtest before touching operations. Pure,
interpretable, no training, no torch.

From a P2 feature row (`μ_LTD`, `σ_LTD`, `soh`) it computes:

    P(stockout before resupply) = P( demand-during-lead-time > available stock )
    safety_stock                = z(service_level) · σ_LTD
    reorder_point               = μ_LTD + safety_stock

Demand model is **hybrid** to suit the intermittent catalog: a Poisson tail for
low-volume SKUs (μ_LTD < 20 — the majority), the normal approximation for
high-volume ones. No SciPy: the normal CDF uses ``math.erf`` and the inverse
normal (z-score) uses Acklam's rational approximation.
"""

from __future__ import annotations

import math

_INTERMITTENT_MU = 20.0   # below this lead-time demand, use the Poisson tail


# ── distribution primitives (pure) ──────────────────────────────────────────
def norm_cdf(x: float) -> float:
    """Standard normal CDF Φ(x) via the error function."""
    return 0.5 * (1.0 + math.erf(float(x) / math.sqrt(2.0)))


def z_score(p: float) -> float:
    """Inverse standard normal CDF (probit) — Acklam's approximation.

    z_score(0.5)=0, z_score(0.975)≈1.96. Used for the service-level safety factor.
    """
    p = min(1 - 1e-9, max(1e-9, float(p)))
    a = [-3.969683028665376e+01, 2.209460984245205e+02, -2.759285104469687e+02,
         1.383577518672690e+02, -3.066479806614716e+01, 2.506628277459239e+00]
    b = [-5.447609879822406e+01, 1.615858368580409e+02, -1.556989798598866e+02,
         6.680131188771972e+01, -1.328068155288572e+01]
    c = [-7.784894002430293e-03, -3.223964580411365e-01, -2.400758277161838e+00,
         -2.549732539343734e+00, 4.374664141464968e+00, 2.938163982698783e+00]
    d = [7.784695709041462e-03, 3.224671290700398e-01, 2.445134137142996e+00,
         3.754408661907416e+00]
    plow, phigh = 0.02425, 1 - 0.02425
    if p < plow:
        q = math.sqrt(-2 * math.log(p))
        return (((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
               ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)
    if p <= phigh:
        q = p - 0.5
        r = q * q
        return (((((a[0] * r + a[1]) * r + a[2]) * r + a[3]) * r + a[4]) * r + a[5]) * q / \
               (((((b[0] * r + b[1]) * r + b[2]) * r + b[3]) * r + b[4]) * r + 1)
    q = math.sqrt(-2 * math.log(1 - p))
    return -(((((c[0] * q + c[1]) * q + c[2]) * q + c[3]) * q + c[4]) * q + c[5]) / \
        ((((d[0] * q + d[1]) * q + d[2]) * q + d[3]) * q + 1)


def _poisson_cdf(k: int, lam: float) -> float:
    """P(X <= k) for X ~ Poisson(lam), by stable summation (lam kept small)."""
    if k < 0:
        return 0.0
    term = math.exp(-lam)
    s = term
    for i in range(1, int(k) + 1):
        term *= lam / i
        s += term
    return min(1.0, s)


# ── risk + safety stock (pure) ──────────────────────────────────────────────
def stockout_probability(available: float, mu_ltd: float, sigma_ltd: float) -> float:
    """P(demand during lead time exceeds ``available`` stock), in [0,1].

    ``available`` = on-hand + on-order. Poisson tail for low μ_LTD (intermittent),
    normal approximation otherwise.
    """
    mu = max(0.0, float(mu_ltd or 0))
    avail = max(0.0, float(available or 0))
    if mu <= 1e-9:
        return 0.0                                   # no demand -> no stockout
    if mu < _INTERMITTENT_MU:
        # P(D > avail) = 1 - P(D <= floor(avail))
        return max(0.0, min(1.0, 1.0 - _poisson_cdf(math.floor(avail), mu)))
    sd = float(sigma_ltd or 0)
    if sd <= 1e-9:
        return 1.0 if avail < mu else 0.0
    return max(0.0, min(1.0, 1.0 - norm_cdf((avail - mu) / sd)))


def safety_stock(sigma_ltd: float, service_level: float = 0.95) -> float:
    """z(service_level) · σ_LTD, floored at 0 (pure)."""
    return max(0.0, z_score(service_level) * max(0.0, float(sigma_ltd or 0)))


def reorder_point(mu_ltd: float, sigma_ltd: float, service_level: float = 0.95) -> float:
    """Cycle stock + safety stock = μ_LTD + z·σ_LTD (pure)."""
    return max(0.0, float(mu_ltd or 0)) + safety_stock(sigma_ltd, service_level)


def baseline_recommendation(row: dict, service_level: float = 0.95,
                            on_order: float = 0.0) -> dict:
    """Turn a P2 feature row into the baseline risk + replenishment recommendation.

    Returns stockout probability (the risk score), the safety stock / reorder
    point, and the recommended order = max(0, reorder_point − on-hand − on-order).
    """
    mu = float(row.get("mu_ltd", 0) or 0)
    sd = float(row.get("sigma_ltd", 0) or 0)
    soh = max(0.0, float(row.get("soh", 0) or 0))
    on_order = max(0.0, float(on_order or 0))
    available = soh + on_order

    prob = stockout_probability(available, mu, sd)
    ss = safety_stock(sd, service_level)
    rop = mu + ss
    rec = max(0.0, rop - available)
    return {
        "stockout_prob": round(prob, 4),    # the baseline risk score, 0..1
        "service_level": service_level,
        "safety_stock": round(ss, 3),
        "reorder_point": round(rop, 3),
        "available": round(available, 3),
        "recommended_order": round(rec, 3),
    }


def baseline_risk(row: dict, on_order: float = 0.0) -> float:
    """Convenience: just the baseline stockout-probability risk score (0..1)."""
    return stockout_probability(
        max(0.0, float(row.get("soh", 0) or 0)) + max(0.0, on_order),
        float(row.get("mu_ltd", 0) or 0), float(row.get("sigma_ltd", 0) or 0))
