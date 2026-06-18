"""
Risk feature builder (Risk-Scoring Redesign, P2).

Turns the raw signals — demand series, supplier patterns, and the ledger's
position — into the per-SKU feature vector the risk model trains on, plus the
**lead-time demand distribution** (`μ_LTD`, `σ_LTD`) that the P3 newsvendor
baseline and the safety-stock formula consume directly.

Design notes
------------
* All feature math here is **pure** (no I/O) and unit tested. Thin JSON loaders
  at the bottom are integration-level (skipped in CI).
* Features are computed from *available* aggregates; when wired to the live POS
  stream the same functions run on trailing windows as-of each date (leak-free)
  with no change to the math.
* The catalog is demand-**intermittent** (most SKUs < 0.4 units/day), so the
  daily-demand σ uses ``max(cv-based, Poisson)`` — Poisson (`σ=√λ`) dominates
  for slow movers, the CV estimate for volatile fast movers.
"""

from __future__ import annotations

import math
from typing import Dict, Optional, Tuple

DAYS_PER_MONTH = 30.4
_DEFAULT_LEAD_DAYS = 3.0


# ── demand ─────────────────────────────────────────────────────────────────
def demand_stats(monthly_sales: Dict[str, float], total_months: int = 12) -> dict:
    """Demand level / volatility / intermittency from a {month: qty} series (pure).

    ``total_months`` is the calendar window the series is drawn from (used for
    intermittency: months_active / total_months).
    """
    vals = [float(v or 0) for v in (monthly_sales or {}).values()]
    if not vals:
        return {"ads": 0.0, "mean_monthly": 0.0, "cv": 0.0,
                "months_active": 0, "intermittency": 1.0, "total": 0.0}
    n = len(vals)
    total = sum(vals)
    months_active = sum(1 for v in vals if v > 0)
    mean_monthly = total / n
    if n > 1 and mean_monthly > 0:
        var = sum((v - mean_monthly) ** 2 for v in vals) / n  # population
        cv = math.sqrt(var) / mean_monthly
    else:
        cv = 0.0
    # Clamp ADS at 0: the source series can go net-negative when returns exceed
    # sales in a month, but negative demand is meaningless for stockout risk.
    ads = max(0.0, total) / (n * DAYS_PER_MONTH)
    denom = max(int(total_months), n)
    intermittency = 1.0 - (months_active / denom) if denom else 1.0
    return {"ads": round(ads, 5), "mean_monthly": round(mean_monthly, 3),
            "cv": round(cv, 4), "months_active": months_active,
            "intermittency": round(intermittency, 4), "total": round(total, 2)}


def daily_demand_sigma(ads: float, cv: float) -> float:
    """Daily-demand standard deviation (pure).

    ``max(cv-based, Poisson)``: the CV estimate (ads·cv) captures volatile fast
    movers; the Poisson floor (√ads) captures intermittent slow movers whose
    monthly CV understates day-to-day lumpiness.
    """
    ads = max(0.0, float(ads or 0))
    cv = max(0.0, float(cv or 0))
    return max(ads * cv, math.sqrt(ads))


# ── supply ──────────────────────────────────────────────────────────────────
def supply_stats(pattern: Optional[dict], default_lead: float = _DEFAULT_LEAD_DAYS) -> dict:
    """Lead-time / reliability features from a supplier_patterns entry (pure)."""
    p = pattern or {}
    lead = float(p.get("estimated_delivery_days", default_lead) or default_lead)
    lead = max(0.5, lead)
    lead_sd = max(0.0, float(p.get("lata_stdev_days", 0.0) or 0.0))
    fulfillment = float(p.get("lata_avg_fulfillment_rate", 1.0) or 1.0)
    reliability = float(p.get("reliability_score", 50) or 50) / 100.0
    return {"lead_time_days": round(lead, 3), "lead_time_sd": round(lead_sd, 3),
            "fulfillment_rate": max(0.0, min(1.0, fulfillment)),
            "reliability": max(0.0, min(1.0, reliability))}


# ── the bridge to the newsvendor baseline (P3) ──────────────────────────────
def leadtime_demand_distribution(ads: float, sigma_d: float,
                                 lead_time: float, lead_time_sd: float) -> Tuple[float, float]:
    """(μ_LTD, σ_LTD): mean & std of demand over the replenishment lead time (pure).

    Standard stochastic-demand-and-lead-time result:
        μ_LTD = ADS · L
        σ_LTD = sqrt( L·σ_d²  +  ADS²·σ_L² )
    This is what safety stock keys off: safety_stock = z(service_level) · σ_LTD.
    """
    ads = max(0.0, float(ads or 0))
    sigma_d = max(0.0, float(sigma_d or 0))
    L = max(0.0, float(lead_time or 0))
    sL = max(0.0, float(lead_time_sd or 0))
    mu = ads * L
    var = L * (sigma_d ** 2) + (ads ** 2) * (sL ** 2)
    return mu, math.sqrt(max(0.0, var))


# ── assembly ────────────────────────────────────────────────────────────────
def feature_row(name: str, soh: float, monthly_sales: Dict[str, float],
                pattern: Optional[dict], impact: Optional[dict] = None,
                total_months: int = 12) -> dict:
    """Assemble the full per-SKU feature vector (pure).

    Combines demand + supply + position + the LTD distribution + impact weight.
    ``impact`` is an optional sales_profitability entry (sales_rank/revenue/margin).
    """
    from .stockout_ledger import days_of_cover

    d = demand_stats(monthly_sales, total_months)
    s = supply_stats(pattern)
    sigma_d = daily_demand_sigma(d["ads"], d["cv"])
    mu_ltd, sigma_ltd = leadtime_demand_distribution(
        d["ads"], sigma_d, s["lead_time_days"], s["lead_time_sd"])
    soh = max(0.0, float(soh or 0))
    imp = impact or {}
    return {
        "name": name,
        # position
        "soh": soh,
        "days_of_cover": days_of_cover(soh, d["ads"]),
        # demand
        "ads": d["ads"], "demand_cv": d["cv"],
        "months_active": d["months_active"], "intermittency": d["intermittency"],
        "sigma_d": round(sigma_d, 5),
        # supply
        "lead_time_days": s["lead_time_days"], "lead_time_sd": s["lead_time_sd"],
        "fulfillment_rate": s["fulfillment_rate"], "reliability": s["reliability"],
        # lead-time demand distribution (P3 bridge)
        "mu_ltd": round(mu_ltd, 4), "sigma_ltd": round(sigma_ltd, 4),
        # cover relative to lead-time demand — the intuitive risk ratio
        "cover_vs_ltd": round(soh / mu_ltd, 3) if mu_ltd > 1e-9 else float("inf"),
        # impact
        "sales_rank": imp.get("sales_rank"),
        "revenue": float(imp.get("revenue", 0) or 0),
        "margin_pct": float(imp.get("margin_pct", 0) or 0),
    }


# ── thin loaders (integration; not in CI) ───────────────────────────────────
def load_supply_map(data_dir: str) -> Tuple[dict, dict]:
    """(product->supplier, supplier->pattern) from the JSON intelligence files."""
    import json
    import os
    psm_raw = json.load(open(os.path.join(data_dir, "product_supplier_map.json"), encoding="utf-8"))
    product_supplier = {}
    for name, v in psm_raw.items():
        sup = v if isinstance(v, str) else (
            v.get("supplier") if isinstance(v, dict) else (v[0] if isinstance(v, list) and v else None))
        if sup:
            product_supplier[str(name).strip().upper()] = str(sup).strip().upper()
    sp_path = os.path.join(data_dir, "supplier_patterns_2025.json")
    sp_raw = json.load(open(sp_path, encoding="utf-8"))
    supplier_pattern = {str(k).strip().upper(): v for k, v in sp_raw.items()}
    return product_supplier, supplier_pattern


def load_demand_series(data_dir: str) -> dict:
    """Upper-cased name -> monthly_sales dict from sales_forecasting."""
    import json
    import os
    raw = json.load(open(os.path.join(data_dir, "sales_forecasting_2025"), encoding="utf-8")) \
        if os.path.exists(os.path.join(data_dir, "sales_forecasting_2025")) \
        else json.load(open(os.path.join(data_dir, "sales_forecasting_2025 (1).json"), encoding="utf-8"))
    return {str(k).strip().upper(): (v.get("monthly_sales", {}) if isinstance(v, dict) else {})
            for k, v in raw.items()}
