"""
Risk backtest harness (Risk-Scoring Redesign, P4a).

Measures how well the P3 newsvendor baseline predicts **realized** stockouts, so
we have a quantified bar (PR-AUC + calibration) that any ML model (P4) or the
GNN (P6) must beat before it is allowed near operations.

Method
------
1. Reconstruct each SKU's daily on-hand series over the data window
   (2025-01-01 → 2026-01-20). Opening at the window start is estimated by walking
   the net balance back from the known dated STOCK snapshot (the 2026-01-20
   anchor), then the P1 engine forward-simulates with lost-sales accounting.
2. At a fixed review stride, score the as-of on-hand with the P3 risk and label
   it with whether a stockout actually occurs in the next ``horizon`` days.
3. Pool (risk, label) pairs across SKUs and compute average precision (PR-AUC),
   base rate, Brier score, and a calibration table.

Metric + sampling functions are **pure** and unit tested; ``run_backtest`` is the
integration entry that loads the real data.
"""

from __future__ import annotations

from typing import List, Sequence, Tuple

from .risk_baseline import stockout_probability
from .stockout_ledger import DayFlow, simulate_ledger

Pair = Tuple[float, int]


# ── pure metrics ────────────────────────────────────────────────────────────
def base_rate(pairs: Sequence[Pair]) -> float:
    """Fraction of positive (stockout) samples (pure)."""
    n = len(pairs)
    return round(sum(y for _, y in pairs) / n, 4) if n else 0.0


def average_precision(pairs: Sequence[Pair]) -> float:
    """Area under the precision-recall curve (average precision), in [0,1] (pure).

    A perfect ranker (all positives scored above all negatives) returns 1.0;
    a random ranker returns ≈ base rate.
    """
    pos = sum(1 for _, y in pairs if y == 1)
    if pos == 0:
        return 0.0
    ordered = sorted(pairs, key=lambda x: -x[0])
    tp = fp = 0
    ap = 0.0
    prev_recall = 0.0
    for _, y in ordered:
        if y == 1:
            tp += 1
        else:
            fp += 1
        recall = tp / pos
        precision = tp / (tp + fp)
        ap += (recall - prev_recall) * precision
        prev_recall = recall
    return round(ap, 4)


def brier_score(pairs: Sequence[Pair]) -> float:
    """Mean squared error of the probability forecast (pure); lower is better."""
    n = len(pairs)
    if not n:
        return 0.0
    return round(sum((s - y) ** 2 for s, y in pairs) / n, 4)


def calibration_table(pairs: Sequence[Pair], n_buckets: int = 10) -> List[dict]:
    """Predicted-probability buckets vs observed stockout frequency (pure)."""
    rows = []
    for b in range(n_buckets):
        lo, hi = b / n_buckets, (b + 1) / n_buckets
        bucket = [(s, y) for s, y in pairs
                  if (lo <= s < hi) or (b == n_buckets - 1 and s >= hi - 1e-9)]
        if not bucket:
            rows.append({"bucket": f"{lo:.1f}-{hi:.1f}", "n": 0,
                         "pred": None, "observed": None})
            continue
        rows.append({
            "bucket": f"{lo:.1f}-{hi:.1f}", "n": len(bucket),
            "pred": round(sum(s for s, _ in bucket) / len(bucket), 3),
            "observed": round(sum(y for _, y in bucket) / len(bucket), 3),
        })
    return rows


# ── pure sampling ────────────────────────────────────────────────────────────
def estimate_opening(anchor_soh: float, receipts: float, demand: float,
                     outflow: float) -> float:
    """Estimate on-hand at the window start by removing the net flow that occurred
    between the start and the anchor snapshot (pure). Clamped at 0."""
    net = float(receipts or 0) - float(demand or 0) - float(outflow or 0)
    return max(0.0, float(anchor_soh or 0) - net)


def backtest_samples(states, mu_ltd: float, sigma_ltd: float,
                     horizon: int = 7, stride: int = 7) -> List[Pair]:
    """(risk, label) pairs for one reconstructed series (pure).

    risk  = P3 stockout probability of the as-of on-hand at each stride point;
    label = 1 if any stockout occurs in the next ``horizon`` periods.
    Stops early enough that every sample has a full forward window.
    """
    n = len(states)
    h = max(1, int(horizon))
    stride = max(1, int(stride))
    so = [1 if s.stockout else 0 for s in states]
    pairs: List[Pair] = []
    for i in range(0, n - h, stride):
        risk = stockout_probability(states[i].soh_end, mu_ltd, sigma_ltd)
        label = 1 if any(so[i + 1:i + 1 + h]) else 0
        pairs.append((risk, label))
    return pairs


def summarize(pairs: Sequence[Pair]) -> dict:
    """Headline backtest metrics for a pooled set of (risk, label) pairs (pure)."""
    return {
        "samples": len(pairs),
        "base_rate": base_rate(pairs),
        "pr_auc": average_precision(pairs),
        "brier": brier_score(pairs),
        "calibration": calibration_table(pairs),
    }


# ── integration entry ────────────────────────────────────────────────────────
def run_backtest(data_dir: str, *, anchor="2026-01-20", horizon: int = 7,
                 stride: int = 7, min_ads: float = 0.3, sample_cap: int = 3000) -> dict:
    """Reconstruct the real Rhapta series and measure the P3 baseline (integration).

    Samples SKUs with meaningful demand (ads >= min_ads), reconstructs daily
    on-hand from the anchor-derived opening, and pools (risk, label) pairs.
    """
    import datetime as dt

    from . import ledger_loader as L
    from . import risk_features as RF

    master = L.load_master(data_dir)
    moves = L.load_movements(data_dir)
    psm, sp = RF.load_supply_map(data_dir)
    demand = RF.load_demand_series(data_dir)
    anchor_d = dt.date.fromisoformat(anchor)

    # window start = earliest movement date
    all_days = [d for s in moves.values() for d in s]
    if not all_days:
        return {"error": "no movements loaded"}
    start = min(all_days)
    end = min(anchor_d, max(all_days))
    days = [start + dt.timedelta(days=i) for i in range((end - start).days + 1)]

    pooled: List[Pair] = []
    used = 0
    for bc, info in master.items():
        if used >= sample_cap:
            break
        nm = info["name"].upper()
        if nm not in demand or not demand[nm] or nm not in psm or psm[nm] not in sp:
            continue
        row = RF.feature_row(info["name"], info["stock"], demand[nm], sp[psm[nm]])
        ads = row["ads"]
        if ads < min_ads:
            continue
        flow = moves.get(bc, {})
        # within-window flow totals to estimate the opening
        rec = sum(f.receipts for d, f in flow.items() if start <= d <= anchor_d)
        out = sum(f.outflow for d, f in flow.items() if start <= d <= anchor_d)
        dem = ads * len(days)
        opening = estimate_opening(info["stock"], rec, dem, out)
        per = {d: DayFlow(receipts=flow.get(d, DayFlow()).receipts,
                          outflow=flow.get(d, DayFlow()).outflow, demand=ads)
               for d in days}
        states = simulate_ledger(opening, days, per)
        pooled.extend(backtest_samples(states, row["mu_ltd"], row["sigma_ltd"],
                                       horizon=horizon, stride=stride))
        used += 1

    out = summarize(pooled)
    out["skus_sampled"] = used
    return out
