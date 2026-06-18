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


def calibrated_evaluation(train_pairs: Sequence[Pair], test_pairs: Sequence[Pair]) -> dict:
    """Fit isotonic calibration on train, evaluate on a held-out test set (pure).

    Monotonic calibration preserves ranking, so PR-AUC is unchanged; the win is
    Brier + calibration alignment. Fitting on a separate split avoids optimistic
    in-sample calibration.
    """
    from .risk_calibration import calibrate_pairs, fit_isotonic
    model = fit_isotonic(train_pairs)
    cal = calibrate_pairs(model, test_pairs)
    return {
        "test_samples": len(test_pairs),
        "pr_auc": average_precision(test_pairs),
        "brier_raw": brier_score(test_pairs),
        "brier_calibrated": brier_score(cal),
        "calibration_raw": calibration_table(test_pairs),
        "calibration_calibrated": calibration_table(cal),
    }


# ── de-circularized supply backtest (pure helpers) ──────────────────────────
def supply_risk(stat: dict) -> float:
    """Past supply-risk score from H1 supplier behavior (pure): worse of
    under-fill (1 - fill rate) and short-supply rate."""
    return max(0.0, min(1.0, max(1.0 - float(stat.get("fill", 1.0)),
                                 float(stat.get("short_rate", 0.0)))))


def supplier_failure_pairs(h1_stats: dict, h2_fail: dict) -> List[Pair]:
    """(supply_risk from H1, failed-in-H2) pairs for suppliers seen in both halves
    (pure). This is temporally separated, so it does NOT share inputs with the
    on-hand reconstruction — the de-circularized test."""
    pairs: List[Pair] = []
    for v, st in h1_stats.items():
        if v in h2_fail:
            pairs.append((supply_risk(st), int(h2_fail[v])))
    return pairs


# ── integration entry ────────────────────────────────────────────────────────
def run_backtest(data_dir: str, *, anchor="2026-01-20", horizon: int = 7,
                 stride: int = 7, min_ads: float = 0.3, sample_cap: int = 3000,
                 calibrate: bool = True) -> dict:
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
    groups: List[List[Pair]] = []   # per-SKU, for a leakage-free calibration split
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
        outf = sum(f.outflow for d, f in flow.items() if start <= d <= anchor_d)
        dem = ads * len(days)
        opening = estimate_opening(info["stock"], rec, dem, outf)
        per = {d: DayFlow(receipts=flow.get(d, DayFlow()).receipts,
                          outflow=flow.get(d, DayFlow()).outflow, demand=ads)
               for d in days}
        states = simulate_ledger(opening, days, per)
        samples = backtest_samples(states, row["mu_ltd"], row["sigma_ltd"],
                                   horizon=horizon, stride=stride)
        if samples:
            groups.append(samples)
            pooled.extend(samples)
        used += 1

    out = summarize(pooled)
    out["skus_sampled"] = used
    if calibrate and len(groups) >= 4:
        mid = len(groups) // 2  # SKU-level split: no within-SKU leakage
        train = [p for g in groups[:mid] for p in g]
        test = [p for g in groups[mid:] for p in g]
        out["calibrated"] = calibrated_evaluation(train, test)
    return out


def run_supply_backtest(data_dir: str, *, cutoff: str = "2025-07-01",
                        min_orders: int = 5, fail_fill: float = 0.9) -> dict:
    """De-circularized test: does H1 supplier behavior predict H2 supply failures?

    Independent of the on-hand reconstruction — features come from the supplier's
    first-half GRN fill rate + short-supply rate, the label from second-half
    under-fills / short-supply. Validates the supply-risk signal that feeds
    sigma_LTD without the risk↔label circularity of the inventory backtest.
    """
    import datetime as dt
    import glob
    import os

    import pandas as pd

    cut = dt.date.fromisoformat(cutoff)
    # vendor -> half -> [po_qty, grn_qty, short_lines, lines]
    agg = {}

    def _v(cell):
        return str(cell).split(" - ")[0].strip().upper()

    for f in sorted(set(glob.glob(os.path.join(data_dir, "*grnds*.xlsx")) +
                        glob.glob(os.path.join(data_dir, "grnds_*.xlsx")))):
        try:
            df = pd.read_excel(f)
        except Exception:
            continue
        df.columns = [str(c).strip() for c in df.columns]
        if not {"Vendor Code - Name", "GRN Date", "PO Qty", "GRN Qty"} <= set(df.columns):
            continue
        dates = pd.to_datetime(df["GRN Date"], errors="coerce", dayfirst=True)
        for v, d, po, grn in zip(df["Vendor Code - Name"], dates, df["PO Qty"], df["GRN Qty"]):
            if pd.isna(d):
                continue
            half = "h1" if d.date() < cut else "h2"
            a = agg.setdefault(_v(v), {"h1": [0.0, 0.0, 0, 0], "h2": [0.0, 0.0, 0, 0]})[half]
            po = float(po or 0)
            grn = float(grn or 0)
            a[0] += po
            a[1] += grn
            a[3] += 1
            if po > 0 and grn < po * fail_fill:
                a[2] += 1

    h1_stats, h2_fail = {}, {}
    for v, halves in agg.items():
        h1, h2 = halves["h1"], halves["h2"]
        if h1[3] < min_orders or h2[3] < min_orders or h1[0] <= 0 or h2[0] <= 0:
            continue
        h1_stats[v] = {"fill": h1[1] / h1[0], "short_rate": h1[2] / h1[3], "orders": h1[3]}
        h2_fill = h2[1] / h2[0]
        h2_fail[v] = 1 if (h2_fill < fail_fill or h2[2] > 0) else 0

    pairs = supplier_failure_pairs(h1_stats, h2_fail)
    out = summarize(pairs)
    out["suppliers"] = len(pairs)
    out["cutoff"] = cutoff
    return out
