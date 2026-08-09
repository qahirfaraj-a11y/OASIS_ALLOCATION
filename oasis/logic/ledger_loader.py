"""
Ledger loader (Risk-Scoring Redesign, P1b).

Parses the real anchor-store data files into the per-SKU ``DayFlow`` stream that the
pure ledger engine (:mod:`oasis.logic.stockout_ledger`) consumes, and joins in
the demand signal. This is the messy-I/O boundary, deliberately kept separate
from the pure engine so the engine stays CI-testable without the data.

Join strategy (verified coverage 2026-06-18):
  * **movements keyed by barcode** — GRN receipts, transfers in/out, vendor
    returns all carry a barcode that joins to the `dept_*` master at ~85-92%;
  * **demand keyed by item name** — `corrected_ads_from_pos.json` is keyed by
    name, joined to the master name at ~86%.

Flows for one organisation (default the anchor store, org 027):
  receipts = GRN Qty (grnds_*) + transfer-IN STI Qty (trn_*, To Org == org)
  outflow  = transfer-OUT STO Qty (trout_*, From Org == org) + return Rejc Qty (prts_*)
  demand   = ADS (corrected_ads, by name) spread across the period

Pure helpers (`norm_barcode`, `org_matches`) are unit tested; the file-reading
functions are integration-level (run against the real data, skipped in CI).
"""

from __future__ import annotations

import glob
import json
import os
from collections import defaultdict
from typing import Dict

from .stockout_ledger import DayFlow, simulate_ledger, stockout_summary

ANCHOR = "027"


# ── pure helpers (unit tested) ─────────────────────────────────────────────
def norm_barcode(v) -> str:
    """Normalise a barcode to a stable string key (pure).

    pandas often reads barcodes as floats ('6.16e+12' / '12345.0'); strip the
    float artifact and whitespace so the same code joins across files.
    """
    s = str(v).strip()
    if s.endswith(".0"):
        s = s[:-2]
    if "e" in s.lower() or "E" in s:
        try:
            s = str(int(float(s)))
        except (ValueError, OverflowError):
            pass
    return s


def org_matches(cell, org: str = ANCHOR) -> bool:
    """True if an 'NNN - Name' org cell refers to ``org`` (pure)."""
    return str(cell).strip().startswith(str(org))


# ── file loading (integration) ─────────────────────────────────────────────
def _read_excel(path: str, wanted: set):
    import pandas as pd
    try:
        df = pd.read_excel(path)
    except Exception:
        return None
    df.columns = [str(c).strip() for c in df.columns]
    have = [c for c in df.columns if c in wanted]
    return df[have] if have else None


def _to_date(series):
    import pandas as pd
    return pd.to_datetime(series, errors="coerce", dayfirst=True).dt.date


def load_master(data_dir: str) -> Dict[str, dict]:
    """barcode -> {name, stock} from all dept_*.xlsx (the catalog + stock snapshot)."""
    master: Dict[str, dict] = {}
    for f in sorted(glob.glob(os.path.join(data_dir, "dept_*.xlsx"))):
        df = _read_excel(f, {"BARCODE", "ITM_NAME", "STOCK", "DEPARTMENT"})
        if df is None:
            continue
        for _, r in df.iterrows():
            bc = norm_barcode(r.get("BARCODE"))
            if not bc or bc == "nan":
                continue
            master[bc] = {
                "name": str(r.get("ITM_NAME", "")).strip(),
                "stock": float(r.get("STOCK", 0) or 0),
                "dept": str(r.get("DEPARTMENT", "")).strip(),
            }
    return master


def load_movements(data_dir: str, org: str = ANCHOR) -> Dict[str, Dict[object, DayFlow]]:
    """barcode -> {date: DayFlow(receipts, outflow)} for one org (demand attached later)."""
    rec = defaultdict(lambda: defaultdict(float))   # receipts by (bc, date)
    out = defaultdict(lambda: defaultdict(float))    # outflow by (bc, date)

    # GRN receipts (org is the receiving store)
    for f in sorted(set(glob.glob(os.path.join(data_dir, "*grnds*.xlsx")) +
                        glob.glob(os.path.join(data_dir, "grnds_*.xlsx")))):
        df = _read_excel(f, {"Bar Code", "GRN Date", "GRN Qty", "Org Code - Name"})
        if df is None or "GRN Qty" not in df:
            continue
        if "Org Code - Name" in df:
            df = df[df["Org Code - Name"].map(lambda c: org_matches(c, org))]
        dates = _to_date(df["GRN Date"]) if "GRN Date" in df else [None] * len(df)
        for bc, d, q in zip(df["Bar Code"], dates, df["GRN Qty"]):
            if d is not None:
                rec[norm_barcode(bc)][d] += float(q or 0)

    # Transfers IN (To Org == org)
    for f in sorted(glob.glob(os.path.join(data_dir, "trn_*.xlsx"))):
        df = _read_excel(f, {"Bar Code", "STI Date", "STI Qty", "To Org Code/ Name"})
        if df is None or "STI Qty" not in df:
            continue
        if "To Org Code/ Name" in df:
            df = df[df["To Org Code/ Name"].map(lambda c: org_matches(c, org))]
        dates = _to_date(df["STI Date"]) if "STI Date" in df else [None] * len(df)
        for bc, d, q in zip(df["Bar Code"], dates, df["STI Qty"]):
            if d is not None:
                rec[norm_barcode(bc)][d] += float(q or 0)

    # Transfers OUT (From Org == org)
    for f in sorted(glob.glob(os.path.join(data_dir, "trout_*.xlsx"))):
        df = _read_excel(f, {"Bar Code", "STO Date", "STO Qty", "From Org Code/ Name"})
        if df is None or "STO Qty" not in df:
            continue
        if "From Org Code/ Name" in df:
            df = df[df["From Org Code/ Name"].map(lambda c: org_matches(c, org))]
        dates = _to_date(df["STO Date"]) if "STO Date" in df else [None] * len(df)
        for bc, d, q in zip(df["Bar Code"], dates, df["STO Qty"]):
            if d is not None:
                out[norm_barcode(bc)][d] += float(q or 0)

    # Vendor returns (org's returns out of stock)
    for f in sorted(glob.glob(os.path.join(data_dir, "prts_*.xlsx"))):
        df = _read_excel(f, {"Barcode", "Doc Date", "Rejc Qty", "Org Code/ Name"})
        if df is None or "Rejc Qty" not in df:
            continue
        if "Org Code/ Name" in df:
            df = df[df["Org Code/ Name"].map(lambda c: org_matches(c, org))]
        dates = _to_date(df["Doc Date"]) if "Doc Date" in df else [None] * len(df)
        for bc, d, q in zip(df["Barcode"], dates, df["Rejc Qty"]):
            if d is not None:
                out[norm_barcode(bc)][d] += float(q or 0)

    barcodes = set(rec) | set(out)
    series: Dict[str, Dict[object, DayFlow]] = {}
    for bc in barcodes:
        days = set(rec[bc]) | set(out[bc])
        series[bc] = {d: DayFlow(receipts=rec[bc].get(d, 0.0),
                                 outflow=out[bc].get(d, 0.0)) for d in days}
    return series


def load_demand_ads(data_dir: str) -> Dict[str, float]:
    """Upper-cased item name -> corrected ADS (units/day) from POS."""
    path = os.path.join(data_dir, "corrected_ads_from_pos.json")
    try:
        d = json.load(open(path, encoding="utf-8"))
    except Exception:
        return {}
    out = {}
    for name, rec in d.items():
        ads = rec.get("new_ads", rec.get("old_ads", 0)) if isinstance(rec, dict) else rec
        out[str(name).strip().upper()] = float(ads or 0)
    return out


def feasibility_report(data_dir: str, org: str = ANCHOR) -> dict:
    """Load real flows + demand, run the ledger engine, and report join coverage,
    flow magnitudes, and a turnover-consistency check (the P1b validation).

    Returns a dict of diagnostics; raises nothing on a clean run.
    """
    master = load_master(data_dir)
    moves = load_movements(data_dir, org)
    ads_by_name = load_demand_ads(data_dir)

    matched = [bc for bc in moves if bc in master]
    # Build full series (receipts/outflow + spread demand) and run the engine
    import datetime as _dt
    recon = []
    for bc in matched:
        info = master[bc]
        flows = moves[bc]
        days = sorted(flows)
        if not days:
            continue
        ads = ads_by_name.get(info["name"].upper(), 0.0)
        span = max(1, (days[-1] - days[0]).days + 1) if isinstance(days[0], _dt.date) else len(days)
        receipts = sum(f.receipts for f in flows.values())
        outflow = sum(f.outflow for f in flows.values())
        demand_total = ads * span
        # Treat current STOCK as opening for a forward run with daily demand on
        # GRN/transfer days (engine smoke; exact SOH needs dated snapshots).
        per = {d: DayFlow(receipts=flows[d].receipts, outflow=flows[d].outflow,
                          demand=ads) for d in days}
        states = simulate_ledger(info["stock"], days, per)
        s = stockout_summary(states)
        recon.append({
            "barcode": bc, "name": info["name"], "stock": info["stock"],
            "receipts": receipts, "outflow": outflow, "ads": ads,
            "span_days": span, "annual_demand_est": round(demand_total, 1),
            "turnover_ratio": round(receipts / demand_total, 2) if demand_total > 0 else None,
            "stockout_rate": s["stockout_rate"],
        })

    n = len(recon)
    with_demand = [r for r in recon if r["ads"] > 0]
    return {
        "master_skus": len(master),
        "skus_with_movements": len(moves),
        "movement_join_rate": round(len(matched) / max(1, len(moves)), 3),
        "reconstructed_skus": n,
        "skus_with_demand": len(with_demand),
        "total_receipts": round(sum(r["receipts"] for r in recon), 1),
        "total_outflow": round(sum(r["outflow"] for r in recon), 1),
        "sample": recon[:8],
    }
