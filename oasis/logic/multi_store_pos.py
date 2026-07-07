"""
Multi-Store POS Simulator
==========================

Runs 5 affinity-aware POS streams **concurrently** (one thread per store),
each with traffic cadence, basket size, and hero-SKU concentration derived
from its StoreProfile. All streams write to the SAME SQLite database (WAL
mode enables concurrent readers) so the Command Center sees all five stores
live.

Three operating modes:

    seed-multi-history  — pre-seed N prior days of demand per store
    multi-pos-stream    — real-time streaming (one receipt at a time, committed
                          instantly so the dashboard refreshes reflect it)
    multi-pos-sim       — batch mode for fast throughput testing

Stock integrity is authoritative: on-hand is checked per-store before
writing a receipt, and quantities never exceed availability.
"""

from __future__ import annotations

import random
import sqlite3
import threading
import time
from datetime import datetime, timedelta
from typing import Dict, List, Optional

from .multi_store_profiles import STORE_PROFILES, StoreProfile
from .pos_injector import SaleLine, build_bill
from .pos_simulator import (
    assign_popularity,
    generate_basket,
    ring_up,
    _load_dept_items,
    _seed_dept_weights,
    _weighted_pick,
)
from .vault_prior import load_prior


# -- Helpers ------------------------------------------------------------------

def _store_tag(profile: StoreProfile) -> str:
    """Short log prefix: ORG001/RHAPTA."""
    return f"{profile.org_cd}/{profile.short_name}"


def _connect(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path, timeout=120.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=120000")
    return conn


# -- Per-store streaming (runs in its own thread) -----------------------------

def _stream_one_store(
    db_path: str,
    profile: StoreProfile,
    prior: Dict[str, Dict[str, float]],
    batches: int,   # 0 = infinite
    stop_event: threading.Event,
    seed: Optional[int] = None,
    quiet: bool = False,
) -> dict:
    """Ring up baskets for one store in real-time, committing each receipt.

    Returns summary stats when stopped (Ctrl-C or stop_event).
    """
    rng = random.Random(seed)
    tag = _store_tag(profile)
    interval = profile.interval_seconds()
    conn = _connect(db_path)

    try:
        dept_items, meta = _load_dept_items(conn, profile.org_cd,
                                            core_per_dept=12)
        if not meta:
            return {"store": tag, "error": "no in-stock SKUs"}

        popularity = assign_popularity(list(meta), rng,
                                       exponent=profile.pop_exponent)
        seed_weights = _seed_dept_weights(dept_items, prior)

        bills = lines = oos = 0
        units = revenue = 0.0
        limit = batches if batches and batches > 0 else None
        if not quiet:
            print(f"[{tag}] streaming every {interval}s  "
                  f"skus={len(meta)} depts={len(dept_items)}  "
                  f"{'(' + str(limit) + ')' if limit else 'INF'}")

        b = 0
        while not stop_event.is_set() and (limit is None or b < limit):
            b += 1
            seed_dept = _weighted_pick(seed_weights, rng)
            codes = generate_basket(seed_dept, dept_items, popularity, prior,
                                    rng, max_attach=profile.max_attach)
            # Authoritative live on-hand
            live = {}
            for itm in codes:
                row = conn.execute(
                    "SELECT SM_QTY FROM STOCK_MASTER "
                    "WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
                    (profile.org_cd, itm)).fetchone()
                live[itm] = float(row[0]) if row and row[0] is not None else 0.0

            sale_lines, dec = ring_up(codes, live, meta, rng, profile.max_qty)
            oos += sum(1 for c in codes if live.get(c, 0.0) <= 0)
            if not sale_lines:
                continue

            today = datetime.now().strftime("%Y-%m-%d")
            bill_no = f"POS{profile.short_name[:3]}{datetime.now().strftime('%H%M%S%f')[:10]}"
            hdr, dtl = build_bill(profile.org_cd, bill_no, today, sale_lines)
            conn.execute(
                f"INSERT INTO POS_SALES_HDR ({','.join(hdr)}) "
                f"VALUES ({','.join(['?'] * len(hdr))})", list(hdr.values()))
            for d in dtl:
                conn.execute(
                    f"INSERT INTO POS_SALES_DTL ({','.join(d)}) "
                    f"VALUES ({','.join(['?'] * len(d))})", list(d.values()))
            for itm, nq in dec.items():
                conn.execute(
                    "UPDATE STOCK_MASTER SET SM_QTY=?, SM_LAST_ISSUE_DT=? "
                    "WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
                    (nq, today, profile.org_cd, itm))
            conn.commit()

            bills += 1
            value = sum(s.qty * s.sell_price for s in sale_lines)
            units += sum(s.qty for s in sale_lines)
            lines += len(sale_lines)
            revenue += value

            if not quiet:
                ts = datetime.now().strftime("%H:%M:%S")
                depleted = [s.itm_cd for s in sale_lines
                            if dec.get(s.itm_cd, 1) <= 0]
                # ASCII only, and never let a console-encoding error kill a
                # till thread (Windows cp1252 stdout can't print emoji).
                tail = f"  ! {len(depleted)} depleted" if depleted else ""
                try:
                    print(f"  [{ts}] {tag}  {bill_no}  "
                          f"{len(sale_lines)} items  KES {value:,.0f}{tail}")
                except (UnicodeEncodeError, OSError):
                    pass

            if interval and not stop_event.is_set():
                stop_event.wait(timeout=max(0.01, interval))

        return {"store": tag, "bills": bills, "lines": lines,
                "units": round(units), "revenue": round(revenue),
                "oos_skipped": oos}
    finally:
        conn.commit()
        conn.close()


# -- Multi-store concurrent streaming ----------------------------------------

def stream_multi_store(
    db_path: str,
    prior_path: Optional[str] = None,
    batches: int = 0,
    profiles: Optional[List[StoreProfile]] = None,
    seed: Optional[int] = None,
) -> dict:
    """Launch one streaming thread per store. Runs until Ctrl-C.

    All threads share the same SQLite file (WAL mode). Each thread has its own
    connection and its own traffic cadence.
    """
    profiles = profiles or STORE_PROFILES
    prior = load_prior(prior_path) if prior_path else {}
    stop = threading.Event()
    results: Dict[str, dict] = {}
    threads: List[threading.Thread] = []

    print(f"\n{'=' * 72}")
    print("  O.A.S.I.S. Multi-Store POS Stream")
    print(f"  {len(profiles)} stores  |  DB: {db_path}")
    print(f"  Prior: {'ON (' + str(len(prior)) + ' depts)' if prior else 'OFF'}")
    print(f"{'=' * 72}\n")

    def _run(p: StoreProfile, s: int):
        try:
            results[p.org_cd] = _stream_one_store(
                db_path, p, prior, batches=batches,
                stop_event=stop, seed=s)
        except Exception as e:
            results[p.org_cd] = {"store": _store_tag(p), "error": str(e)}

    rng = random.Random(seed)
    for p in profiles:
        t = threading.Thread(target=_run, args=(p, rng.randint(0, 2**31)),
                             daemon=True, name=f"pos-{p.org_cd}")
        threads.append(t)
        t.start()
        # Stagger start by 0.3s so log lines don't all collide
        time.sleep(0.3)

    try:
        while any(t.is_alive() for t in threads):
            time.sleep(0.5)
    except KeyboardInterrupt:
        print(f"\n[multi-pos] Ctrl-C — stopping all {len(profiles)} stores...")
        stop.set()
        for t in threads:
            t.join(timeout=10)

    _print_stream_summary(results)
    return results


# -- Multi-store demand history seeder ----------------------------------------

def seed_multi_store_history(
    db_path: str,
    prior_path: Optional[str] = None,
    profiles: Optional[List[StoreProfile]] = None,
    seed: Optional[int] = None,
) -> dict:
    """Seed prior-day demand history for ALL stores (sequential).

    Each store's history density (bills_per_day) and depth come from its
    StoreProfile. Writes dated POS bills for each store WITHOUT decrementing
    stock (the history represents prior replenished days).
    """
    profiles = profiles or STORE_PROFILES
    prior = load_prior(prior_path) if prior_path else {}
    rng = random.Random(seed)
    today = datetime.now().date()

    print(f"\n{'=' * 72}")
    print("  Seeding Multi-Store Demand History")
    print(f"{'=' * 72}")

    all_stats = {}
    conn = _connect(db_path)
    try:
        for profile in profiles:
            tag = _store_tag(profile)
            dept_items, meta = _load_dept_items(conn, profile.org_cd,
                                                core_per_dept=12)
            if not meta:
                all_stats[profile.org_cd] = {"store": tag, "error": "no SKUs"}
                continue

            popularity = assign_popularity(list(meta), rng,
                                           exponent=profile.pop_exponent)
            seed_weights = _seed_dept_weights(dept_items, prior)

            hdr_rows: List[tuple] = []
            dtl_rows: List[tuple] = []
            hdr_cols = dtl_cols = None
            bills = lines = seq = 0

            for d in range(profile.history_days, 0, -1):
                bdate = (today - timedelta(days=d)).strftime("%Y-%m-%d")
                for _ in range(profile.history_bills_per_day):
                    sd = _weighted_pick(seed_weights, rng)
                    codes = generate_basket(sd, dept_items, popularity, prior,
                                            rng, max_attach=profile.max_attach)
                    if not codes:
                        continue
                    sale_lines = [
                        SaleLine(itm, meta[itm][0],
                                 float(rng.randint(1, profile.max_qty)),
                                 meta[itm][1])
                        for itm in codes if itm in meta
                    ]
                    if not sale_lines:
                        continue
                    seq += 1
                    bn = f"H{profile.short_name[:3]}{bdate.replace('-', '')}{seq:06d}"
                    hdr, dtl = build_bill(profile.org_cd, bn, bdate, sale_lines)
                    if hdr_cols is None:
                        hdr_cols, dtl_cols = list(hdr), list(dtl[0])
                    hdr_rows.append(tuple(hdr[c] for c in hdr_cols))
                    dtl_rows.extend(tuple(x[c] for c in dtl_cols) for x in dtl)
                    bills += 1
                    lines += len(sale_lines)

            if hdr_cols:
                conn.executemany(
                    f"INSERT INTO POS_SALES_HDR ({','.join(hdr_cols)}) "
                    f"VALUES ({','.join(['?'] * len(hdr_cols))})", hdr_rows)
                conn.executemany(
                    f"INSERT INTO POS_SALES_DTL ({','.join(dtl_cols)}) "
                    f"VALUES ({','.join(['?'] * len(dtl_cols))})", dtl_rows)
                conn.commit()

            all_stats[profile.org_cd] = {
                "store": tag,
                "history_days": profile.history_days,
                "bills_per_day": profile.history_bills_per_day,
                "total_bills": bills,
                "total_lines": lines,
            }
            print(f"  {tag:<18} {bills:>6,} bills  "
                  f"({profile.history_days}d × {profile.history_bills_per_day}/d)  "
                  f"{lines:>8,} lines")

    finally:
        conn.commit()
        conn.close()

    print(f"{'=' * 72}\n")
    return all_stats


def _print_stream_summary(results: Dict[str, dict]) -> None:
    print(f"\n{'=' * 72}")
    print("  Multi-Store POS Stream -- Session Summary")
    print(f"{'-' * 72}")
    total_bills = total_lines = 0
    total_rev = 0.0
    for org, r in sorted(results.items()):
        if r.get("error"):
            print(f"  {r['store']:<18}  ERROR: {r['error']}")
            continue
        b = r.get("bills", 0)
        l = r.get("lines", 0)
        rev = r.get("revenue", 0)
        total_bills += b
        total_lines += l
        total_rev += rev
        print(f"  {r['store']:<18}  {b:>5,} bills  "
              f"{l:>7,} lines  KES {rev:>10,.0f}")
    print(f"{'-' * 72}")
    print(f"  {'TOTAL':<18}  {total_bills:>5,} bills  "
          f"{total_lines:>7,} lines  KES {total_rev:>10,.0f}")
    print(f"{'=' * 72}\n")

