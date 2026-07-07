"""
Multi-Store POS Database Builder
=================================

Creates a SINGLE SQLite database with 5 stores (ORG001–ORG005), each with a
differentiated stock profile seeded from the real Rhapta dept_*.xlsx catalogue.

Differences per store:
    • **Assortment** — some stores carry a subset of the full catalogue.
    • **Stock depth** — multiplied by per-department weights (e.g. Karen stocks
      heavy on staples, Westgate is lean everywhere).
    • **Pricing** — slight variance (+/- 3%) reflecting location premiums.
    • **Same ITEM_MST** — all five stores share the same item master (barcodes),
      but STOCK_MASTER / BASIC_SP_MST / BASIC_CP_MST rows are per-org.

Usage (via entrypoint):
    python entrypoint.py --mode build-multi-store-db

Reuses the canonical RXL schema (mock_pos_erp.SCHEMA_SQL) and the single-store
builder's system seeds (users / tax / counters) so the dashboard still logs in.
"""

from __future__ import annotations

import os
import random
import sqlite3
from datetime import datetime
from typing import Dict, List, Optional

from .mock_pos_erp import SCHEMA_SQL, MockPosErpBuilder
from .multi_store_profiles import (
    STORE_PROFILES,
    StoreProfile,
    dept_weight_for,
)

COST_RATIO = 0.82   # estimated cost as a fraction of sell price


def _reset_db(db_path: str) -> None:
    for suffix in ("", "-wal", "-shm"):
        p = db_path + suffix
        if os.path.exists(p):
            os.remove(p)


def _should_stock(profile: StoreProfile, dept: str, rng: random.Random) -> bool:
    """Decide whether this store carries a given item based on assortment_pct
    and department weight. Higher dept_weight → more likely to be included even
    when assortment_pct < 1."""
    base = profile.assortment_pct
    w = dept_weight_for(profile, dept)
    # Adjusted probability: blend assortment_pct with dept weight
    # A dept_weight of 1.5 on a 0.55 assortment store →  0.55 * min(1.5, 1.4) ≈ 0.77
    prob = min(1.0, base * max(0.3, w))
    return rng.random() < prob


def _stock_qty(catalog_qty: float, profile: StoreProfile,
               dept: str, rng: random.Random) -> float:
    """Compute the initial on-hand quantity for a SKU at this store.
    Applies global depth × department weight × small random jitter."""
    w = dept_weight_for(profile, dept)
    jitter = rng.uniform(0.85, 1.15)
    qty = catalog_qty * profile.stock_depth * w * jitter
    return round(max(0.0, qty), 1)


def _price_variant(base_price: float, profile: StoreProfile,
                   rng: random.Random) -> float:
    """Slight store-level price variation (+/- 3%)."""
    # Lavington charges a premium; Westgate a small one too (mall markup).
    premiums = {"ORG002": 1.03, "ORG004": 1.02, "ORG005": 1.01}
    mult = premiums.get(profile.org_cd, 1.0) * rng.uniform(0.98, 1.02)
    return round(base_price * mult, 2)


def build_multi_store_db(rows: List[dict], db_path: str,
                         profiles: Optional[List[StoreProfile]] = None,
                         reset: bool = True, seed: int = 42) -> dict:
    """Create a single SQLite DB with 5 stores from the catalogue rows.

    Parameters
    ----------
    rows : list[dict]
        Normalised catalogue rows from rhapta_catalog.load_catalog().
    db_path : str
        Path to the output SQLite file.
    profiles : list[StoreProfile], optional
        Override store profiles (defaults to STORE_PROFILES).
    reset : bool
        If True, delete existing DB first.
    seed : int
        Random seed for reproducibility.

    Returns
    -------
    dict  — summary with per-store stats.
    """
    profiles = profiles or STORE_PROFILES
    rng = random.Random(seed)

    if reset:
        _reset_db(db_path)
    os.makedirs(os.path.dirname(os.path.abspath(db_path)), exist_ok=True)

    b = MockPosErpBuilder(db_path=db_path)
    b.conn = sqlite3.connect(db_path)
    conn = b.conn
    conn.execute("PRAGMA journal_mode=WAL")
    b.org_codes = [p.org_cd for p in profiles]
    today = datetime.now().strftime("%Y-%m-%d")

    try:
        conn.executescript(SCHEMA_SQL)

        # ── 1. Organisations ─────────────────────────────────────────────
        for p in profiles:
            conn.execute(
                "INSERT OR REPLACE INTO ORGANIZATION_MST "
                "(ORG_CD, ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS, ORG_CITY, "
                " ORG_STATE, ORG_COUNTRY, CURRENCY_CD, LEVEL_NUMBER, ACTIVE_FLAG) "
                "VALUES (?,?,?,?,?,?,?,?,?,?)",
                (p.org_cd, p.name, p.short_name, p.address, p.city,
                 p.city, "KE", "KES", 1, "Y"))

        # ── 2. System seeds (auth / tax / counters / config) ─────────────
        os.environ.setdefault("OASIS_SEED_PASSWORD", "oasis2026")
        b._seed_system_preferences()
        b._seed_tax_plans()
        b._seed_counters()
        b._seed_customers()
        b._seed_oasis_users()
        b._seed_system_config()

        # ── 3. Suppliers (from catalogue vendors) ────────────────────────
        vendors = sorted({r["vendor"] for r in rows if r["vendor"]})
        vmap = {v: f"SUP{i:05d}" for i, v in enumerate(vendors, start=1)}
        conn.executemany(
            "INSERT OR IGNORE INTO SUPPLIER_MST "
            "(SUPPLIER_CD, SUPPLIER_NAME, ACTIVE_FLAG) VALUES (?,?,?)",
            [(cd, v, "Y") for v, cd in vmap.items()])

        # ── 4. Items (shared master) ─────────────────────────────────────
        conn.executemany(
            "INSERT OR REPLACE INTO ITEM_MST "
            "(ITM_CD, ITM_LONG_NAME, ITM_SHORT_NAME, SCAN_ITM_CD, UOM_CD, "
            " UOM_DESC, DEPARTMENT, SUPPLIER_CD, ITM_TYPE, ACTIVE_FLAG) "
            "VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(r["itm_cd"], r["name"] or r["itm_cd"],
              (r["name"] or r["itm_cd"])[:40],
              r["itm_cd"], "EA", "EACH", r["dept"],
              vmap.get(r["vendor"]), "F", "Y")
             for r in rows])

        # ── 5. Per-store stock, prices, assortment ───────────────────────
        store_stats: Dict[str, dict] = {}
        for profile in profiles:
            sp_rows, cp_rows, sm_rows = [], [], []
            stocked = 0
            total_units = 0.0
            dept_counts: Dict[str, int] = {}

            for r in rows:
                dept = r["dept"]
                if not _should_stock(profile, dept, rng):
                    continue   # this store doesn't carry this item

                base_price = float(r["price"] or 0)
                catalog_qty = max(0.0, float(r["stock"] or 0))
                sell_price = _price_variant(base_price, profile, rng)
                cost = round(sell_price * COST_RATIO, 2)
                qty = _stock_qty(catalog_qty, profile, dept, rng)

                sp_rows.append((profile.org_cd, r["itm_cd"], sell_price,
                                sell_price, today))
                cp_rows.append((profile.org_cd, r["itm_cd"], cost, today))
                sm_rows.append((profile.org_cd, r["itm_cd"], "MAIN",
                                qty, cost, today))
                stocked += 1
                total_units += qty
                dept_counts[dept] = dept_counts.get(dept, 0) + 1

            conn.executemany(
                "INSERT OR REPLACE INTO BASIC_SP_MST "
                "(BSP_ORG_CD, BSP_ITEM_CD, BSP_SP, BSP_MRP, BSP_EFF_DATE) "
                "VALUES (?,?,?,?,?)", sp_rows)
            conn.executemany(
                "INSERT OR REPLACE INTO BASIC_CP_MST "
                "(BCP_ORG_CD, BCP_ITEM_CD, BCP_CP, BCP_EFF_DATE) "
                "VALUES (?,?,?,?)", cp_rows)
            conn.executemany(
                "INSERT OR REPLACE INTO STOCK_MASTER "
                "(SM_ORG_CD, SM_ITM_CD, SM_LOC_CD, SM_QTY, SM_WAC, "
                " SM_LAST_RECV_DT) VALUES (?,?,?,?,?,?)", sm_rows)

            store_stats[profile.org_cd] = {
                "name": profile.name,
                "items_stocked": stocked,
                "total_units": round(total_units, 0),
                "departments": len(dept_counts),
                "assortment_pct": round(stocked / max(1, len(rows)) * 100, 1),
                "top_depts": sorted(dept_counts.items(),
                                    key=lambda x: -x[1])[:5],
            }

        conn.commit()
        summary = {
            "db_path": db_path,
            "stores": len(profiles),
            "catalog_skus": len(rows),
            "suppliers": len(vmap),
            "per_store": store_stats,
        }
        _print_summary(summary)
        return summary
    finally:
        conn.close()
        b.conn = None


def _print_summary(s: dict) -> None:
    """Pretty-print the build summary to stdout."""
    print(f"\n{'=' * 72}")
    print("  O.A.S.I.S. Multi-Store Database Built")
    print(f"{'=' * 72}")
    print(f"  DB Path    : {s['db_path']}")
    print(f"  Stores     : {s['stores']}")
    print(f"  Catalog    : {s['catalog_skus']:,} SKUs  |  {s['suppliers']} suppliers")
    print(f"{'-' * 72}")
    for org, st in s["per_store"].items():
        pct = st["assortment_pct"]
        bar_len = int(pct / 5)
        bar = "#" * bar_len + "." * (20 - bar_len)
        print(f"  {org}  {st['name'][:38]:<38}")
        print(f"         SKUs: {st['items_stocked']:>5,}  ({pct:>5.1f}%)  "
              f"Units: {st['total_units']:>8,.0f}  Depts: {st['departments']}")
        print(f"         [{bar}]")
    print(f"{'=' * 72}\n")



def build_multi_store_from_xlsx(data_dir: str, db_path: str,
                                 profiles: Optional[List[StoreProfile]] = None,
                                 seed: int = 42) -> dict:
    """End-to-end: load catalog from dept_*.xlsx → build multi-store DB."""
    from .rhapta_catalog import load_catalog
    rows = load_catalog(data_dir)
    return build_multi_store_db(rows, db_path, profiles=profiles, seed=seed)
