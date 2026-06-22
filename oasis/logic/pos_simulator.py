"""
Affinity-aware mock POS simulator.

Where pos_injector rings up *random* baskets, this one plants realistic
co-purchase structure so the directional basket algorithm can recover genuine
SKU-level anchor -> attachment baskets:

  * a shopper trip starts at a seed department, picks an Anchor SKU there, then
    pulls Attachment SKUs from *complementary* departments drawn from the vault
    halo prior (vault_prior.basket_prior.json);
  * SKU popularity is Pareto/Zipfian, so a minority of "hero" SKUs recur across
    trips — that repetition is what gives specific SKU pairs lift > 1.

IMPORTANT (honesty): recovering these baskets validates the *pipeline wiring*
(the algorithm extracts affinity when it exists), NOT that the affinities are
ground truth — the planted structure comes from a coarse supply-side prior. Real
validation needs live customer co-purchase. This is a stand-in until that lands.

Purely transactional: it sells and decrements on-hand; it never restocks.
"""

from __future__ import annotations

import random
from typing import Dict, List, Optional

from .pos_injector import SaleLine, build_bill


# ── pure basket construction ─────────────────────────────────────────────────
def assign_popularity(item_codes: List[str], rng: random.Random,
                      exponent: float = 1.0) -> Dict[str, float]:
    """Zipfian popularity: shuffle, then weight 1/rank**exponent — a few heroes
    dominate (steeper exponent ⇒ more concentrated, so pairs recur faster)."""
    shuffled = list(item_codes)
    rng.shuffle(shuffled)
    return {itm: 1.0 / (i + 1) ** exponent for i, itm in enumerate(shuffled)}


def _weighted_pick(weights: Dict[str, float], rng: random.Random) -> Optional[str]:
    if not weights:
        return None
    keys = list(weights)
    return rng.choices(keys, weights=[max(1e-9, weights[k]) for k in keys], k=1)[0]


def _weighted_distinct(weights: Dict[str, float], k: int, rng: random.Random) -> List[str]:
    chosen: List[str] = []
    pool = dict(weights)
    for _ in range(min(k, len(pool))):
        pick = _weighted_pick(pool, rng)
        if pick is None:
            break
        chosen.append(pick)
        pool.pop(pick, None)
    return chosen


def generate_basket(seed_dept: str, dept_items: Dict[str, List[str]],
                    popularity: Dict[str, float], prior: Dict[str, Dict[str, float]],
                    rng: random.Random, max_attach: int = 3,
                    noise_p: float = 0.15) -> List[str]:
    """Build one basket: anchor (seed dept) + attachments (complementary depts)
    + occasional noise. Returns distinct item codes."""
    basket: List[str] = []

    def pick_from(dept: str):
        items = dept_items.get(dept) or []
        if not items:
            return None
        return _weighted_pick({i: popularity.get(i, 1e-6) for i in items}, rng)

    anchor = pick_from(seed_dept)
    if anchor:
        basket.append(anchor)

    comps = prior.get(seed_dept, {})
    n = rng.randint(1, max_attach)
    for dept in _weighted_distinct(comps, n, rng):
        a = pick_from(dept)
        if a:
            basket.append(a)

    if rng.random() < noise_p and dept_items:
        a = pick_from(rng.choice(list(dept_items)))
        if a:
            basket.append(a)

    seen, out = set(), []
    for i in basket:
        if i not in seen:
            seen.add(i)
            out.append(i)
    return out


def _seed_dept_weights(dept_items: Dict[str, List[str]],
                       prior: Dict[str, Dict[str, float]]) -> Dict[str, float]:
    """Seed departments weighted toward those that actually have halo partners."""
    return {d: (len(items) * (2.0 if d in prior else 0.5))
            for d, items in dept_items.items() if items}


# ── DB integration ───────────────────────────────────────────────────────────
def _load_dept_items(conn, org: str, core_per_dept: Optional[int] = None):
    """{dept: [itm]}, {itm: (name, price, stock)} for in-stock SKUs.

    With core_per_dept set, keep only the top-N highest-stock SKUs per department
    — a realistic fast-moving "core assortment" so co-purchase concentrates and
    SKU-level baskets become recoverable from a feasible number of trips.
    """
    rows = conn.execute(
        "SELECT i.DEPARTMENT, s.SM_ITM_CD, COALESCE(i.ITM_LONG_NAME,''), "
        "       COALESCE(sp.BSP_SP, 100.0), s.SM_QTY "
        "FROM STOCK_MASTER s JOIN ITEM_MST i ON i.ITM_CD = s.SM_ITM_CD "
        "LEFT JOIN BASIC_SP_MST sp ON sp.BSP_ITEM_CD = s.SM_ITM_CD AND sp.BSP_ORG_CD = s.SM_ORG_CD "
        "WHERE s.SM_ORG_CD = ? AND s.SM_QTY > 0 ORDER BY s.SM_QTY DESC", (org,)).fetchall()
    dept_items: Dict[str, List[str]] = {}
    meta: Dict[str, tuple] = {}
    for dept, itm, name, price, qty in rows:
        d = dept or "UNKNOWN"
        bucket = dept_items.setdefault(d, [])
        if core_per_dept and len(bucket) >= core_per_dept:
            continue
        bucket.append(itm)
        meta[itm] = (name, float(price or 0), float(qty or 0))
    return dept_items, meta


def run_simulator(db_path: str, prior_path: Optional[str] = None, batches: int = 50,
                  interval: float = 0.0, org: str = "ORG001", max_qty: int = 4,
                  core_per_dept: int = 12, pop_exponent: float = 1.2,
                  max_attach: int = 4, seed: Optional[int] = None) -> dict:
    """Stream affinity-structured baskets into the POS DB (sales only).

    Stock is held in memory and decremented there; bills and final stock are
    written in batches — so tens of thousands of trips run in seconds.
    """
    import sqlite3
    import time
    from datetime import datetime

    from .vault_prior import load_prior
    rng = random.Random(seed)
    prior = load_prior(prior_path) if prior_path else {}
    today = datetime.now().strftime("%Y-%m-%d")

    conn = sqlite3.connect(db_path, timeout=60.0)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("PRAGMA busy_timeout=60000")
    try:
        dept_items, meta = _load_dept_items(conn, org, core_per_dept=core_per_dept)
        if not meta:
            return {"error": "no in-stock SKUs", "org": org}
        popularity = assign_popularity(list(meta), rng, exponent=pop_exponent)
        seed_weights = _seed_dept_weights(dept_items, prior)
        stock = {itm: m[2] for itm, m in meta.items()}     # in-memory on-hand

        hdr_rows: List[tuple] = []
        dtl_rows: List[tuple] = []
        hdr_cols = dtl_cols = None
        bills = lines = 0
        units = 0.0
        touched: set = set()
        print(f"[pos-sim] {db_path} org={org} batches={batches} core/dept={core_per_dept} "
              f"active_skus={len(meta)} depts={len(dept_items)} prior={'on' if prior else 'off'}")
        for b in range(1, batches + 1):
            seed_dept = _weighted_pick(seed_weights, rng)
            codes = generate_basket(seed_dept, dept_items, popularity, prior, rng,
                                    max_attach=max_attach)
            sale_lines = []
            for itm in codes:
                if stock.get(itm, 0) <= 0:
                    continue
                name, price, _ = meta[itm]
                qty = min(stock[itm], float(rng.randint(1, max_qty)))
                stock[itm] = round(stock[itm] - qty, 3)
                touched.add(itm)
                sale_lines.append(SaleLine(itm, name, qty, price))
            if len(sale_lines) < 1:
                continue
            bill_no = f"POS{b:08d}"
            hdr, dtl = build_bill(org, bill_no, today, sale_lines)
            if hdr_cols is None:
                hdr_cols, dtl_cols = list(hdr), list(dtl[0])
            hdr_rows.append(tuple(hdr[c] for c in hdr_cols))
            dtl_rows.extend(tuple(d[c] for c in dtl_cols) for d in dtl)
            bills += 1
            lines += len(sale_lines)
            units += sum(s.qty for s in sale_lines)
            if interval:
                time.sleep(interval)

        if hdr_cols:
            conn.executemany(
                f"INSERT INTO POS_SALES_HDR ({','.join(hdr_cols)}) "
                f"VALUES ({','.join(['?'] * len(hdr_cols))})", hdr_rows)
            conn.executemany(
                f"INSERT INTO POS_SALES_DTL ({','.join(dtl_cols)}) "
                f"VALUES ({','.join(['?'] * len(dtl_cols))})", dtl_rows)
            conn.executemany(
                "UPDATE STOCK_MASTER SET SM_QTY=?, SM_LAST_ISSUE_DT=? "
                "WHERE SM_ORG_CD=? AND SM_ITM_CD=?",
                [(stock[i], today, org, i) for i in touched])
            conn.commit()
        print(f"  done: bills={bills} lines={lines} units={units:.0f}")
        return {"org": org, "bills": bills, "lines": lines, "units": round(units, 0),
                "active_skus": len(meta)}
    finally:
        conn.close()
