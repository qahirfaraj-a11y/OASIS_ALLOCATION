"""Seed a 14-store network from the Rhapta snapshot WITH REAL VARIATION AND AN ANSWER KEY.

DEV TOOLING — lives in devkit/, never ships to a client.

WHY THE EXISTING SEED CANNOT TEST TRANSFERS
-------------------------------------------
The current 14-store set is one Rhapta snapshot transformed by a scalar per
store. If store B is store A x 0.7 then there is no STRUCTURAL reason for A to
hold what B lacks — so either every store has excess or every store is short.
Measured on the existing set: 1,443,876 excess units against 75,039 deficit
units, 19:1, and every single store classified as a donor. Transfers need
COMPLEMENTARITY, and a scalar transform cannot produce it.

WHAT THIS DOES DIFFERENTLY
--------------------------
1. ASSORTMENT CARVE-OUT — each store ranges a SUBSET of the catalogue, sized by
   tier and biased toward the head. A store that does not range a SKU cannot
   donate it; one that ranges it and holds none is a genuine recipient.
2. DEPARTMENT INDEXING — every store draws its own per-department demand
   multiplier once, so the demand MIX diverges rather than just the scale.
3. INDEPENDENT STOCK DRAWS — cover days are drawn per store per SKU rather than
   scaled from a common parent. Independent draws are what put one store at 40
   days and another at 2 on the same SKU.
4. INJECTED IMBALANCES WITH AN ANSWER KEY — a known set of donor/recipient pairs
   is planted deliberately and written to JSON.

THE ANSWER KEY IS THE POINT
---------------------------
Without it a run reporting "755 moves, KES 1.6M" cannot be judged: it might be
excellent or mostly noise, and the seven single-unit air fryers suggest some of
it is noise. With a key, the transfer engine can be scored on precision and
recall against transfers that are known to be correct.

ADS IS NOT A FREE PARAMETER. The adapter derives it as
    avg_daily = total_qty / (months_active * 30)
so the sales rows written here are scaled by the number of DISTINCT calendar
months they land in. Get that wrong and every velocity in the rig is wrong by
a constant factor, which would quietly invalidate every threshold the engine
applies.
"""

from __future__ import annotations

import argparse
import json
import os
import random
import sqlite3
from collections import defaultdict
from datetime import date, datetime, timedelta

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
DEFAULT_SRC = os.path.join(ROOT, "oasis", "data", "mock_pos_erp.db")
DEFAULT_OUT = os.path.join(ROOT, "oasis", "data", "variant_network.db")
DEFAULT_KEY = os.path.join(ROOT, "oasis", "data", "variant_network_answers.json")

#: REAL store identities, read from store_coords.json.
#:
#: The first cut of this seeder invented ORG001..ORG014 with hand-written
#: names. Those codes then collided with the coordinate file, which is keyed
#: '001','002','016' — so ORG001 ("Chandarana Yaya Centre" in the seed) resolved
#: to coord '001' = HEAD OFFICE, which carries is_warehouse_hub. The seeded
#: store silently inherited a 3x donor boost it was never meant to have and
#: became the network's top donor. Deriving identity from the coordinate file
#: means code, name and position agree by construction and that class of
#: artifact cannot recur.
#:
#: Warehouse hubs are EXCLUDED. Baba Dogo is a real warehouse but stocks
#: differently from a retail branch, and seeded retail stock cannot represent
#: it — modelling it as just another store would be worse than leaving it out.
REGISTRY_PATH = os.path.join(ROOT, "oasis", "data", "store_registry.json")

#: 3 flagship, 6 standard, 5 compact — assigned by position, not measured.
TIER_PLAN = ([("FLAGSHIP", 0.94, 1.30), ("FLAGSHIP", 0.90, 1.15),
              ("FLAGSHIP", 0.88, 1.05)]
             + [("STANDARD", 0.74 - i * 0.03, 0.85 - i * 0.05) for i in range(6)]
             + [("COMPACT", 0.44 - i * 0.04, 0.42 - i * 0.045) for i in range(5)])


def _load_stores(n: int = 14):
    """Pick n real branches from the CANONICAL registry.

    The registry is used rather than store_coords.json directly because that
    file holds 44 entries for 29 sites — fifteen shops appear twice under two
    code schemes. Seeding from it produced pairs like "Mobil Branch" and
    "Chandarana Mobil Plaza Muthaiga" as separate stores at identical
    coordinates, which is a transfer network containing shops that are actually
    the same shop.

    Warehouse hubs are excluded: Baba Dogo stocks differently from a retail
    branch and seeded retail stock cannot represent it.

    One deliberately distant branch is kept — with every store inside 15km the
    distance term barely varies and distance-aware ranking cannot be seen to
    work at all.
    """
    import math
    with open(REGISTRY_PATH, "r", encoding="utf-8") as fh:
        reg = json.load(fh)
    sites = [s for s in reg["sites"] if not s.get("is_warehouse_hub")]
    lat0 = sum(s["lat"] for s in sites) / len(sites)
    lon0 = sum(s["lon"] for s in sites) / len(sites)

    def km(s):
        return math.hypot((s["lat"] - lat0) * 111.0,
                          (s["lon"] - lon0) * 111.0 * math.cos(math.radians(lat0)))

    sites.sort(key=km)
    chosen = sites[:n - 1] + [sites[-1]]
    return [(f"ORG{s['org_cd'].zfill(3)}" if s["org_cd"].isdigit()
             else f"ORG{s['org_cd']}", s["name"], tier, rs, sc)
            for s, (tier, rs, sc) in zip(chosen, TIER_PLAN)]


STORES = _load_stores()

SCHEMA = """
CREATE TABLE ORGANIZATION_MST (ORG_CD TEXT PRIMARY KEY, ORG_NAME TEXT,
  ORG_SHORT_NAME TEXT, ORG_ADDRESS TEXT, ORG_CITY TEXT, ORG_STATE TEXT,
  ORG_COUNTRY TEXT, ORG_PIN TEXT, ORG_PHONE TEXT, ORG_EMAIL TEXT,
  CURRENCY_CD TEXT, GST_NO TEXT, LEVEL_NUMBER INTEGER, PARENT_ORG_CD TEXT,
  ACTIVE_FLAG TEXT);
CREATE TABLE ITEM_MST (ITM_CD TEXT PRIMARY KEY, ITM_LONG_NAME TEXT,
  ITM_SHORT_NAME TEXT, SCAN_ITM_CD TEXT, HSN_CD TEXT, UOM_CD TEXT,
  UOM_DESC TEXT, ITM_GROUP_CD TEXT, ITM_TYPE TEXT, TAX_PLAN_CD TEXT,
  WEIGHT_FLAG TEXT, SERIAL_FLAG TEXT, PRODUCTION_FLAG TEXT, CATEGORY TEXT,
  DEPARTMENT TEXT, SUPPLIER_CD TEXT, ACTIVE_FLAG TEXT);
CREATE TABLE STOCK_MASTER (SM_ORG_CD TEXT, SM_ITM_CD TEXT, SM_LOC_CD TEXT,
  SM_QTY REAL, SM_WAC REAL, SM_LAST_RECV_DT TEXT, SM_LAST_ISSUE_DT TEXT);
CREATE TABLE BASIC_SP_MST (BSP_ORG_CD TEXT, BSP_ITEM_CD TEXT, BSP_SP REAL,
  BSP_MRP REAL, BSP_EFF_DATE TEXT);
CREATE TABLE BASIC_CP_MST (BCP_ORG_CD TEXT, BCP_ITEM_CD TEXT, BCP_CP REAL,
  BCP_EFF_DATE TEXT);
CREATE TABLE SUPPLIER_MST (SUPPLIER_CD TEXT PRIMARY KEY, SUPPLIER_NAME TEXT,
  CONTACT_PERSON TEXT, PHONE TEXT, EMAIL TEXT, ADDRESS TEXT,
  PAYMENT_TERMS TEXT, ORDER_FREQUENCY TEXT, LEAD_TIME_DAYS INTEGER,
  RELIABILITY_SCORE REAL, ACTIVE_FLAG TEXT);
CREATE TABLE POS_SALES_HDR (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
  CUST_CD TEXT, COUNTER_CD TEXT, LEVEL_NUMBER INTEGER, TOTAL_QTY REAL,
  TOTAL_AMT REAL, NET_AMT REAL, TAX_AMT REAL, DISC_AMT REAL,
  PAYMENT_MODE TEXT, VOID_FLAG TEXT, CUS_REF_CODE TEXT, CUS_REF_REMARKS TEXT);
CREATE TABLE POS_SALES_DTL (ORG_CD TEXT, BILL_NO TEXT, BILL_DT TEXT,
  SERIAL_NO INTEGER, ITM_CD TEXT, ITEM_NAME TEXT, QTY REAL, SELL_PRICE REAL,
  NET_AMT REAL, TAX_AMT REAL, DISC_AMT REAL, NET_TAX_AMT REAL,
  TOTAL_VALUE REAL, UOM_CD TEXT, UOM_DESC TEXT, VOID_FLAG TEXT,
  PROMO_ITEM_FLAG TEXT, SCAN_ITM_CD TEXT, TAX_PLAN_CD TEXT);
CREATE TABLE GRN_HDR (GRN_NO TEXT, ORG_CD TEXT, GRN_DT TEXT, SUPPLIER_CD TEXT);
CREATE TABLE INTEGRATION_PURCHASE_ORDERS (TENANT_ID TEXT DEFAULT 'default_tenant',
  PO_ID INTEGER PRIMARY KEY AUTOINCREMENT, ORG_CD TEXT, ITM_CD TEXT,
  PRODUCT_NAME TEXT, SUPPLIER_CD TEXT, QUANTITY REAL, UNIT_COST REAL,
  TOTAL_COST REAL, REASONING TEXT, STATUS TEXT, CREATED_DT TEXT,
  APPROVED_DT TEXT, APPROVED_BY TEXT);
CREATE TABLE INTEGRATION_TRANSFER_ORDERS (TENANT_ID TEXT DEFAULT 'default_tenant',
  TRANSFER_ID INTEGER PRIMARY KEY AUTOINCREMENT, FROM_ORG_CD TEXT,
  TO_ORG_CD TEXT, ITM_CD TEXT, PRODUCT_NAME TEXT, QUANTITY REAL,
  VALUE_KES REAL, STATUS TEXT, URGENCY TEXT, REQUESTED_BY TEXT,
  CREATED_DT TEXT, COMPLETED_DT TEXT);
CREATE INDEX ix_stock ON STOCK_MASTER(SM_ORG_CD, SM_ITM_CD);
CREATE INDEX ix_sp ON BASIC_SP_MST(BSP_ORG_CD, BSP_ITEM_CD);
CREATE INDEX ix_cp ON BASIC_CP_MST(BCP_ORG_CD, BCP_ITEM_CD);
CREATE INDEX ix_dtl ON POS_SALES_DTL(ORG_CD, ITM_CD, BILL_DT);
"""


def build(src: str, out: str, keypath: str, n_skus: int, window_days: int,
          n_plants: int, seed: int) -> None:
    rng = random.Random(seed)
    if os.path.exists(out):
        os.remove(out)

    s = sqlite3.connect(src)
    items = s.execute("""
        SELECT i.ITM_CD, i.ITM_LONG_NAME, i.UOM_CD, i.CATEGORY, i.DEPARTMENT,
               i.SUPPLIER_CD, AVG(sp.BSP_SP), AVG(cp.BCP_CP)
        FROM ITEM_MST i
        LEFT JOIN BASIC_SP_MST sp ON sp.BSP_ITEM_CD = i.ITM_CD
        LEFT JOIN BASIC_CP_MST cp ON cp.BCP_ITEM_CD = i.ITM_CD
        GROUP BY i.ITM_CD HAVING AVG(sp.BSP_SP) > 0
        LIMIT ?""", (n_skus,)).fetchall()
    suppliers = s.execute("SELECT * FROM SUPPLIER_MST").fetchall()
    s.close()
    print(f"catalogue: {len(items)} SKUs, {len(suppliers)} suppliers")

    depts = sorted({(r[4] or "GENERAL") for r in items})
    print(f"departments: {len(depts)}")

    # ── the calendar months the sales will land in ───────────────────────
    today = date.today()
    days = [today - timedelta(days=d) for d in range(window_days)]
    months = sorted({(d.year, d.month) for d in days})
    m_active = len(months)
    print(f"window: {window_days}d over {m_active} calendar month(s) — "
          f"ADS = total_qty / ({m_active} x 30)")

    # ── base popularity: a head-and-tail curve over the catalogue ────────
    # rank 0 is the fastest mover. Base ADS decays so most SKUs are slow,
    # which is what a real supermarket looks like.
    base_ads = {}
    for rank, it in enumerate(items):
        p = rank / max(1, len(items))
        base = 40.0 * (1.0 - p) ** 6 + 0.05
        base_ads[it[0]] = base * rng.uniform(0.6, 1.5)

    o = sqlite3.connect(out)
    o.executescript(SCHEMA)
    o.executemany("INSERT INTO SUPPLIER_MST VALUES (%s)" % ",".join("?" * len(suppliers[0])),
                  suppliers)
    o.executemany("INSERT INTO ITEM_MST (ITM_CD,ITM_LONG_NAME,ITM_SHORT_NAME,"
                  "SCAN_ITM_CD,UOM_CD,UOM_DESC,CATEGORY,DEPARTMENT,SUPPLIER_CD,"
                  "ACTIVE_FLAG) VALUES (?,?,?,?,?,?,?,?,?,'Y')",
                  [(r[0], r[1], (r[1] or "")[:20], r[0], r[2] or "EA",
                    r[2] or "EACH", r[3], r[4], r[5]) for r in items])
    o.executemany("INSERT INTO ORGANIZATION_MST (ORG_CD,ORG_NAME,ORG_SHORT_NAME,"
                  "ORG_CITY,ORG_COUNTRY,CURRENCY_CD,LEVEL_NUMBER,ACTIVE_FLAG) "
                  "VALUES (?,?,?,'Nairobi','KE','KES',2,'Y')",
                  [(c, n, n[:18]) for c, n, _, _, _ in STORES])

    # ── per-store divergence ────────────────────────────────────────────
    profiles = {}
    for org, name, tier, range_share, scale in STORES:
        # each store draws its OWN department mix, once
        idx = {d: max(0.15, rng.lognormvariate(0.0, 0.42)) for d in depts}
        profiles[org] = {"name": name, "tier": tier, "range": range_share,
                         "scale": scale, "dept_index": idx}

    carried: dict = defaultdict(dict)     # org -> itm -> (ads, stock)
    for org, name, tier, range_share, scale in STORES:
        prof = profiles[org]
        for rank, it in enumerate(items):
            itm, dept = it[0], (it[4] or "GENERAL")
            # head of the range is carried by everyone; the tail only by the
            # bigger stores. A compact store simply does not range the tail.
            head = 1.0 - (rank / max(1, len(items)))
            keep_p = min(1.0, 0.15 + 1.35 * range_share * head ** 0.45)
            if rng.random() > keep_p:
                continue
            ads = base_ads[itm] * scale * prof["dept_index"][dept] \
                * rng.uniform(0.55, 1.6)
            if ads < 0.01:
                ads = 0.0
            # INDEPENDENT cover draw — the source of donor/recipient pairs
            cover = rng.lognormvariate(2.95, 0.85) if ads > 0 else rng.uniform(0, 40)
            stock = round(ads * cover, 1) if ads > 0 else float(rng.randint(0, 12))
            carried[org][itm] = [ads, stock]

    for org in carried:
        print(f"  {org} ranges {len(carried[org]):>6} of {len(items)} SKUs")

    # ── injected imbalances: the answer key ─────────────────────────────
    by_item = defaultdict(list)
    for org, m in carried.items():
        for itm, (ads, _) in m.items():
            if ads > 0.3:
                by_item[itm].append(org)
    plantable = [i for i, orgs in by_item.items() if len(orgs) >= 4]
    rng.shuffle(plantable)
    meta = {r[0]: r for r in items}

    answers = []
    for itm in plantable[:n_plants]:
        orgs = by_item[itm][:]
        rng.shuffle(orgs)
        donor, recip = orgs[0], orgs[1]
        d_ads = carried[donor][itm][0]
        r_ads = carried[recip][itm][0]
        # donor: deliberately deep cover, well past the 30-day overstock gate
        d_cover = rng.uniform(75, 150)
        carried[donor][itm][1] = round(d_ads * d_cover, 1)
        # recipient: emptied, with live demand — a real, findable deficit
        carried[recip][itm][1] = 0.0
        row = meta[itm]
        answers.append({
            "itm_cd": itm,
            "product_name": row[1],
            "department": row[4],
            "donor_org": donor,
            "recipient_org": recip,
            "donor_stock": carried[donor][itm][1],
            "donor_ads": round(d_ads, 3),
            "donor_cover_days": round(d_cover, 1),
            "recipient_ads": round(r_ads, 3),
            "recipient_stock": 0.0,
            # what the engine SHOULD want: 7 days of cover at the recipient
            "recipient_deficit_units": round(r_ads * 7.0, 1),
            # what the donor may legally release: excess above the 14d floor,
            # capped at the PULL rule's half
            "donor_excess_units": round(max(0.0, carried[donor][itm][1] - d_ads * 14.0), 1),
        })
    print(f"planted {len(answers)} known donor/recipient pairs")

    # ── write stock, prices ─────────────────────────────────────────────
    stock_rows, sp_rows, cp_rows = [], [], []
    for org, m in carried.items():
        for itm, (ads, stock) in m.items():
            row = meta[itm]
            sell = float(row[6] or 0)
            cost = float(row[7] or sell * 0.75)
            recv = (today - timedelta(days=rng.randint(1, 90))).isoformat()
            stock_rows.append((org, itm, "MAIN", stock, cost, recv, None))
            sp_rows.append((org, itm, sell, round(sell * 1.05, 2), today.isoformat()))
            cp_rows.append((org, itm, cost, today.isoformat()))
    o.executemany("INSERT INTO STOCK_MASTER VALUES (?,?,?,?,?,?,?)", stock_rows)
    o.executemany("INSERT INTO BASIC_SP_MST VALUES (?,?,?,?,?)", sp_rows)
    o.executemany("INSERT INTO BASIC_CP_MST VALUES (?,?,?,?)", cp_rows)
    print(f"stock rows: {len(stock_rows):,}")

    # ── sales history that RECONSTRUCTS the intended ADS ────────────────
    # total_qty must equal ads * m_active * 30, because the adapter divides by
    # months_active * 30 rather than by the window length.
    hdr, dtl = [], []
    bill_no = 0
    for org, m in carried.items():
        movers = [(i, a) for i, (a, _) in m.items() if a > 0]
        # spread lines over ~1 emission per 5 days so each month is populated
        emit_days = [d for k, d in enumerate(sorted(days)) if k % 5 == 0]
        if not emit_days:
            emit_days = [today]
        per_day = {d: [] for d in emit_days}
        for itm, ads in movers:
            total = ads * m_active * 30.0
            share = total / len(emit_days)
            for d in emit_days:
                q = share * rng.uniform(0.55, 1.45)
                if q > 0.01:
                    per_day[d].append((itm, round(q, 2)))
        for d, lines in per_day.items():
            for k in range(0, len(lines), 40):
                chunk = lines[k:k + 40]
                bill_no += 1
                bn = f"B{bill_no:09d}"
                amt = 0.0
                for sn, (itm, q) in enumerate(chunk, 1):
                    row = meta[itm]
                    price = float(row[6] or 0)
                    val = round(q * price, 2)
                    amt += val
                    # VOID_FLAG is 'F' (false), NOT 'N'. The adapter filters
                    # WHERE d.VOID_FLAG = 'F', so seeding 'N' produces a
                    # database full of sales that the engine cannot see and
                    # every ADS comes back zero — the same map-the-VALUE-not-
                    # just-the-column trap as RXL's ACTIVE_FLAG='O' vs 'Y'.
                    dtl.append((org, bn, d.isoformat(), sn, itm, row[1], q, price,
                                val, 0.0, 0.0, 0.0, val, row[2] or "EA",
                                row[2] or "EACH", "F", "N", itm, None))
                hdr.append((org, bn, d.isoformat(), None, "C1", 2,
                            sum(q for _, q in chunk), amt, amt, 0.0, 0.0,
                            "CASH", "F", None, None))
    o.executemany("INSERT INTO POS_SALES_HDR VALUES (%s)" % ",".join("?" * 15), hdr)
    o.executemany("INSERT INTO POS_SALES_DTL VALUES (%s)" % ",".join("?" * 19), dtl)
    print(f"sales: {len(hdr):,} bills / {len(dtl):,} lines")

    o.commit()
    o.close()

    with open(keypath, "w", encoding="utf-8") as f:
        json.dump({
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "source_snapshot": os.path.basename(src),
            "seed": seed,
            "window_days": window_days,
            "months_active": m_active,
            "catalogue_skus": len(items),
            "stores": [{"org_cd": c, "name": n, "tier": t, "range_share": r,
                        "demand_scale": s} for c, n, t, r, s in STORES],
            "planted_transfers": answers,
            "scoring": {
                "recall": "planted pairs the engine found, donor and recipient "
                          "both matching, over the number planted",
                "precision": "engine moves that correspond to a planted pair "
                             "over all engine moves — treat unplanted moves as "
                             "UNSCORED rather than wrong: the network has real "
                             "imbalance beyond what was planted",
                "sizing": "compare engine qty against recipient_deficit_units; "
                          "the PULL rule caps a single donor at half its excess",
            },
        }, f, indent=2)
    print(f"\nwrote {out}")
    print(f"wrote {keypath}")


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--src", default=DEFAULT_SRC)
    p.add_argument("--out", default=DEFAULT_OUT)
    p.add_argument("--key", default=DEFAULT_KEY)
    p.add_argument("--skus", type=int, default=6000,
                   help="catalogue size (default 6000 keeps a run ~1 min)")
    p.add_argument("--days", type=int, default=60)
    p.add_argument("--plants", type=int, default=150,
                   help="known donor/recipient pairs to inject")
    p.add_argument("--seed", type=int, default=20260819)
    a = p.parse_args()
    build(a.src, a.out, a.key, a.skus, a.days, a.plants, a.seed)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
