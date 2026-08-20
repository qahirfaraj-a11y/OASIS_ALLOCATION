"""Give each outlet its own assortment and its own demand mix.

THE PROBLEM THIS SOLVES
-----------------------
stores_network.json gives every store the same SKUs with qty and ads both scaled
by one demand_scale_factor. Days of cover is qty/ads, so the factor cancels and
every store holds the same cover -- a network with nothing to transfer. Depleting
over a replenishment cycle adds phase differences, but measured on the seeded
depot it still yields almost nothing worth moving:

    40,461 store-SKU states carry excess > 0
         0 of them carry TWO OR MORE UNITS of it

Because the only things overstocked in DAYS are slow movers holding one or two
physical units. A transfer moves units, so nothing is transferable. Real chains
transfer pallets of fast movers that landed in the wrong store.

THE MODEL
---------
Two forces, applied separately, because their MISMATCH is the whole point:

  demand   is LOCAL. A wine sells differently in Lavington than at a Muthaiga
           forecourt. ads(store, sku) = base x dsf x affinity(store, dept) x noise
           where affinity comes from the store's real attributes in the profile --
           catchment affluence, floor area, store category, cold chain.

  stock    is CENTRAL. Allocated on the NETWORK-AVERAGE velocity, not the local
           one: qty(store, sku) = ads_network(sku) x dsf x target_days. This is
           how chains actually buy, and it is precisely why stores end up holding
           the wrong things.

Cover then falls out as:

    cover = qty/ads_local = target_days / (affinity x noise)

so a SKU with affinity 0.25 at a store sits on ~56 days of cover, and one with
affinity 3.0 sits on ~5 days and is a deficit. Critically the UNITS scale with
the network-average velocity, so a fast mover stranded in a low-affinity store is
overstocked by HUNDREDS of units, not by one. That is a donor worth the lorry.

Assortment breadth also varies: a 5,000 sqft forecourt does not range what a
22,500 sqft anchor ranges, so small stores carry the head of the catalogue only.

Deterministic throughout (md5, never the salted builtin hash), so the network is
identical on every machine and the depot can be rebuilt exactly.

Usage:
    python differentiate_store_network.py                 # rewrite the seed
    python differentiate_store_network.py --report        # show, write nothing
    python differentiate_store_network.py --target-days 21
"""

from __future__ import annotations

import argparse
import hashlib
import json
import math
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = os.path.join(HERE, "store_network_seed.json")

#: Days of cover the central plan buys for. The engine's fill target is 14d and
#: its donor gate is 30d, so a plan of 14 leaves NO room to ever be a donor --
#: see the threshold-coherence section of devkit/analyse_transfer_funnel.py.
#: 18 is a realistic buy-ahead that still sits under the donor gate, leaving the
#: affinity mismatch (not the plan) to decide who ends up long.
DEFAULT_TARGET_DAYS = 18.0

#: Department archetypes, matched on substrings of the real department names.
#: Order matters -- first match wins.
ARCHETYPES = (
    ("premium", ("WINE", "WHISKY", "SPIRIT", "CHAMPAGNE", "COGNAC", "LIQUEUR",
                 "CHEESE", "DELI", "IMPORTED", "ORGANIC", "GOURMET", "SALMON",
                 "OLIVE", "COFFEE BEAN", "TEA SPECIAL", "CONTINENTAL")),
    ("baby", ("BABY", "INFANT", "DIAPER", "NAPPY", "FORMULA")),
    ("fresh", ("MILK", "DAIRY", "MEAT", "BUTCHERY", "BAKERY", "BREAD",
               "PRODUCE", "VEGETABLE", "FRUIT", "FISH", "SEAFOOD", "POULTRY")),
    ("convenience", ("SNACK", "SOFT DRINK", "SODA", "WATER", "JUICE", "CHOCOLATE",
                     "SWEET", "CIGARETTE", "TOBACCO", "ENERGY", "CRISP", "GUM",
                     "BISCUIT", "ICE CREAM")),
    ("bulk", ("FLOUR", "MAIZE", "RICE", "SUGAR", "COOKING FAT", "COOKING OIL",
              "DETERGENT", "SOAP", "PULSE", "CEREAL", "UNGA", "SALT")),
    ("household", ("HOUSEHOLD", "CLEAN", "TISSUE", "KITCHEN", "HARDWARE",
                   "STATIONERY", "GENERAL MERCH", "PLASTIC", "ELECTRIC")),
)

#: How much each archetype responds to each store trait. Positive means the
#: archetype sells BETTER as the trait rises. These are elasticities on a log
#: scale, so 0.6 roughly means "up to ~1.8x at the top of the range".
#:                    affluence  size   convenience(small+forecourt)
RESPONSE = {
    "premium":     (0.85, 0.35, -0.45),
    "baby":        (0.35, 0.20, -0.20),
    "fresh":       (0.20, 0.40, -0.30),
    "convenience": (-0.30, -0.35, 0.80),
    "bulk":        (-0.55, 0.15, -0.35),
    "household":   (0.00, 0.25, -0.20),
    "other":       (0.05, 0.10, 0.00),
}


def archetype(department: str) -> str:
    d = (department or "").upper()
    for name, keys in ARCHETYPES:
        if any(k in d for k in keys):
            return name
    return "other"


def h01(*parts) -> float:
    """Deterministic float in [0, 1) from the given parts."""
    raw = "|".join(str(p) for p in parts).encode("utf-8")
    return int(hashlib.md5(raw).hexdigest()[:8], 16) / 0xFFFFFFFF


def store_traits(stores):
    """Normalise each outlet's real attributes to -1..+1 trait scores."""
    areas = [float(s.get("floor_area_sqft") or 0) for s in stores]
    affl = [float(s.get("catchment_affluence_index") or 3.0) for s in stores]
    a_lo, a_hi = min(areas), max(areas)
    f_lo, f_hi = min(affl), max(affl)

    traits = {}
    for s in stores:
        area = float(s.get("floor_area_sqft") or 0)
        aff = float(s.get("catchment_affluence_index") or 3.0)
        size = 2 * ((area - a_lo) / (a_hi - a_lo) - 0.5) if a_hi > a_lo else 0.0
        wealth = 2 * ((aff - f_lo) / (f_hi - f_lo) - 0.5) if f_hi > f_lo else 0.0
        # a convenience outlet is small, and the profile names it
        cat = str(s.get("store_category") or "").upper()
        conv = -size
        if any(k in cat for k in ("FORECOURT", "EXPRESS", "CONVENIENCE", "KIOSK")):
            conv = 1.0
        traits[s["store_id"]] = {"size": size, "wealth": wealth, "conv": conv,
                                 "category": cat}
    return traits


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--seed", default=SEED)
    p.add_argument("--out", default=None, help="default: rewrite --seed in place")
    p.add_argument("--target-days", type=float, default=DEFAULT_TARGET_DAYS)
    p.add_argument("--min-assortment", type=float, default=0.45,
                   help="smallest store's share of the catalogue (default 0.45)")
    p.add_argument("--spread", type=float, default=1.0,
                   help="scales every affinity response; 0 = uniform network")
    p.add_argument("--dead-below", type=float, default=0.30,
                   help="affinity under this means the line does not sell at "
                        "that outlet: ADS 0 with stock still on hand, i.e. real "
                        "dead stock for the PUSH pass to clear (default 0.30)")
    p.add_argument("--report", action="store_true", help="analyse, write nothing")
    args = p.parse_args(argv)

    seed = json.load(open(args.seed, encoding="utf-8"))
    stores = seed["stores"]
    cat = {c["sku"]: c for c in seed["catalogue"]}
    traits = store_traits(stores)

    # network-average velocity per SKU, from the ORIGINAL profile, before any
    # store-specific mix is applied. This is what central buying sees.
    base_ads = {}
    for st in stores:
        dsf = float(st.get("demand_scale_factor") or 1.0) or 1.0
        for r in st["stock_profile"]:
            # de-scale each store's ads back to a network-neutral figure
            base_ads.setdefault(r["sku"], []).append(float(r["ads"]) / dsf)
    base_ads = {k: (sum(v) / len(v)) for k, v in base_ads.items()}

    # assortment: rank SKUs by network value; small stores carry the head only
    value = {}
    for sku, c in cat.items():
        value[sku] = base_ads.get(sku, 0.0) * float(c.get("price") or 0.0)
    ranked = sorted(cat, key=lambda s: (-value.get(s, 0.0), s))
    rank_of = {s: i for i, s in enumerate(ranked)}
    n_total = len(ranked)

    report_rows = []
    for st in stores:
        t = traits[st["store_id"]]
        dsf = float(st.get("demand_scale_factor") or 1.0)

        # breadth scales with floor area between min_assortment and 100%
        share = args.min_assortment + (1 - args.min_assortment) * ((t["size"] + 1) / 2)
        keep_n = max(50, int(round(n_total * share)))

        rows, arche_counts = [], {}
        for r in st["stock_profile"]:
            sku = r["sku"]
            if rank_of.get(sku, n_total) >= keep_n:
                continue                       # not ranged at this outlet
            c = cat.get(sku, {})
            arc = archetype(c.get("department", ""))
            w_aff, w_size, w_conv = RESPONSE.get(arc, RESPONSE["other"])
            # log-linear affinity from the store's real traits
            expo = (w_aff * t["wealth"] + w_size * t["size"] + w_conv * t["conv"])
            # per-SKU idiosyncrasy so stores differ WITHIN a department too --
            # without it every SKU in a department moves together and the
            # network still has only 258 distinct behaviours
            noise = 0.55 + 1.10 * h01(st["store_id"], sku)
            affinity = math.exp(expo * args.spread) * noise

            ads = base_ads.get(sku, 0.0) * dsf * affinity
            # STOCK IS CENTRAL: bought on network-average velocity, not local
            qty = base_ads.get(sku, 0.0) * dsf * args.target_days

            # DEAD STOCK. Below a floor of local appetite the line simply does
            # not sell at this outlet -- wrong demographic, a seasonal leftover,
            # a delist that never cleared -- while the stock bought centrally is
            # still sitting there. That is the entire PUSH case: capital frozen
            # at one node that would turn over at another. A network where every
            # SKU sells a little everywhere has no dead stock to eliminate, and
            # cannot exercise the clearance half of the engine at all.
            if affinity < args.dead_below:
                ads = 0.0
            rows.append({"sku": sku, "qty": round(qty, 2), "ads": round(ads, 4)})
            arche_counts[arc] = arche_counts.get(arc, 0) + 1

        st["stock_profile"] = rows
        st["assortment_share"] = round(share, 3)
        st["traits"] = {k: round(v, 3) for k, v in t.items() if k != "category"}
        covers = sorted(r["qty"] / r["ads"] for r in rows if r["ads"] > 0)
        report_rows.append((st, rows, covers, arche_counts))

    # ── report ────────────────────────────────────────────────────────────
    print(f"{'store':<7}{'name':<27}{'SKUs':>6}{'wealth':>8}{'size':>7}"
          f"{'medCov':>8}{'<7d':>7}{'>30d':>7}")
    tot_def = tot_don = tot = 0
    for st, rows, covers, arche in report_rows:
        if not covers:
            continue
        med = statistics.median(covers)
        deficit = sum(1 for c in covers if c < 7)
        donor = sum(1 for c in covers if c > 30)
        tot_def += deficit
        tot_don += donor
        tot += len(covers)
        print(f"{st['code']:<7}{st['name'][:26]:<27}{len(rows):>6,}"
              f"{st['traits']['wealth']:>8.2f}{st['traits']['size']:>7.2f}"
              f"{med:>8.1f}{deficit:>7,}{donor:>7,}")
    print(f"\nnetwork: {tot:,} store-SKU pairs, {tot_def:,} deficits "
          f"({100 * tot_def / tot:.1f}%), {tot_don:,} over 30d "
          f"({100 * tot_don / tot:.1f}%)")

    # the number that actually decides whether transfers exist: donors holding
    # a meaningful number of UNITS, not merely a lot of days
    fat = 0
    for st, rows, covers, arche in report_rows:
        for r in rows:
            if r["ads"] > 0 and r["qty"] / r["ads"] > 30:
                excess = r["qty"] - r["ads"] * 14
                if excess >= 10:
                    fat += 1
    print(f"donors holding 10+ spare UNITS: {fat:,}   "
          f"(was 0 across the whole uniform network)")

    # dead stock: the PUSH case. Frozen capital at a node that does not sell it,
    # while some OTHER node does -- which is what makes it worth moving rather
    # than marking down.
    dead = dead_units = dead_value = 0
    sells_elsewhere = 0
    active_by_sku = {}
    for st, rows, covers, arche in report_rows:
        for r in rows:
            if r["ads"] > 0:
                active_by_sku.setdefault(r["sku"], []).append(st["code"])
    for st, rows, covers, arche in report_rows:
        for r in rows:
            if r["ads"] <= 0 and r["qty"] > 0:
                dead += 1
                dead_units += r["qty"]
                dead_value += r["qty"] * float(cat.get(r["sku"], {}).get("cost") or 0)
                if active_by_sku.get(r["sku"]):
                    sells_elsewhere += 1
    print(f"DEAD stock lines (ADS 0, stock on hand): {dead:,}  "
          f"{dead_units:,.0f} units, KES {dead_value:,.0f} at cost")
    print(f"  of which the SKU is ACTIVE at another store: {sells_elsewhere:,}"
          f"  <- the PUSH opportunity")

    # cross-store: the same SKU long in one store and short in another
    cov_by_sku = {}
    for st, rows, covers, arche in report_rows:
        for r in rows:
            if r["ads"] > 0:
                cov_by_sku.setdefault(r["sku"], []).append(r["qty"] / r["ads"])
    both = sum(1 for v in cov_by_sku.values()
               if len(v) > 1 and max(v) > 30 and min(v) < 7)
    print(f"SKUs long in one store AND short in another: {both:,} of "
          f"{len(cov_by_sku):,}")

    if args.report:
        print("\n--report: nothing written.")
        return 0

    seed["differentiated"] = {
        "target_days": args.target_days, "spread": args.spread,
        "min_assortment": args.min_assortment,
        "model": "local demand via store-trait affinity; central stock on "
                 "network-average velocity",
    }
    out = args.out or args.seed
    with open(out, "w", encoding="utf-8") as f:
        json.dump(seed, f, indent=1)
    print(f"\n-> wrote {os.path.basename(out)} "
          f"({os.path.getsize(out) / 1e6:.1f} MB)")
    return 0


if __name__ == "__main__":
    sys.exit(main())
