"""Prove the seeded 14-outlet network is real, THROUGH THE ADAPTER.

Querying Odoo directly would only prove the seeder wrote what the seeder wrote.
Everything here goes through ``OdooAdapter`` over XML-RPC -- the same path OASIS
uses against a client ERP -- and then runs the actual transfer engine on the
result. A depot that "looks seeded" but reads back wrong through the adapter is
the failure mode worth catching.

Checks, in order of what would hurt most if wrong:

  1. organisations   all 14 outlets visible as OASIS orgs
  2. SITE SCOPING    each store reads its OWN stock and demand. This is the
                     check with teeth: fetch_enriched_products once accepted
                     org_cd and ignored it, so every store saw the whole
                     company's stock and under-ordered with no error. Identical
                     readings across stores is that bug, not a coincidence.
  3. fidelity        stock and ADS read back match the seed profile
  4. diagnose        the adapter's own warnings are clear
  5. transfers       can_transfer() between outlets is legal (one company)
  6. ENGINE          ConsolidatedTransferService actually finds opportunities on
                     Odoo-sourced data -- the reason the network was seeded

Usage:
    python verify_store_network.py
    python verify_store_network.py --stores 4      # quicker pass
"""

from __future__ import annotations

import argparse
import json
import os
import statistics
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, REPO)

from oasis.logic.odoo_adapter import OdooAdapter          # noqa: E402

SEED = os.path.join(HERE, "store_network_seed.json")
PASS, FAIL, WARN = "PASS", "FAIL", "WARN"
results = []


def fetch_with_retry(a, code, tries=4):
    """Read one site, surviving a server blip.

    A full pass is 14 heavy reads over XML-RPC and Odoo may restart or recycle a
    worker under that load -- which drops the socket mid-call. Without a retry a
    transient restart looks identical to a broken depot, and the whole
    verification is lost on the 8th store.
    """
    import time as _t
    for n in range(tries):
        try:
            return a.fetch_enriched_products(code)
        except Exception as e:
            if n == tries - 1:
                raise
            wait = 2 ** n
            print(f"     ... {code}: {type(e).__name__}, retrying in {wait}s "
                  f"({n + 1}/{tries - 1})")
            a._uid = None                 # force a fresh authenticate
            _t.sleep(wait)


def check(label, ok, detail=""):
    tag = PASS if ok is True else (WARN if ok == WARN else FAIL)
    results.append(tag)
    print(f"  [{tag}] {label}" + (f" -- {detail}" if detail else ""))
    return ok is True


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--odoo", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    p.add_argument("--db", default=os.getenv("ODOO_DB", "oasis"))
    p.add_argument("--user", default=os.getenv("ODOO_USER", "admin"))
    p.add_argument("--password", default=os.getenv("ODOO_PASSWORD", "admin"))
    p.add_argument("--stores", type=int, default=0)
    args = p.parse_args(argv)

    seed = json.load(open(SEED, encoding="utf-8"))
    stores = seed["stores"][:args.stores] if args.stores > 0 else seed["stores"]
    codes = [s["code"] for s in stores]
    a = OdooAdapter(url=args.odoo, db=args.db, user=args.user, password=args.password)

    # Fail loudly on a dead Odoo instead of quietly reporting an empty depot.
    # OdooAdapter catches connection errors and returns [] by design, so a
    # stopped container is indistinguishable from a wiped database from the
    # results alone: every store reads 0 SKUs and six checks fail, which
    # invites a hunt for a data bug that does not exist.
    print("=" * 74)
    health = a.health_check()
    if not health.get("connected"):
        print("ODOO IS NOT REACHABLE -- verification cannot run.")
        print(f"  {args.odoo} db={args.db}")
        print(f"  {health.get('error', 'no detail')}")
        print("\nThis is an environment failure, NOT a depot or engine failure.")
        print("Start the stack, then re-run:")
        print("  docker start oasis-odoo-odoo-db-1 oasis-odoo-odoo-1")
        return 2
    print(f"connected: {health['tables_found']:,} products visible, "
          f"{health['latency_ms']}ms")

    print("1. ORGANISATIONS")
    orgs = a.fetch_all_organizations()
    seen = {o["ORG_CD"] for o in orgs}
    missing = [c for c in codes if c not in seen]
    check(f"all {len(codes)} outlets visible as OASIS orgs",
          not missing, f"missing: {missing}" if missing else
          f"{len(seen)} orgs total in Odoo")

    print("\n2. SITE SCOPING -- each store must read its OWN position")
    catalogue = {}
    per_store = {}
    for st in stores:
        prods = fetch_with_retry(a, st["code"])
        per_store[st["code"]] = prods
        stock = sum(float(p["current_stocks"]) for p in prods)
        ads = sum(float(p["avg_daily_sales"]) for p in prods)
        withstock = sum(1 for p in prods if float(p["current_stocks"]) > 0)
        catalogue[st["code"]] = (len(prods), stock, ads, withstock)
        print(f"     {st['code']}  {st['name'][:28]:<28} "
              f"{len(prods):>5,} SKUs  stock {stock:>12,.0f}  "
              f"ADS {ads:>9,.1f}  with-stock {withstock:>5,}")

    stock_vals = [v[1] for v in catalogue.values()]
    ads_vals = [v[2] for v in catalogue.values()]
    check("stores report DIFFERENT total stock (site scoping is live)",
          len(set(round(v) for v in stock_vals)) == len(stock_vals),
          f"{len(set(round(v) for v in stock_vals))} distinct of {len(stock_vals)}")
    check("stores report DIFFERENT total demand",
          len(set(round(v, 1) for v in ads_vals)) == len(ads_vals),
          f"spread {min(ads_vals):,.0f} ... {max(ads_vals):,.0f}")

    # the spread should track demand_scale_factor, not be arbitrary noise
    ranked = sorted(stores, key=lambda s: -catalogue[s["code"]][2])
    by_dsf = sorted(stores, key=lambda s: -s["demand_scale_factor"])
    top_match = ranked[0]["code"] == by_dsf[0]["code"]
    check("busiest store by ADS is the one with the highest demand_scale_factor",
          top_match, f"ADS leader {ranked[0]['code']} "
                     f"({ranked[0]['name'][:24]}), dsf leader {by_dsf[0]['code']}")

    print("\n3. FIDELITY -- adapter readings vs the seed profile")
    st = stores[0]
    want = {r["sku"][:64]: r for r in st["stock_profile"]}
    got = {p["item_code"]: p for p in per_store[st["code"]]}
    matched = [(want[k], got[k]) for k in want if k in got]
    check(f"{st['code']}: seeded SKUs found through the adapter",
          len(matched) >= len(want) * 0.99,
          f"{len(matched):,} of {len(want):,}")
    if matched:
        # On-hand is the PLAN minus what sold since each store's last delivery,
        # so it must be checked against the depletion model, not against the raw
        # profile. The invariant that matters is internal consistency:
        #     on_hand == plan_qty - ADS x days_since_delivery
        # If those disagree, days_since_delivery is decoration and the
        # dead-stock guard is reading a number that means nothing.
        stock_err, ads_err, model_err = [], [], []
        for w, g in matched:
            ads, plan = w["ads"], w["qty"]
            age = int(g["days_since_delivery"])
            expect = float(int(round(max(0.0, plan - ads * age)))) if ads > 0 else plan
            stock_err.append(abs(float(g["current_stocks"]) - expect))
            model_err.append(abs(float(g["current_stocks"])
                                 - max(0.0, plan - ads * age)))
            if ads > 0:
                ads_err.append(abs(float(g["avg_daily_sales"]) - ads))
        # ASCII deliberately: this prints to a Windows console whose cp1252
        # codec cannot encode U+2212 or U+00D7, same trap odoo_adapter documents
        check("on-hand == plan - ADS x days_since_delivery (depot is self-consistent)",
              max(stock_err) < 0.5,
              f"worst error {max(stock_err):.3f} units, "
              f"worst pre-rounding {max(model_err):.3f}")
        check("ADS reproduces the profile", max(ads_err) < 0.05 if ads_err else False,
              f"worst error {max(ads_err):.4f}/day over {len(ads_err):,} SKUs")
        costed = sum(1 for w, g in matched if float(g["cost_price"]) > 0)
        check("cost price present (budget gating and risk_kes need it)",
              costed >= len(matched) * 0.99, f"{costed:,} of {len(matched):,}")
        dates = [int(g["days_since_delivery"]) for w, g in matched]
        check("days_since_delivery is real, not uniformly zero",
              len(set(dates)) > 1 and min(dates) > 0,
              f"{len(set(dates))} distinct ages, {min(dates)}-{max(dates)} days")

        # THE check that the depletion model is doing its job. An age that
        # depended only on the SKU would deplete every store by the same number
        # of days and leave them exactly as equal as the opening plan -- which is
        # the state in which the engine found nothing at all.
        ages_by_sku = {}
        for code, prods in per_store.items():
            for p in prods:
                if p["item_code"] in want:
                    ages_by_sku.setdefault(p["item_code"], set()).add(
                        int(p["days_since_delivery"]))
        varies = sum(1 for v in ages_by_sku.values() if len(v) > 1)
        check("the SAME SKU sits at different cycle positions across stores",
              varies > len(ages_by_sku) * 0.9,
              f"{varies:,} of {len(ages_by_sku):,} SKUs differ store to store")
        depts = {g["department"] for w, g in matched}
        check("department hierarchy read back", len(depts) > 50,
              f"{len(depts)} departments")

    print("\n4. ADAPTER DIAGNOSE")
    d = a.diagnose(stores[0]["code"])
    for w in d.get("warnings", []):
        print(f"     ! {w}")
    check(f"{stores[0]['code']} diagnose reports no warnings",
          not d.get("warnings"), f"{len(d.get('warnings', []))} warning(s)")

    print("\n5. TRANSFER LEGALITY")
    v = a.can_transfer(codes[0], codes[1])
    check(f"{codes[0]} -> {codes[1]} is a legal internal transfer",
          v["ok"], v.get("reason", ""))

    print("\n6. TRANSFER ENGINE on Odoo-sourced data")
    from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService
    coords = {}
    for s in stores:
        if s.get("latitude") is not None:
            coords[s["code"]] = {"lat": s["latitude"], "lon": s["longitude"]}
    org_names = {s["code"]: s["name"] for s in stores}
    # data_dir so the service picks up LATA's measured supplier rhythm and
    # AMIT's dead-stock thresholds. Without it the engine falls back to fixed
    # horizons and this check would be verifying the wrong code path.
    cts = ConsolidatedTransferService(org_names=org_names, stock_data=per_store,
                                      distance_map=coords,
                                      data_dir=os.path.join(REPO, "oasis", "data"))
    check("LATA supplier rhythm loaded (horizons are derived, not fixed)",
          bool(cts.supplier_rhythm),
          f"{len(cts.supplier_rhythm):,} suppliers, median relief "
          f"{cts._median_relief or 0:.1f}d")
    check("AMIT dead-stock thresholds loaded",
          bool(cts._perishability),
          f"{len(cts._perishability)} category tiers, default "
          f"{cts._dead_days_default:.0f}d")
    scan = cts.scan_network_opportunities()
    opps = scan.opportunities
    pull = [o for o in opps if o.type == "PULL"]
    push = [o for o in opps if o.type == "PUSH"]
    check("engine finds transfer opportunities on Odoo data", bool(opps),
          f"{len(opps):,} ({len(pull):,} pull / {len(push):,} push)")
    if not opps:
        # Not a seeding defect, and worth saying so in the output rather than
        # leaving whoever runs this to re-derive it: stores_network.json is an
        # opening-stock PLAN, where every store's qty and ads are the same
        # Rhapta snapshot scaled by the same demand_scale_factor. Cover is
        # qty/ads, so the scale factor CANCELS and every store holds nearly
        # identical days of cover for a given SKU. The engine moves stock
        # between stores that differ; this profile has no store that differs.
        print("     WHY: cover = qty/ads and both scale with demand_scale_factor,")
        print("          so the factor cancels -- every store holds ~the same days")
        print("          of cover per SKU. Minimum cover anywhere is 9.3d, so no")
        print("          store is ever a deficit (<7d) and no SKU is cold in one")
        print("          store while hot in another by more than a fraction of a")
        print("          unit. The depot is faithful; the PROFILE cannot express")
        print("          imbalance. Needs a trading position, not an opening plan.")
    if opps:
        donors = {o.from_org for o in opps}
        recips = {o.to_org for o in opps}
        check("more than one donor and recipient participate",
              len(donors) > 1 and len(recips) > 1,
              f"{len(donors)} donors, {len(recips)} recipients")
        check("no self-transfers", not any(o.from_org == o.to_org for o in opps))
        units = sum(o.transfer_qty for o in opps)
        val = sum(o.value_kes for o in opps)
        print(f"     {units:,.0f} units, KES {val:,.0f}, "
              f"median line {statistics.median(o.transfer_qty for o in opps):.0f} units")
        fresh = sum(1 for o in opps if o.manual_only)
        print(f"     {fresh:,} fresh lines flagged manual_only "
              f"({fresh / len(opps) * 100:.1f}%)")

    print("\n" + "=" * 74)
    n_fail = results.count(FAIL)
    print(f"{results.count(PASS)} passed, {results.count(WARN)} warned, {n_fail} failed")
    return 1 if n_fail else 0


if __name__ == "__main__":
    sys.exit(main())
