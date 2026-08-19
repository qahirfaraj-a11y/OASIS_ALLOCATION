"""Score the transfer engine against the seeded answer key.

DEV TOOLING — devkit/, never ships.

"755 moves, KES 1.6M" is not a result. It could be excellent or mostly noise,
and the seven single-unit air fryers suggest some of it is noise. This scores
the engine against transfers that are KNOWN to be correct because they were
planted deliberately by devkit/seed_variant_network.py.

RECALL is the honest headline: of the imbalances we planted, how many did the
engine find? A planted pair is a store holding 75-150 days of cover for a SKU
another store has run to zero on, with live demand — if the engine misses that,
it will miss the real thing.

PRECISION is reported but qualified. An unplanted move is NOT necessarily
wrong: the seeded network has genuine imbalance beyond what was planted, so
those are counted as UNSCORED rather than false. What precision does catch is
the pathological shapes — self-transfers, and many donors shipping single units
to one recipient.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections import defaultdict

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, ROOT)

DEFAULT_DB = os.path.join(ROOT, "oasis", "data", "variant_network.db")
DEFAULT_KEY = os.path.join(ROOT, "oasis", "data", "variant_network_answers.json")


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--db", default=DEFAULT_DB)
    p.add_argument("--key", default=DEFAULT_KEY)
    a = p.parse_args()

    os.environ["OASIS_DB_PATH"] = a.db
    os.environ.pop("OASIS_POS_DB_URL", None)
    os.environ.pop("OASIS_ERP", None)
    import logging
    logging.basicConfig(level=logging.ERROR)

    import oasis.desktop.data as D

    key = json.load(open(a.key, encoding="utf-8"))
    planted = key["planted_transfers"]

    # THE KEY STATES GROUND TRUTH ABOUT THE DATA, NOT ABOUT POLICY.
    #
    # It records that a donor holds deep cover and a recipient is empty with
    # live demand. How much SHOULD move is a policy question, and policy has
    # since changed: the target is now min(T_standard, T_next + lead) rather
    # than a flat 7 days, and 65% of SKUs consequently target LESS than 7 days
    # because their supplier delivers sooner.
    #
    # Scoring a calendar-aware engine against a flat-7-day key marks correct
    # restraint as under-delivery. So expected need is recomputed here from the
    # live policy; the key stays untouched and keeps its meaning across policy
    # changes.
    try:
        from oasis.data.supplier_calendar import SupplierCalendar
        from oasis.logic.simulation_bridge import _find_calendar_path
        cal = SupplierCalendar(_find_calendar_path(os.path.join(ROOT, "oasis", "data")))
        cal.load()
    except Exception:
        cal = None

    import oasis.desktop.data as _D
    _ads = {}
    _sup = {}
    for _s in _D.list_stores():
        for _p in _D.get_adapter().fetch_enriched_products(_s["org_cd"]) or []:
            _ads[(_p.get("item_code"), _s["org_cd"])] = float(_p.get("avg_daily_sales") or 0)
            _sup[_p.get("item_code")] = (str(_p.get("supplier_name") or ""),
                                         float(_p.get("estimated_delivery_days") or 0))

    TARGET_STANDARD = 14.0
    for pl in planted:
        ads = _ads.get((pl["itm_cd"], pl["recipient_org"]), pl.get("recipient_ads", 0))
        nm, lead = _sup.get(pl["itm_cd"], ("", 0.0))
        t = TARGET_STANDARD
        if cal is not None and nm:
            d = cal.days_to_next_order(nm)
            if d is not None:
                t = min(TARGET_STANDARD, max(1.0, d + lead))
        pl["expected_units"] = round(ads * t, 2)
        pl["target_days"] = round(t, 1)
    print(f"answer key: {len(planted)} planted pairs "
          f"(seed {key['seed']}, {key['catalogue_skus']} SKUs)\n")

    _t = [p["target_days"] for p in planted if p.get("target_days")]
    if _t:
        _t.sort()
        print("expected need recomputed from live policy: target "
              "%.0f-%.0f days, median %.0f\n" % (_t[0], _t[-1], _t[len(_t)//2]))
    print("running network scan …")
    scan = D.network_transfer_scan()
    if scan.get("error"):
        print("scan error:", scan["error"])
        return 1
    ops = scan.get("opportunities") or []
    print(f"engine produced {len(ops)} moves\n")

    # ── index the engine's output ────────────────────────────────────────
    by_pair = defaultdict(list)            # (itm, from, to) -> moves
    by_item_to = defaultdict(list)         # (itm, to)       -> moves
    for o in ops:
        by_pair[(o["itm_cd"], o["from_org"], o["to_org"])].append(o)
        by_item_to[(o["itm_cd"], o["to_org"])].append(o)

    exact = partial = missed = 0
    sized_ok = 0
    rows = []
    for pl in planted:
        itm, donor, recip = pl["itm_cd"], pl["donor_org"], pl["recipient_org"]
        hits = by_pair.get((itm, donor, recip)) or []
        anyto = by_item_to.get((itm, recip)) or []
        if hits:
            exact += 1
            verdict = "EXACT"
            qty = sum(h["qty"] for h in hits)
        elif anyto:
            # right need, different donor — the engine found the deficit but
            # sourced it elsewhere. That is a correct outcome, not a miss.
            partial += 1
            verdict = "FOUND (other donor)"
            qty = sum(h["qty"] for h in anyto)
        else:
            missed += 1
            verdict = "MISSED"
            qty = 0.0
        want = pl.get("expected_units") or pl["recipient_deficit_units"]
        if qty > 0 and want > 0 and 0.4 * want <= qty <= 2.5 * want:
            sized_ok += 1
        rows.append((verdict, pl["product_name"][:30], donor, recip, want, qty))

    found = exact + partial
    n = len(planted)
    print("=" * 78)
    print("RECALL — did the engine find the imbalances we planted?")
    print("=" * 78)
    print(f"  exact pair (donor and recipient both matched) {exact:>5} / {n}"
          f"  {exact/n*100:>5.1f}%")
    print(f"  deficit found, sourced from another donor      {partial:>5} / {n}"
          f"  {partial/n*100:>5.1f}%")
    print(f"  MISSED entirely                                {missed:>5} / {n}"
          f"  {missed/n*100:>5.1f}%")
    print(f"  --> recall (found at all)                      {found:>5} / {n}"
          f"  {found/n*100:>5.1f}%")
    if found:
        print(f"  of those found, sized within 0.4x-2.5x of need {sized_ok:>4} / {found}"
              f"  {sized_ok/found*100:>5.1f}%")

    print("\n" + "=" * 78)
    print("SHAPE — pathologies precision would otherwise hide")
    print("=" * 78)
    self_t = [o for o in ops if o["from_org"] == o["to_org"]]
    print(f"  self-transfers (from == to)                    {len(self_t):>5}")
    fan = defaultdict(set)
    for o in ops:
        fan[(o["itm_cd"], o["to_org"])].add(o["from_org"])
    many = {k: v for k, v in fan.items() if len(v) >= 4}
    print(f"  recipients pulling one SKU from 4+ donors      {len(many):>5}")
    if many:
        k, v = next(iter(many.items()))
        print(f"      e.g. {k[0]} -> {k[1]} from {len(v)} donors")
    singles = [o for o in ops if o["qty"] <= 1]
    print(f"  moves of a single unit                         {len(singles):>5}"
          f"  ({len(singles)/max(1,len(ops))*100:.0f}% of all moves)")
    planted_keys = {(p["itm_cd"], p["donor_org"], p["recipient_org"]) for p in planted}
    unscored = sum(1 for o in ops
                   if (o["itm_cd"], o["from_org"], o["to_org"]) not in planted_keys)
    print(f"  unplanted moves (UNSCORED, not wrong)          {unscored:>5}")

    print("\n" + "=" * 78)
    print("MISSES — worth reading, these are the engine's blind spots")
    print("=" * 78)
    shown = 0
    for v, name, d, r, want, got in rows:
        if v == "MISSED" and shown < 12:
            print(f"  {name:<30} {d} -> {r}  needed {want:>8.1f}")
            shown += 1
    if not shown:
        print("  none")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
