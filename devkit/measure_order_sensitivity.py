"""Does greedy allocation actually change who gets stock?

Fair-share only matters if the CURRENT order-dependent allocation produces a
different outcome from a different order. So: run the identical scan with the
stores iterated in three different orders and compare what each recipient got.

If the outputs match, no recipient is being starved by iteration order and
fair-share is a refinement with nothing to fix. If they diverge, the size of
the divergence is the prize.
"""
import collections
import json
import logging
import os
import sys

sys.path.insert(0, r"C:\Users\iLink\.gemini\antigravity\scratch")
os.environ["OASIS_DB_PATH"] = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\variant_network.db"
os.environ.pop("OASIS_POS_DB_URL", None)
os.environ.pop("OASIS_ERP", None)
logging.basicConfig(level=logging.ERROR)

import oasis.desktop.data as D
from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService as CTS
from oasis.logic.consolidated_transfer_service import _is_fresh_department

ROOT = r"C:\Users\iLink\.gemini\antigravity\scratch"
adapter = D.get_adapter()
stores = D.list_stores()
names = {s["org_cd"]: s["name"] for s in stores}

print("loading network stock …")
net = {s["org_cd"]: (adapter.fetch_enriched_products(s["org_cd"]) or []) for s in stores}
nd = D._next_delivery_days(os.path.join(ROOT, "oasis", "data"), net)
print(f"  {len(net)} stores, calendar covers {len(nd)} suppliers\n")


def run(order):
    ordered = {k: net[k] for k in order}
    cts = CTS(org_names={k: names[k] for k in order}, stock_data=ordered,
              registry_path=None, distance_map=json.load(
                  open(os.path.join(ROOT, "store_coords.json"), encoding="utf-8")),
              cold_node_days=60, hot_node_days=14, next_delivery_days=nd)
    scan = cts.scan_network_opportunities()
    got = collections.defaultdict(float)          # (item, recipient) -> qty
    per_store = collections.Counter()
    donor_out = collections.defaultdict(float)    # (donor, item) -> qty
    for o in scan.opportunities:
        got[(o.itm_cd, o.to_org)] += o.transfer_qty
        per_store[o.to_org] += o.transfer_qty
        donor_out[(o.from_org, o.itm_cd)] += o.transfer_qty
    return scan, got, per_store, donor_out


base_order = [s["org_cd"] for s in stores]
orders = {
    "as-listed": base_order,
    "reversed": list(reversed(base_order)),
    # NOTE: sorting on len(net[o]) is DEGENERATE here — the adapter returns the
    # full catalogue for every store, so every value is equal, the sort is
    # stable, and this reproduces the base order exactly. Kept as a control:
    # it should always show zero difference, and if it ever does not, something
    # non-deterministic has crept into the scan.
    "control (same order)": sorted(base_order, key=lambda o: len(net[o])),
}

results = {}
for label, order in orders.items():
    scan, got, per_store, donor_out = run(order)
    results[label] = (got, per_store, donor_out)
    print(f"{label:<16} moves {len(scan.opportunities):>6}   "
          f"units {sum(per_store.values()):>12,.0f}")

print("\n" + "=" * 74)
print("DOES ORDER CHANGE WHO GETS WHAT?")
print("=" * 74)
base = results["as-listed"][0]
for label in ("reversed", "control (same order)"):
    other = results[label][0]
    keys = set(base) | set(other)
    diff = [(k, base.get(k, 0.0), other.get(k, 0.0)) for k in keys
            if abs(base.get(k, 0.0) - other.get(k, 0.0)) > 0.5]
    tot_b = sum(base.values()) or 1
    moved = sum(abs(b - o) for _, b, o in diff)
    print(f"  vs {label:<16} {len(diff):>6} of {len(keys):,} recipient-SKUs differ"
          f"   {moved:>12,.0f} units ({moved/tot_b*100:.1f}% of volume)")

print("\n" + "=" * 74)
print("PER-STORE TOTALS — is any store systematically favoured?")
print("=" * 74)
print(f"{'store':<28}{'as-listed':>13}{'reversed':>13}{'control':>13}{'spread':>10}")
pb, pr, ps = (results[k][1] for k in ("as-listed", "reversed", "control (same order)"))
for org in base_order:
    v = [pb[org], pr[org], ps[org]]
    spread = (max(v) - min(v)) / max(1.0, max(v)) * 100
    print(f"{names[org][:28]:<28}{v[0]:>13,.0f}{v[1]:>13,.0f}{v[2]:>13,.0f}"
          f"{spread:>9.1f}%")

print("\n" + "=" * 74)
print("CONTENTION — are donors actually running out?")
print("=" * 74)
donor_out = results["as-listed"][2]
exhausted = near = 0
checked = 0
for s in stores:
    org = s["org_cd"]
    for p in net[org]:
        itm = p.get("item_code")
        A = float(p.get("avg_daily_sales") or 0)
        S = float(p.get("current_stocks") or 0)
        fresh = bool(p.get("is_fresh")) or _is_fresh_department(str(p.get("department") or ""))
        E = CTS._excess_units(A, S, fresh)
        if E <= 0:
            continue
        out = donor_out.get((org, itm), 0.0)
        if out <= 0:
            continue
        checked += 1
        if out >= E * 0.99:
            exhausted += 1
        elif out >= E * 0.80:
            near += 1
print(f"  donor-SKUs that gave anything          {checked:>7,}")
print(f"  fully exhausted (>=99% of excess)      {exhausted:>7,}"
      f"  {exhausted/max(1,checked)*100:.1f}%")
print(f"  near-exhausted (80-99%)                {near:>7,}"
      f"  {near/max(1,checked)*100:.1f}%")
print("\n  A donor that never runs out cannot starve anyone, whatever the order.")
