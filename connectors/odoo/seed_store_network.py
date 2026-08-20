"""Seed the 14-outlet Chandarana store profile into the Odoo test depot.

WHY
---
Everything the transfer engine has been measured on rests on ONE Rhapta Road
snapshot fanned out by a demand_scale_factor. That is enough to prove the
arithmetic and not enough to trust the store-matrix axes: fair-share allocation,
donor contention and the cold/hot PUSH pass only mean anything when several real
stores with genuinely different stock and velocity compete for the same donor.

This puts all 14 outlets into Odoo as real warehouses, so OASIS reads them
through ``OdooAdapter`` over XML-RPC exactly as it would read a client's ERP —
no fixture, no mock, no direct DB access.

WHAT IT WRITES
--------------
  warehouses     one stock.warehouse per outlet, ALL IN ONE COMPANY so that
                 inter-store transfers are legal (Odoo refuses to confirm an
                 internal picking across companies — see OdooAdapter.can_transfer)
  categories     one product.category per department, under an OASIS root, so
                 split_hierarchy() reads "All / OASIS / <dept>"
  partners       one vendor per supplier in the slice
  products       the SKU slice, with list_price, standard_price and supplierinfo
  on-hand        stock.quant written directly, per store, from each outlet's own
                 qty — created or rewritten in place so re-runs converge
  receipts       done incoming moves, BACK-DATED across 2-56 days so
                 days_since_delivery is real rather than uniformly zero
  demand         done outgoing moves (stock -> customer) carrying each store's
                 own ads x window units, spread across the sales window

Both histories are written as bare ``stock.move`` records flipped to ``done``
rather than as validated pickings. That is deliberate on both sides: a validated
delivery would DEPLETE the on-hand just set, and filling a 3,000-line receipt
picking costs 3,000 round trips because Odoo 16's immediate-transfer wizard
cannot be driven in bulk over XML-RPC. The moves are the exact shape the
adapter's queries read (``state = done`` plus the right location usage), and are
probe-verified not to move stock.

HONEST LIMIT: each SKU's sales are spread across a few dated buckets, not
simulated day by day. ADS reproduces exactly over the window; intra-week
seasonality does not exist in this depot.

Idempotent: warehouses, partners, categories and products are found-or-created,
and a previous seed's movements are cleared before new ones are written, so
re-running converges rather than accumulating.

Usage:
    python build_store_network_seed.py           # produce the slice first
    python seed_store_network.py                 # then seed all 14 outlets
    python seed_store_network.py --stores 1      # smoke test on one outlet
    python seed_store_network.py --dry-run       # report, write nothing
"""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import time
import xmlrpc.client
from datetime import datetime, timedelta

HERE = os.path.dirname(os.path.abspath(__file__))
SEED = os.path.join(HERE, "store_network_seed.json")

#: everything this script writes is tagged, so a re-run can find and clear its
#: own movements without touching anything else in the depot
TAG = "OASIS-NETSEED"
CATEGORY_ROOT = "OASIS"
SALES_WINDOW_DAYS = 90
#: Days in a store's replenishment cycle. Each store-SKU pair is placed at a
#: deterministic point inside it, and stock is depleted by the selling that has
#: happened since. 8 is a weekly cycle plus a day of slack, which matches the
#: 7-day supplier lead time seeded on every product.
REPLENISHMENT_CYCLE_DAYS = 8
#: sales are split across this many dated buckets inside the window, so a demand
#: query over a SHORTER window still lands on a representative slice rather than
#: catching every unit or none.
SALES_BUCKETS = 3
BATCH = 500


def connect(url, db, user, password):
    common = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/common")
    uid = common.authenticate(db, user, password, {})
    if not uid:
        raise PermissionError("Odoo auth failed — check db/user/password.")
    models = xmlrpc.client.ServerProxy(f"{url}/xmlrpc/2/object")

    def ex(model, method, args, kw=None):
        # A full seed is tens of thousands of writes and Odoo will occasionally
        # recycle a worker or restart under that load, dropping the socket. That
        # is a blip, not a failure of the seed — but without a retry it aborts
        # the run halfway and leaves the depot half-written, which is worse than
        # either finishing or not starting.
        last = None
        for attempt in range(4):
            try:
                return models.execute_kw(db, uid, password, model, method,
                                         args, kw or {})
            except xmlrpc.client.Fault:
                raise                      # a real Odoo error — never retry it
            except Exception as e:         # transport-level only
                last = e
                if attempt < 3:
                    time.sleep(2 ** attempt)
        raise last
    return ex, uid


def with_company(ex, company_id):
    """Pin every call to one company.

    Needed because a record's company is NOT inherited from the record it points
    at — it comes from the USER's active company. Creating warehouses for company
    A while the admin's active company is B produces stock moves owned by B
    aimed at locations owned by A, and Odoo rejects the picking at validation
    with "Incompatible companies on records" — after the warehouse, its
    locations and its picking types have all already been created in the wrong
    place. Setting allowed_company_ids makes the two agree up front.
    """
    def _ex(model, method, args, kw=None):
        kw = dict(kw or {})
        ctx = dict(kw.get("context") or {})
        ctx.setdefault("allowed_company_ids", [company_id])
        kw["context"] = ctx
        return ex(model, method, args, kw)
    return _ex


def ignoring_none(ex, model, method, args, kw=None):
    """Call an Odoo action whose return may be None.

    Odoo's XML-RPC endpoint serialises with allow_none=False, so a method
    returning None raises "cannot marshal None" AFTER the write has committed.
    Same trap OdooAdapter._call_ignoring_none documents; the outcome must be
    confirmed by reading state back, never inferred from the return.
    """
    try:
        return ex(model, method, args, kw)
    except xmlrpc.client.Fault as e:
        if "cannot marshal None" not in str(e):
            raise
        return None


def chunked(seq, n=BATCH):
    for i in range(0, len(seq), n):
        yield seq[i:i + n]


def cycle_age(store_code: str, sku: str, cycle_days: int, plan_cover: float) -> int:
    """Days since this store last received this SKU.

    THE POINT OF VARYING IT PER STORE
    ---------------------------------
    stores_network.json is an opening-stock PLAN: every store's qty and ads are
    the same Rhapta snapshot scaled by the same demand_scale_factor, so
    qty/ads — days of cover — is nearly identical at every store for a given
    SKU. A network where no store differs from another has nothing to transfer,
    which is why the engine found zero opportunities on the undepleted seed.

    Depleting by an age that depends only on the SKU would preserve that exactly:
    subtracting the same number of days from every store leaves them equal. The
    age must therefore depend on the STORE as well — real outlets sit at
    different points in their replenishment cycle, and that phase difference is
    the whole source of the imbalance a transfer exploits.

    Deterministic via md5, NOT the builtin hash(), which is salted per process
    and would reshuffle the entire network on every run.

    Capped by plan_cover so a store is never depleted past empty by this model:
    the age represents time since delivery, and a SKU planned for 3 days of
    cover cannot have gone 8 days without one.
    """
    h = int(hashlib.md5(f"{store_code}|{sku}".encode("utf-8")).hexdigest()[:8], 16)
    span = max(1, int(min(float(cycle_days), max(1.0, plan_cover))))
    return 1 + (h % span)


#: A line with no local demand has not been reordered in months -- that is WHY
#: it is dead. Giving it the same 1-8 day receipt age as a live line is not a
#: cosmetic detail: FulfillmentDecider.find_donors grants a 2x preference to
#: aged, slow-moving donors gated on `days_since_delivery > 45`, so a recent
#: receipt date makes dead stock invisible to the very rule meant to clear it.
DEAD_MIN_AGE_DAYS, DEAD_MAX_AGE_DAYS = 60, 180


def receipt_age(store_code: str, sku: str, ads: float, qty: float,
                cycle_days: int) -> int:
    """Days since this store last received this SKU -- ONE definition.

    Shared with devkit/analyse_transfer_funnel.py, which reconstructs the depot
    offline; if the two drifted, the analysis would be measuring a network that
    is not the one in Odoo.
    """
    if ads > 0:
        plan_cover = qty / ads if ads > 0 else 1.0
        return cycle_age(store_code, sku, cycle_days or 8, plan_cover)
    span = DEAD_MAX_AGE_DAYS - DEAD_MIN_AGE_DAYS
    h = int(hashlib.md5(f"dead|{store_code}|{sku}".encode("utf-8")).hexdigest()[:8], 16)
    return DEAD_MIN_AGE_DAYS + (h % span)


def clean_name(s: str) -> str:
    """Odoo's category path is '/'-delimited and OdooAdapter.split_hierarchy
    splits on it, so a department containing '/' would silently read as two
    levels of hierarchy. Neutralise it at the source."""
    return str(s or "").replace("/", "-").strip() or "GENERAL"


class Seeder:
    def __init__(self, ex, seed, company_id, dry_run=False,
                 cycle_days=REPLENISHMENT_CYCLE_DAYS):
        self.ex = ex
        self.seed = seed
        self.company_id = company_id
        self.dry_run = dry_run
        #: 0 disables depletion and seeds the raw opening-stock plan
        self.cycle_days = int(cycle_days)
        self.t0 = time.time()

    def say(self, msg):
        print(f"  [{time.time() - self.t0:6.1f}s] {msg}")

    # ── reference data ────────────────────────────────────────────────────
    def warehouses(self, stores):
        """One warehouse per outlet, all inside ONE company.

        Company matters: Odoo refuses to CONFIRM an internal picking whose
        source and destination belong to different legal entities, and the
        create still succeeds — so a cross-company depot would produce transfers
        that sit at REQUESTED forever. can_transfer() exists to catch exactly
        that, and the depot should not be built to trip it.
        """
        out = {}
        for st in stores:
            code = st["code"]
            # scoped to the target company on purpose: a same-code warehouse in
            # ANOTHER company must not be reused, or every transfer to it would
            # be created fine and then refused at confirmation
            found = self.ex("stock.warehouse", "search_read",
                            [[["code", "=", code],
                              ["company_id", "=", self.company_id]]],
                            {"fields": ["name", "lot_stock_id", "in_type_id",
                                        "int_type_id", "company_id"], "limit": 1})
            if found:
                out[code] = found[0]
                continue
            if self.dry_run:
                out[code] = {"id": 0, "lot_stock_id": [0], "in_type_id": [0]}
                continue
            wid = self.ex("stock.warehouse", "create", [{
                "name": f"{st['name']} ({st['store_id']})",
                "code": code,
                "company_id": self.company_id,
            }])
            out[code] = self.ex("stock.warehouse", "read", [[wid],
                                ["name", "lot_stock_id", "in_type_id",
                                 "int_type_id", "company_id"]])[0]
        self.say(f"warehouses ready: {len(out)}")
        return out

    def categories(self, departments):
        """Departments hang directly off Odoo's own root, NOT a custom parent.

        Odoo reads a category back as a PATH and OdooAdapter.split_hierarchy
        takes the first meaningful level as the department. Nesting these under
        an "OASIS" root produced "All / OASIS / FRESH MILK", so every product in
        the depot reported its department as "OASIS" — one department for 3,000
        SKUs, which collapses department grouping, budget allocation and the
        fresh rules. Only "All" is dropped as Odoo's synthetic root, so the
        department must sit at that first level.
        """
        root = self.ex("product.category", "search",
                       [[["parent_id", "=", False]]], {"limit": 1})
        root = root[0] if root else False

        existing = {c["name"]: c["id"] for c in self.ex(
            "product.category", "search_read", [[["parent_id", "=", root]]],
            {"fields": ["name"], "limit": 5000})}

        # re-parent any left under the old custom root by a previous run
        stale = self.ex("product.category", "search_read",
                        [[["parent_id.name", "=", CATEGORY_ROOT]]],
                        {"fields": ["name"], "limit": 5000})
        if stale and not self.dry_run:
            self.ex("product.category", "write",
                    [[c["id"] for c in stale], {"parent_id": root}])
            existing.update({c["name"]: c["id"] for c in stale})
            self.say(f"re-parented {len(stale)} categories off '{CATEGORY_ROOT}' "
                     f"(they were reading as department '{CATEGORY_ROOT}')")

        missing = [d for d in departments if d not in existing]
        if missing and not self.dry_run:
            for batch in chunked(missing):
                ids = self.ex("product.category", "create",
                              [[{"name": d, "parent_id": root} for d in batch]])
                existing.update(dict(zip(batch, ids)))
        self.say(f"categories ready: {len(existing)} at department level "
                 f"({len(missing)} new)")
        return existing

    def partners(self, suppliers):
        existing = {p["name"]: p["id"] for p in self.ex(
            "res.partner", "search_read", [[["supplier_rank", ">", 0]]],
            {"fields": ["name"], "limit": 10000})}
        missing = [s for s in suppliers if s not in existing]
        if missing and not self.dry_run:
            for batch in chunked(missing):
                ids = self.ex("res.partner", "create",
                              [[{"name": s, "supplier_rank": 1,
                                 "ref": f"{TAG}:{s[:40]}"} for s in batch]])
                existing.update(dict(zip(batch, ids)))
        self.say(f"vendors ready: {len(missing)} new, {len(existing)} total")
        return existing

    def products(self, catalogue, cat_ids, partner_ids):
        """The SKU slice as storable products, with cost, price and a vendor.

        default_code is the SKU name itself: OASIS keys everything on item_code,
        and the adapter emits `default_code` as item_code, so making them the
        same string is what lets a transfer computed in OASIS be pushed back
        into Odoo by code without a translation table.
        """
        codes = [c["sku"][:64] for c in catalogue]
        existing = {}
        for batch in chunked(codes):
            for p in self.ex("product.product", "search_read",
                             [[["default_code", "in", batch]]],
                             {"fields": ["default_code"], "limit": 5000}):
                existing[p["default_code"]] = p["id"]

        todo = [c for c in catalogue if c["sku"][:64] not in existing]
        if todo and not self.dry_run:
            for batch in chunked(todo, 200):
                vals = [{
                    "name": c["sku"][:120],
                    "default_code": c["sku"][:64],
                    "type": "product",
                    "list_price": round(float(c["price"]), 2),
                    "standard_price": round(float(c["cost"]), 2),
                    "categ_id": cat_ids.get(clean_name(c["department"])),
                } for c in batch]
                ids = self.ex("product.product", "create", [vals])
                existing.update({c["sku"][:64]: i for c, i in zip(batch, ids)})
            # vendor link, so supplier_cd/supplier_name and lead time are real
            tmpl = {}
            for batch in chunked([existing[c["sku"][:64]] for c in todo]):
                for r in self.ex("product.product", "read",
                                 [batch, ["product_tmpl_id"]]):
                    tmpl[r["id"]] = r["product_tmpl_id"][0]
            si = []
            for c in todo:
                pid = existing.get(c["sku"][:64])
                partner = partner_ids.get(c["supplier"])
                if pid and partner and pid in tmpl:
                    si.append({"partner_id": partner,
                               "product_tmpl_id": tmpl[pid],
                               "price": round(float(c["cost"]), 2),
                               "delay": 7})
            for batch in chunked(si, 200):
                self.ex("product.supplierinfo", "create", [batch])
        self.say(f"products ready: {len(todo)} new, {len(existing)} total")
        return existing

    # ── movements ─────────────────────────────────────────────────────────
    def clear_previous(self, store_codes):
        """Remove a previous seed's SALES moves so re-runs converge.

        Only the tagged, non-picking moves are removable — Odoo refuses to
        unlink moves belonging to a done picking, so seeded RECEIPTS stay and
        on-hand is corrected by adjusting the next receipt instead.
        """
        if self.dry_run:
            return 0
        # SCOPED TO THE STORES BEING SEEDED. Clearing every tagged sale would
        # mean `--stores 1` silently destroys the demand history of the other
        # thirteen: they keep their stock, lose their ADS, and every one of them
        # then reads as a cold donor with 999 days of cover — which looks like a
        # network with no demand rather than a half-wiped depot.
        # RECV as well as SALE: the receipt dates encode each store's position
        # in its replenishment cycle, so leaving stale ones behind would pair a
        # re-depleted stock level with the previous run's delivery dates
        ids = []
        for code in store_codes:
            for kind in ("SALE", "RECV"):
                ids += self.ex("stock.move", "search",
                               [[["name", "=", f"{TAG}:{kind} {code}"]]],
                               {"limit": 200000})
        removed = 0
        for batch in chunked(ids, 1000):
            try:
                self.ex("stock.move", "write", [batch, {"state": "draft"}])
                self.ex("stock.move", "unlink", [batch])
                removed += len(batch)
            except xmlrpc.client.Fault as e:
                self.say(f"! could not clear {len(batch)} old sales moves: "
                         f"{str(e)[:100]}")
        if removed:
            self.say(f"cleared {removed:,} movement records from a previous seed")
        return removed

    def receipts(self, store, wh, prod_ids, uom, supplier_loc):
        """Set on-hand directly on stock.quant, and write receipt HISTORY beside it.

        Two concerns, seeded independently and on purpose:

          on-hand   ``stock.quant.quantity``, created or rewritten in bulk. This
                    is a fixture setting an opening position, which is what an
                    inventory adjustment models — and quants can be rewritten in
                    place, so a re-run converges instead of accumulating.

          receipts  bare ``stock.move`` records (vendor -> stock) flipped to
                    ``done`` and back-dated. They exist so days_since_delivery is
                    REAL: without any incoming move every product reads 0 days
                    since delivery and the dead-stock guard silently fails OPEN,
                    which is the failure OdooAdapter.diagnose warns about.

        Why not a validated receipt picking, which would produce both at once:
        filling 3,000 done quantities costs 3,000 round trips (~140s per store),
        and Odoo 16's immediate-transfer wizard cannot be driven over XML-RPC to
        do it in bulk — ``process()`` takes its targets from a context key and
        still leaves qty_done at zero, so validation falls through to a backorder.
        Probe-verified: a bare move flipped to done does NOT touch quants, so the
        two seeds cannot corrupt each other.

        HONEST LIMIT: the movement history does not arithmetically produce the
        on-hand figure. This is a test depot for reading, not a reconstruction of
        the ledger.
        """
        stock_loc = wh["lot_stock_id"][0]
        if self.dry_run:
            return 0, 0

        # ── position each SKU inside this store's replenishment cycle ──────
        # on_hand = planned opening stock MINUS what has sold since the last
        # delivery. The same age sets the receipt date below, so the depot is
        # internally consistent: days_since_delivery x ADS actually accounts for
        # the gap between the plan and the stock on the shelf.
        target, age_of = {}, {}
        depleted_units = stockouts = 0.0
        for r in store["stock_profile"]:
            pid = prod_ids.get(r["sku"][:64])
            qty = float(r["qty"])
            if not pid or qty <= 0:
                continue
            ads = float(r.get("ads") or 0)
            age = receipt_age(store["code"], r["sku"], ads, qty, self.cycle_days)
            if self.cycle_days > 0 and ads > 0:
                sold = ads * age
                # whole units: the source quantities are integers, and a shelf
                # holds whole items. It also collapses the distinct-value count
                # from ~1,500 to ~200 per store, and quants are written grouped
                # BY VALUE — so the unrounded model cost 1,500 write calls per
                # store and was enough load to make Odoo drop the connection.
                on_hand = float(int(round(max(0.0, qty - sold))))
                depleted_units += min(sold, qty)
                if on_hand <= 0:
                    stockouts += 1
            else:
                on_hand = qty          # dead, or depletion disabled: stock stands
            age_of[pid] = age
            target[pid] = target.get(pid, 0.0) + on_hand
        if self.cycle_days > 0:
            self.say(f"  {store['code']}: depleted {depleted_units:,.0f} units "
                     f"over a {self.cycle_days}d cycle, {stockouts:,.0f} at zero")

        # every quant already at this site, including products NOT in the slice
        # — those get zeroed, so a re-run after a smaller slice leaves no stock
        # behind that no longer belongs to the profile
        held = {}
        for q in self.ex("stock.quant", "search_read",
                         [[["location_id", "child_of", stock_loc]]],
                         {"fields": ["product_id", "quantity"], "limit": 100000}):
            if q.get("product_id"):
                held[q["product_id"][0]] = (q["id"], float(q.get("quantity") or 0))

        creates, rewrites = [], {}
        for pid, qty in sorted(target.items()):
            qty = round(qty, 2)
            if pid in held:
                if abs(held[pid][1] - qty) > 0.001:
                    rewrites.setdefault(qty, []).append(held[pid][0])
            else:
                creates.append({"product_id": pid, "location_id": stock_loc,
                                "quantity": qty})
        for pid, (qid, qty) in held.items():          # stale, not in the slice
            if pid not in target and abs(qty) > 0.001:
                rewrites.setdefault(0.0, []).append(qid)

        for batch in chunked(creates, 1000):
            self.ex("stock.quant", "create", [batch])
        # grouped by value so identical quantities share one write instead of
        # one call per quant
        for qty, ids in rewrites.items():
            for batch in chunked(ids, 1000):
                self.ex("stock.quant", "write", [batch, {"quantity": qty}])
        touched = len(creates) + sum(len(v) for v in rewrites.values())

        # ── receipt history ───────────────────────────────────────────────
        # Moves belonging to a DONE picking cannot be unlinked, so clear_previous
        # may not have removed an older run's receipts. Re-date those in place
        # rather than skipping the store: a surviving receipt from a previous
        # cycle would contradict the stock level just written.
        survivors = self.ex("stock.move", "search_read",
                            [[["name", "=", f"{TAG}:RECV {store['code']}"]]],
                            {"fields": ["product_id"], "limit": 100000})
        if survivors:
            regroup = {}
            for m in survivors:
                pid = m["product_id"][0] if m.get("product_id") else None
                regroup.setdefault(age_of.get(pid, 7), []).append(m["id"])
            for age, ids in regroup.items():
                when = (datetime.now() - timedelta(days=age)).strftime("%Y-%m-%d %H:%M:%S")
                for batch in chunked(ids, 1000):
                    self.ex("stock.move", "write", [batch, {"date": when}])
            self.say(f"  {store['code']}: re-dated {len(survivors):,} undeletable "
                     f"receipt moves to the new cycle")
            return touched, len(regroup)

        vals, ages = [], set()
        for pid, qty in sorted(target.items()):
            # the SAME age that drove the depletion — a receipt date that
            # disagreed with the stock level would make days_since_delivery a
            # decoration rather than a fact, and the dead-stock guard reads it
            age = age_of.get(pid, 7)
            ages.add(age)
            when = (datetime.now() - timedelta(days=age)).strftime("%Y-%m-%d %H:%M:%S")
            # the receipt is what the store RECEIVED, i.e. the planned quantity,
            # not what is left on the shelf after selling since
            vals.append({"name": f"{TAG}:RECV {store['code']}",
                         "product_id": pid, "product_uom_qty": round(qty, 2),
                         "product_uom": uom, "location_id": supplier_loc,
                         "location_dest_id": stock_loc, "date": when})
        made = 0
        for batch in chunked(vals, 1000):
            ids = self.ex("stock.move", "create", [batch])
            self.ex("stock.move", "write", [ids, {"state": "done"}])
            made += len(ids)
        return touched, len(ages)

    def sales(self, store, wh, prod_ids, uom, customer_loc):
        """Each store's own ADS as done outgoing moves, spread over the window."""
        if self.dry_run:
            return 0
        stock_loc = wh["lot_stock_id"][0]
        vals = []
        for r in store["stock_profile"]:
            ads = float(r.get("ads") or 0)
            if ads <= 0:
                continue                      # dead at this store — no demand
            pid = prod_ids.get(r["sku"][:64])
            if not pid:
                continue
            total = ads * SALES_WINDOW_DAYS
            per = total / SALES_BUCKETS
            if per < 0.01:
                continue
            for b in range(SALES_BUCKETS):
                # buckets sit inside the window, never on its edge
                day = int(SALES_WINDOW_DAYS * (b + 0.5) / SALES_BUCKETS)
                when = (datetime.now() - timedelta(days=day)).strftime("%Y-%m-%d %H:%M:%S")
                vals.append({"name": f"{TAG}:SALE {store['code']}",
                             "product_id": pid, "product_uom_qty": round(per, 3),
                             "product_uom": uom, "location_id": stock_loc,
                             "location_dest_id": customer_loc, "date": when})
        made = 0
        for batch in chunked(vals, 1000):
            ids = self.ex("stock.move", "create", [batch])
            # flip to done WITHOUT a picking: probe-verified to persist and to
            # leave quants untouched, which is what keeps on-hand independent
            # of the demand history written here
            self.ex("stock.move", "write", [ids, {"state": "done"}])
            made += len(ids)
        return made


def main(argv=None):
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--odoo", default=os.getenv("ODOO_URL", "http://localhost:8069"))
    p.add_argument("--db", default=os.getenv("ODOO_DB", "oasis"))
    p.add_argument("--user", default=os.getenv("ODOO_USER", "admin"))
    p.add_argument("--password", default=os.getenv("ODOO_PASSWORD", "admin"))
    p.add_argument("--seed", default=SEED)
    p.add_argument("--stores", type=int, default=0,
                   help="seed only the first N outlets (0 = all)")
    p.add_argument("--company", type=int, default=0,
                   help="company id to build the network in (0 = first)")
    p.add_argument("--cycle-days", type=int, default=REPLENISHMENT_CYCLE_DAYS,
                   help="replenishment cycle each store-SKU pair is positioned "
                        f"inside (default {REPLENISHMENT_CYCLE_DAYS}); stock is "
                        "depleted by the selling since its last delivery")
    p.add_argument("--no-deplete", action="store_true",
                   help="seed the raw opening-stock plan instead — every store "
                        "at full planned cover, which cannot express imbalance")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args(argv)

    if not os.path.exists(args.seed):
        print(f"! {args.seed} not found — run build_store_network_seed.py first.")
        return 1
    seed = json.load(open(args.seed, encoding="utf-8"))
    stores = seed["stores"][:args.stores] if args.stores > 0 else seed["stores"]

    print(f"-> Odoo {args.odoo} db={args.db}")
    ex, uid = connect(args.odoo, args.db, args.user, args.password)
    # The USER's company, not res.company's first row: that model orders by
    # name, so on the stock demo database "My Company (Chicago)" sorts ahead of
    # "My Company (San Francisco)" and the network would be built in whichever
    # company happens to sort first — which is not the one the depot, and every
    # record this user creates by default, belongs to.
    company = args.company or ex("res.users", "read",
                                 [[uid], ["company_id"]])[0]["company_id"][0]
    cname = ex("res.company", "read", [[company], ["name"]])[0]["name"]
    ex = with_company(ex, company)
    print(f"   uid {uid}, company {company} ({cname})")
    print(f"-> seeding {len(stores)} outlets x {seed['sku_count']:,} SKUs "
          f"{'(DRY RUN)' if args.dry_run else ''}")

    cycle = 0 if args.no_deplete else args.cycle_days
    print(f"   stock model: " + ("opening plan, no depletion" if not cycle else
          f"depleted over a {cycle}-day replenishment cycle"))
    s = Seeder(ex, seed, company, args.dry_run, cycle_days=cycle)
    catalogue = seed["catalogue"]
    for c in catalogue:
        c["department"] = clean_name(c["department"])

    whs = s.warehouses(stores)
    cats = s.categories(sorted({c["department"] for c in catalogue}))
    parts = s.partners(sorted({c["supplier"] for c in catalogue}))
    prods = s.products(catalogue, cats, parts)

    if args.dry_run:
        print("\nDRY RUN — nothing written.")
        return 0

    uom = ex("product.product", "read",
             [[next(iter(prods.values()))], ["uom_id"]])[0]["uom_id"][0]
    supplier_loc = ex("stock.location", "search",
                      [[["usage", "=", "supplier"]]], {"limit": 1})[0]
    customer_loc = ex("stock.location", "search",
                      [[["usage", "=", "customer"]]], {"limit": 1})[0]

    s.clear_previous([st["code"] for st in stores])

    total_recv = total_sales = 0
    for st in stores:
        wh = whs[st["code"]]
        n_quants, n_dates = s.receipts(st, wh, prods, uom, supplier_loc)
        n_sales = s.sales(st, wh, prods, uom, customer_loc)
        total_recv += n_quants
        total_sales += n_sales
        s.say(f"{st['code']} {st['name'][:30]:<30} "
              f"quants {n_quants:>5,}  receipt dates {n_dates:>3}  "
              f"sales {n_sales:>6,}")

    print(f"\n-> done: {total_recv:,} quants set, {total_sales:,} sales moves "
          f"across {len(stores)} outlets in {time.time() - s.t0:.0f}s")
    print("   verify with:  python verify_store_network.py")
    return 0


if __name__ == "__main__":
    sys.exit(main())
