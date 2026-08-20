"""End-to-end acceptance test of the transfer algorithm against a live Odoo.

DEV TOOLING - devkit/, never ships.

Runs the plan from the pre-test audit, in order, and reports what it finds:

  1  SCAN        a fresh plan reaches the queue
  2  SANITY      read-only checks on what the queue actually contains
  3  APPROVE     one route becomes ONE draft internal transfer
  4  FEEDBACK    a re-scan must NOT re-propose what was just approved
  5  REPORT      what to look at by hand

Stage 4 is the one that matters. It exercises the whole loop — scan, post,
approve, re-scan — and it is the check that failed before H2 was fixed, when
the Odoo path scanned without telling the engine what was already in flight.

WHAT THIS WRITES: OASIS's own queue, and DRAFT internal transfers for one
route. A draft reserves nothing and moves no stock. --undo removes them.

    python devkit/test_transfer_in_odoo.py
    python devkit/test_transfer_in_odoo.py --undo
"""

from __future__ import annotations

import argparse
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

REPO = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, os.path.join(REPO, "connectors", "odoo"))

for _s in (sys.stdout, sys.stderr):
    try:
        _s.reconfigure(encoding="utf-8", errors="replace")
    except (AttributeError, ValueError):
        pass

from oasis.logic.odoo_adapter import OdooAdapter                      # noqa: E402

M = "oasis.transfer.suggestion"
PASS, FAIL, WARN = [], [], []
ORIGIN = "OASIS transfer"


def check(ok, label, detail=""):
    bucket = PASS if ok is True else (WARN if ok == "warn" else FAIL)
    bucket.append(label)
    tag = "ok  " if ok is True else ("warn" if ok == "warn" else "FAIL")
    print(f"   [{tag}] {label}" + (f" — {detail}" if detail else ""))


def undo(ex):
    picks = ex("stock.picking", "search_read",
               [[["origin", "like", ORIGIN], ["state", "=", "draft"]]],
               {"fields": ["name"]}) or []
    for p in picks:
        mv = ex("stock.move", "search", [[["picking_id", "=", p["id"]]]])
        if mv:
            ex("stock.move", "write", [mv, {"state": "draft"}])
            ex("stock.move", "unlink", [mv])
        ex("stock.picking", "unlink", [[p["id"]]])
    ex(M, "write", [ex(M, "search", [[["state", "=", "approved"]]]),
                    {"state": "new", "picking_id": False}])
    print(f"removed {len(picks)} draft transfer(s); approved suggestions reset")


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=200)
    p.add_argument("--undo", action="store_true")
    args = p.parse_args(argv)

    a = OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                    db=os.getenv("ODOO_DB", "oasis"),
                    user=os.getenv("ODOO_USER", "admin"),
                    password=os.getenv("ODOO_PASSWORD", "admin"))
    if not a.health_check().get("connected"):
        raise SystemExit("Odoo unreachable")
    ex = a._ex

    if args.undo:
        undo(ex)
        return 0

    print("=" * 74)
    print("TRANSFER ALGORITHM — END TO END ON LIVE ODOO")
    print("=" * 74)

    # ── 1. scan ───────────────────────────────────────────────────────────
    print("\n1. SCAN")
    from push_transfer_suggestions import run_scan
    res = run_scan(limit=args.limit, log=lambda m: None)
    check(res["created"] > 0, "a fresh plan reached the queue",
          f"{res['created']:,} suggestions, {res['skipped']} skipped")

    rows = ex(M, "search_read", [[["state", "=", "new"]]],
              {"fields": ["kind", "product_id", "from_warehouse_id",
                          "to_warehouse_id", "quantity", "value_kes", "reason",
                          "donor_ads", "recipient_ads", "donor_days_cover",
                          "recipient_days_cover", "relief_days", "is_fresh",
                          "is_stale"]}) or []

    # ── 2. sanity ─────────────────────────────────────────────────────────
    print("\n2. SANITY — what is actually in the queue")
    check(all(r["reason"] for r in rows), "every line carries its reasoning")
    check(not [r for r in rows if r["donor_days_cover"] >= 900
               or r["recipient_days_cover"] >= 900],
          "no 999 sentinel leaked into a cover figure")
    check(not [r for r in rows if r["is_stale"]], "nothing is stale on arrival")
    check(all(r["quantity"] > 0 for r in rows), "no zero-quantity lines")
    self_moves = [r for r in rows
                  if r["from_warehouse_id"][0] == r["to_warehouse_id"][0]]
    check(not self_moves, "no self-transfers")

    # A transfer must not exceed the need, ALLOWING ONE UNIT of rounding:
    # you cannot ship 0.4 of a sack, so a fractional need on a slow mover
    # legitimately becomes 1. Anything beyond that is over-transfer.
    over = [r for r in rows if r["kind"] == "pull" and r["recipient_ads"] > 0
            and r["quantity"] > r["recipient_ads"] * max(r["relief_days"], 1) + 1]
    check(not over, "pull quantities stay within need (+1 unit of rounding)",
          f"{len(over)} exceed it" if over else "")

    singles = [r for r in rows if r["quantity"] <= 1]
    check("warn" if singles else True, "single-unit lines are rounding, not error",
          f"{len(singles)} of {len(rows)} — fractional needs ceiled to a whole unit")

    fresh = [r for r in rows if r["is_fresh"]]
    check("warn" if fresh else True, "perishables present and flagged",
          f"{len(fresh)} lines — never auto-queued, approve only to dispatch today")

    kinds = {}
    for r in rows:
        kinds[r["kind"]] = kinds.get(r["kind"], 0) + 1
    print(f"        composition: {kinds}")

    # ── 3. approve one route ──────────────────────────────────────────────
    print("\n3. APPROVE — one route becomes one draft transfer")
    routes = {}
    for r in rows:
        if r["is_fresh"]:
            continue                                    # never auto-approve fresh
        routes.setdefault((r["from_warehouse_id"][0], r["to_warehouse_id"][0]),
                          []).append(r)
    route = max(routes.items(), key=lambda kv: len(kv[1]))
    (src, dst), lines = route
    # the grouping key is a warehouse ID; the adapter speaks CODES
    src_code = ex("stock.warehouse", "read", [[src], ["code"]])[0]["code"]
    lines = lines[:5]
    ids = [r["id"] for r in lines]
    src_name = lines[0]["from_warehouse_id"][1]
    dst_name = lines[0]["to_warehouse_id"][1]
    print(f"        {src_name} -> {dst_name}, {len(ids)} lines")

    before = ex("stock.picking", "search_count", [[["origin", "like", ORIGIN]]])
    ex(M, "action_approve", [ids])
    after = ex("stock.picking", "search_count", [[["origin", "like", ORIGIN]]])
    check(after - before == 1, "several lines produced ONE picking, not one each",
          f"{after - before} picking(s) created")

    got = ex(M, "search_read", [[["id", "in", ids]]],
             {"fields": ["state", "picking_id"]})
    check(all(g["state"] == "approved" and g["picking_id"] for g in got),
          "approved suggestions are linked to their transfer")
    pk = got[0]["picking_id"][0]
    pick = ex("stock.picking", "read", [[pk], ["state", "origin", "move_ids"]])[0]
    check(pick["state"] == "draft", "the transfer is DRAFT — nothing reserved",
          f"{pick['origin']}, {len(pick['move_ids'])} move line(s)")

    # ── 4. the feedback loop ──────────────────────────────────────────────
    print("\n4. FEEDBACK — a re-scan must not re-propose what was approved")
    # The right invariant is NOT "never proposed again". A transfer capped by
    # the donor's pool only partly closes the gap, and the remainder is real
    # need that a later scan should still serve. What must hold is that the
    # committed stock is CREDITED — so any repeat is strictly smaller — and
    # that the donor is never drawn below its safety floor.
    approved = {(r["product_id"][0], r["kind"]): r["quantity"] for r in lines}
    run_scan(limit=args.limit, log=lambda m: None)
    again = ex(M, "search_read", [[["state", "=", "new"]]],
               {"fields": ["product_id", "kind", "quantity",
                           "from_warehouse_id", "to_warehouse_id"]}) or []
    repeats = [r for r in again
               if (r["product_id"][0], r["kind"]) in approved
               and r["from_warehouse_id"][0] == src
               and r["to_warehouse_id"][0] == dst]
    grew = [r for r in repeats
            if r["quantity"] >= approved[(r["product_id"][0], r["kind"])]]
    check(not grew, "committed stock is credited — any repeat is SMALLER",
          f"{len(repeats)} residual line(s), largest "
          f"{max((r['quantity'] for r in repeats), default=0):.0f} vs "
          f"{max(approved.values()):.0f} approved" if repeats else
          "nothing came back at all")

    # and the donor must still be above its own safety floor
    donor_stock = {p["item_code"]: p
                   for p in a.fetch_enriched_products(src_code)}
    breached = []
    for r in repeats:
        code = r["product_id"][1].split("] ")[0].lstrip("[")
        prod = donor_stock.get(code)
        if not prod:
            continue
        ads = float(prod["avg_daily_sales"])
        if float(prod["current_stocks"]) - r["quantity"] < ads * 14:
            breached.append((code, r["quantity"]))
    check(not breached, "donor stays above its 14-day safety floor after the residual",
          f"{len(breached)} would breach it: {breached[:2]}" if breached else
          f"{len(repeats)} residual line(s) checked against {src_code}")

    # ── 5. report ─────────────────────────────────────────────────────────
    print("\n" + "=" * 74)
    print(f"{len(PASS)} passed, {len(WARN)} to note, {len(FAIL)} failed")
    if FAIL:
        for f in FAIL:
            print(f"  FAILED: {f}")
    print(f"\nLook at by hand:  OASIS → Transfers → Suggestions")
    print(f"  the draft:      Inventory → Transfers → {pick['origin']}")
    print(f"  undo:           python devkit/test_transfer_in_odoo.py --undo")
    print("\nTwo sample lines, to judge the reasoning against your own view:")
    for r in lines[:2]:
        print(f"\n  {r['kind'].upper()}  {r['quantity']:.0f} x "
              f"{r['product_id'][1][:44]}")
        print(f"    {r['reason']}")
    return 1 if FAIL else 0


if __name__ == "__main__":
    raise SystemExit(main())
