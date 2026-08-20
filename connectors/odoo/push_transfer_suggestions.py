"""Compute the transfer plan and post it into Odoo's OASIS review queue.

This is the seam between the two halves of the product. OASIS reads the Odoo
depot, works out what should move and why, and posts SUGGESTIONS into
``oasis.transfer.suggestion``. The operator reviews them in Odoo — OASIS →
Transfers → Suggestions — and approving one creates the draft internal
transfer.

Nothing here writes a stock document. The queue is OASIS's own model; the only
thing that ever touches Odoo's inventory is an operator pressing Approve.

    python connectors/odoo/push_transfer_suggestions.py --dry-run
    python connectors/odoo/push_transfer_suggestions.py --limit 200
    python connectors/odoo/push_transfer_suggestions.py --clear
"""

from __future__ import annotations

import argparse
import contextlib
import io
import json
import os
import sys
from datetime import datetime

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
sys.path.insert(0, REPO)
sys.path.insert(0, HERE)
sys.path.insert(0, os.path.join(REPO, "devkit"))

from oasis.logic.odoo_adapter import OdooAdapter                      # noqa: E402
from oasis.logic.consolidated_transfer_service import (               # noqa: E402
    ConsolidatedTransferService as CTS)


def reason_for(o, relief=None):
    """The argument, in the operator's language — not the engine's.

    This text is what the customer is actually buying. A draft picking already
    tells them WHAT to move; only this tells them why it is worth doing, and
    what it costs to ignore. So it names the horizon, both positions, and the
    consequence — never 'risk_kes' or 'RELEASE_FRACTION'.
    """
    # 999 is the engine's sentinel for "no demand at all" — never show it to an
    # operator, who would read it as a real 999-day cover figure.
    def position(days):
        return ("is not selling it at all" if days >= 900
                else f"has {days:.0f} days of cover")

    if o.type == "PULL":
        head = (f"{o.to_org} runs out in about {o.recipient_days_cover:.0f} day"
                f"{'s' if round(o.recipient_days_cover) != 1 else ''}")
        if relief:
            head += f", before its next delivery is due in {relief:.0f}"
        body = (f". {o.from_org} is holding {o.donor_excess:.0f} spare and "
                f"{position(o.donor_days_cover)}, so moving "
                f"{o.transfer_qty:.0f} covers the gap without putting the donor short.")
        tail = (f" Leaving it costs roughly KES {o.value_kes:,.0f} of sales "
                f"on this line.")
        return head + body + tail
    return (f"{o.from_org} {position(o.donor_days_cover)} on this line and it is "
            f"not turning over — that is idle capital sitting on a shelf. "
            f"{o.to_org} sells it, and can absorb {o.transfer_qty:.0f} before the "
            f"line would go stale there too. Worth about KES {o.value_kes:,.0f} "
            f"at retail once it moves.")


def main(argv=None):
    p = argparse.ArgumentParser()
    p.add_argument("--limit", type=int, default=0,
                   help="post only the N most valuable suggestions (0 = all)")
    p.add_argument("--dry-run", action="store_true",
                   help="show what would be posted; write nothing")
    p.add_argument("--clear", action="store_true",
                   help="empty the pending queue and stop")
    args = p.parse_args(argv)

    a = OdooAdapter(url=os.getenv("ODOO_URL", "http://localhost:8069"),
                    db=os.getenv("ODOO_DB", "oasis"),
                    user=os.getenv("ODOO_USER", "admin"),
                    password=os.getenv("ODOO_PASSWORD", "admin"))
    if not a.health_check().get("connected"):
        raise SystemExit("Odoo unreachable — is the stack up?")

    if args.clear:
        res = a._ex("oasis.transfer.suggestion", "oasis_replace_queue", [[]])
        print(f"queue cleared ({res})")
        return 0

    seed = json.load(open(os.path.join(HERE, "store_network_seed.json"),
                          encoding="utf-8"))
    codes = [s["code"] for s in seed["stores"]]
    names = {s["code"]: s["name"] for s in seed["stores"]}
    coords = {s["code"]: {"lat": s["latitude"], "lon": s["longitude"]}
              for s in seed["stores"]}

    from verify_store_network import fetch_with_retry
    from oasis.desktop.data import store_db_path
    print("-> reading the depot through OdooAdapter…")
    data = {c: fetch_with_retry(a, c) for c in codes}
    with contextlib.redirect_stderr(io.StringIO()):
        svc = CTS(org_names=names, stock_data=data, distance_map=coords,
                  data_dir=os.path.join(REPO, "oasis", "data"),
                  settings_db=store_db_path(REPO))
        opps = svc.scan_network_opportunities().opportunities

    opps.sort(key=lambda o: -o.value_kes)
    kept = opps[:args.limit] if args.limit > 0 else opps
    rows = [{
        "item_code": o.itm_cd, "from_code": o.from_org, "to_code": o.to_org,
        "quantity": o.transfer_qty, "kind": o.type.lower(),
        "value": o.value_kes, "donor_cover": o.donor_days_cover,
        "recipient_cover": o.recipient_days_cover, "is_fresh": o.manual_only,
        "reason": reason_for(o, svc._median_relief),
    } for o in kept]

    pull = sum(1 for r in rows if r["kind"] == "pull")
    fresh = sum(1 for r in rows if r["is_fresh"])
    print(f"   plan: {len(opps):,} lines; posting {len(rows):,} "
          f"({pull:,} plug-a-gap, {len(rows) - pull:,} clear-idle, "
          f"{fresh:,} perishable)")

    if args.dry_run:
        print("\n--dry-run — nothing written. Three examples:\n")
        for r in rows[:3]:
            print(f"  {r['kind'].upper()}  {r['from_code']} -> {r['to_code']}  "
                  f"{r['quantity']:.0f} x {r['item_code'][:38]}")
            print(f"        {r['reason']}\n")
        return 0

    res = a._ex("oasis.transfer.suggestion", "oasis_replace_queue",
                [rows, datetime.now().strftime("%Y-%m-%d %H:%M:%S")])
    print(f"-> posted {res.get('created', 0):,} suggestions "
          f"({res.get('skipped', 0):,} skipped: unknown product or warehouse)")
    print("   Review in Odoo:  OASIS → Transfers → Suggestions")
    print("   Approving one creates a DRAFT internal transfer; nothing moves "
          "until your team confirms it.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
