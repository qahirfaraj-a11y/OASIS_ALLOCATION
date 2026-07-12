"""
Provision the OASIS Cloud Hub for the Odoo demo, idempotently.

Registers a tenant, a store (and mints its ingest token), a supplier with an
ownership rule, and a granted consent — everything the portal needs to show live
Odoo data. Prints the ingest token and writes it to ``.hub_state.json`` so
``configure_and_sync.py`` can hand it to Odoo.

    python bootstrap_hub.py                       # localhost defaults
    python bootstrap_hub.py --hub http://host:8700 --admin <key>

Stdlib only — runs anywhere with Python 3.
"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request

STATE = os.path.join(os.path.dirname(os.path.abspath(__file__)), ".hub_state.json")

# Demo defaults — must match docker-compose.odoo.yml and configure_and_sync.py.
DEF_HUB = "http://localhost:8700"
DEF_ADMIN = "demo-admin"
TENANT = {"tenant_id": "acme", "name": "Acme Retail Group", "country": "KE"}
STORE = {"store_code": "ODOO-01", "store_name": "Acme (Odoo)", "city": "Nairobi"}
SUPPLIER = {"supplier_code": "COKE", "name": "Coca-Cola Beverages", "password": "demo123"}
OWNERSHIP = {"match_type": "supplier_cd", "match_value": "SUP_COKE"}


def _post(hub, admin, path, body):
    data = json.dumps(body).encode()
    req = urllib.request.Request(
        hub + path, data=data, method="POST",
        headers={"Content-Type": "application/json", "X-Hub-Admin-Key": admin})
    try:
        with urllib.request.urlopen(req, timeout=15) as r:
            return r.getcode(), json.loads(r.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        try:
            return e.code, json.loads(e.read().decode() or "{}")
        except ValueError:
            return e.code, {}


def _ok(status):
    # 200 = created; 409 = already exists (idempotent re-run)
    return status in (200, 409)


def main(argv=None):
    p = argparse.ArgumentParser(description="Provision the OASIS hub for Odoo")
    p.add_argument("--hub", default=os.getenv("OASIS_HUB_URL", DEF_HUB))
    p.add_argument("--admin", default=os.getenv("OASIS_HUB_ADMIN_KEY", DEF_ADMIN))
    args = p.parse_args(argv)
    hub, admin = args.hub.rstrip("/"), args.admin

    print(f"→ provisioning hub at {hub}")
    st, _ = _post(hub, admin, "/admin/tenants", TENANT)
    if not _ok(st):
        print(f"  ! tenant failed ({st}) — is the hub up? try: curl {hub}/health")
        return 1
    print(f"  tenant '{TENANT['tenant_id']}' ok")

    st, store = _post(hub, admin, "/admin/stores", dict(STORE, tenant_id=TENANT["tenant_id"]))
    if st == 409:
        # already exists — the token below is only shown at creation, so reuse state
        if os.path.exists(STATE):
            print("  store exists; reusing saved ingest token")
            print(json.load(open(STATE)))
            return 0
        print("  ! store exists but no saved token — remove it in the hub to re-mint")
        return 1
    if not _ok(st):
        print(f"  ! store failed ({st})")
        return 1
    store_id = store["id"]
    print(f"  store '{STORE['store_code']}' ok ({store_id})")

    st, tok = _post(hub, admin, f"/admin/stores/{store_id}/ingest-token", {})
    token = tok.get("token", "")
    print("  ingest token minted")

    st, _ = _post(hub, admin, "/admin/suppliers", SUPPLIER)
    print(f"  supplier '{SUPPLIER['supplier_code']}' {'ok' if _ok(st) else 'FAILED '+str(st)}")
    st, _ = _post(hub, admin, "/admin/suppliers/ownership",
                  dict(OWNERSHIP, supplier_code=SUPPLIER["supplier_code"]))
    print(f"  ownership rule {OWNERSHIP['match_type']}={OWNERSHIP['match_value']} "
          f"{'ok' if _ok(st) else 'FAILED '+str(st)}")
    st, _ = _post(hub, admin, "/admin/consent",
                  {"store_id": store_id, "supplier_code": SUPPLIER["supplier_code"],
                   "status": "granted", "reveal_identity": True})
    print(f"  consent granted {'ok' if _ok(st) else 'FAILED '+str(st)}")

    state = {"hub": hub, "store_id": store_id, "store_code": STORE["store_code"],
             "ingest_token": token, "supplier_code": SUPPLIER["supplier_code"]}
    with open(STATE, "w") as f:
        json.dump(state, f, indent=2)

    print("\n" + "=" * 56)
    print("  Hub provisioned. Ingest token (also saved to .hub_state.json):")
    print(f"    {token}")
    print(f"  Portal:  {hub}/portal-app/   login  "
          f"{SUPPLIER['supplier_code']} / {SUPPLIER['password']}")
    print("=" * 56)
    return 0


if __name__ == "__main__":
    sys.exit(main())
