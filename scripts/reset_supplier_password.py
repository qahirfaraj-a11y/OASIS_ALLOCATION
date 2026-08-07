"""
Reset a supplier portal password via the Hub's database.

Operator tool — not shipped to clients (the release whitelist drops scripts/).

The password is a REQUIRED argument on purpose. An earlier version defaulted to
a known string, which is how a published credential ends up on a live hub; the
same defect the store-side first-run path fixed by making every seeded password
random and unrepeatable. Generate one if you have nothing in mind:

    python scripts/reset_supplier_password.py COKE --generate

Usage:
    python scripts/reset_supplier_password.py <SUPPLIER_CODE> <PASSWORD>
    python scripts/reset_supplier_password.py <SUPPLIER_CODE> --generate
"""
import argparse
import os
import secrets
import sys

# Ensure project root is on path
_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _ROOT not in sys.path:
    sys.path.insert(0, _ROOT)

from oasis_hub.security import hash_password
from oasis_hub.db import session_scope, init_db
from oasis_hub.models import HubSupplier

MIN_LENGTH = 12


def reset(supplier_code: str, new_password: str) -> bool:
    init_db()
    with session_scope() as db:
        supplier = db.query(HubSupplier).filter(
            HubSupplier.supplier_code == supplier_code).first()
        if not supplier:
            print(f"[FAIL] Supplier '{supplier_code}' not found in the hub database.")
            return False
        supplier.password_hash = hash_password(new_password)
        print(f"[OK] Password for supplier '{supplier_code}' has been reset.")
        print(f"  Portal: http://localhost:8700/portal-app/")
        print(f"  Login:  {supplier_code}")
        print("  Send the password over a channel the supplier already trusts; "
              "it is not stored anywhere in plaintext.")
        return True


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__,
                                 formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("supplier_code", help="e.g. COKE")
    ap.add_argument("password", nargs="?",
                    help=f"new password (min {MIN_LENGTH} chars)")
    ap.add_argument("--generate", action="store_true",
                    help="generate a strong random password and print it once")
    args = ap.parse_args()

    if args.generate:
        password = secrets.token_urlsafe(18)
        print(f"[GENERATED] {password}")
    elif args.password:
        password = args.password
    else:
        ap.error("give a password or pass --generate; there is no default")

    if len(password) < MIN_LENGTH:
        ap.error(f"password must be at least {MIN_LENGTH} characters")

    return 0 if reset(args.supplier_code.upper(), password) else 1


if __name__ == "__main__":
    sys.exit(main())
