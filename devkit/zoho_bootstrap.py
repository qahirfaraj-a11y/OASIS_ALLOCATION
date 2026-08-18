"""Turn a Zoho self-client grant code into the four values OASIS needs.

DEV TOOLING — lives in devkit/, never ships to a client.

The grant code from api-console.zoho.com expires in MINUTES and is single-use,
so doing this by hand with curl usually means generating it twice. This does the
exchange, then immediately calls /organizations so the org id comes from Zoho
rather than being copied off a settings screen.

RUN IT YOURSELF. The secret and the refresh token stay on your machine — they
are written straight to .env (already gitignored) and never printed in full.

    python devkit/zoho_bootstrap.py \
        --client-id 1000.XXXX --client-secret YYYY --code 1000.ZZZZ [--dc com]

Then, to let the conformance battery run:

    OASIS_TEST_ZOHO=1 python -m pytest tests/test_erp_conformance.py -q
"""

from __future__ import annotations

import argparse
import os
import sys

DATA_CENTRES = {
    "com": ("https://www.zohoapis.com", "https://accounts.zoho.com"),
    "eu": ("https://www.zohoapis.eu", "https://accounts.zoho.eu"),
    "in": ("https://www.zohoapis.in", "https://accounts.zoho.in"),
    "au": ("https://www.zohoapis.com.au", "https://accounts.zoho.com.au"),
    "jp": ("https://www.zohoapis.jp", "https://accounts.zoho.jp"),
    "ca": ("https://www.zohoapis.ca", "https://accounts.zohocloud.ca"),
}

ENV_PATH = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                        ".env")


def _mask(secret: str) -> str:
    """Enough to recognise it, not enough to use it."""
    s = str(secret or "")
    return f"{s[:8]}…{s[-4:]}" if len(s) > 14 else "…"


def _load_env_file() -> dict:
    """Read .env, then let real environment variables win.

    Hand-parsed rather than pulling in python-dotenv: this is one file of
    ``KEY=value`` lines and a client install should not gain a dependency for
    a dev-only helper.
    """
    values = {}
    if os.path.exists(ENV_PATH):
        with open(ENV_PATH, "r", encoding="utf-8") as fh:
            for line in fh:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                k, _, v = line.partition("=")
                values[k.strip()] = v.strip().strip('"').strip("'")
    for k in ("ZOHO_CLIENT_ID", "ZOHO_CLIENT_SECRET", "ZOHO_GRANT_CODE",
              "ZOHO_DC"):
        if os.getenv(k):
            values[k] = os.environ[k]
    return values


def main() -> int:
    import requests

    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--client-id")
    p.add_argument("--client-secret")
    p.add_argument("--code",
                   help="grant token from the API console's Generate Code tab "
                        "(expires in minutes, single use)")
    p.add_argument("--dc", choices=sorted(DATA_CENTRES),
                   help="data centre of the Zoho account (default: com)")
    p.add_argument("--write-env", action="store_true", default=True)
    args = p.parse_args()

    # Fall back to .env / the environment, so the three secrets can be supplied
    # in a gitignored file instead of on a command line — a command line ends up
    # in shell history, and pasting one into a chat transcript keeps it there.
    seeded = _load_env_file()
    args.client_id = args.client_id or seeded.get("ZOHO_CLIENT_ID")
    args.client_secret = args.client_secret or seeded.get("ZOHO_CLIENT_SECRET")
    args.code = args.code or seeded.get("ZOHO_GRANT_CODE")
    args.dc = args.dc or seeded.get("ZOHO_DC") or "com"

    missing = [n for n, v in (("ZOHO_CLIENT_ID", args.client_id),
                              ("ZOHO_CLIENT_SECRET", args.client_secret),
                              ("ZOHO_GRANT_CODE", args.code)) if not v]
    if missing:
        print("Missing: " + ", ".join(missing))
        print(f"\nPut them in {ENV_PATH} (gitignored), one per line:\n")
        for n in missing:
            print(f"    {n}=...")
        print("\n  ZOHO_GRANT_CODE is the short-lived code from the API "
              "console's Generate Code tab.")
        print("  Add ZOHO_DC=eu (or in/au/ca/jp) if the account is not on .com.")
        print("\nThen re-run:  python devkit/zoho_bootstrap.py")
        return 2
    if args.dc not in DATA_CENTRES:
        print(f"unknown data centre {args.dc!r}; expected one of "
              f"{sorted(DATA_CENTRES)}")
        return 2

    api_base, accounts = DATA_CENTRES[args.dc]

    print(f"1. exchanging the grant code at {accounts} …")
    r = requests.post(f"{accounts}/oauth/v2/token", params={
        "grant_type": "authorization_code",
        "client_id": args.client_id,
        "client_secret": args.client_secret,
        "code": args.code,
    }, timeout=30)
    body = r.json() if r.headers.get("content-type", "").startswith("application/json") else {}
    # Zoho answers a refused exchange with HTTP 200 and an "error" key, so the
    # status code alone would report success.
    if r.status_code != 200 or "refresh_token" not in body:
        print(f"   FAILED: {body or r.text[:300]}")
        if str(body.get("error")) == "invalid_code":
            print("   -> the grant code has expired or was already used. "
                  "Generate a fresh one and re-run within the validity window.")
        return 1
    refresh = body["refresh_token"]
    access = body["access_token"]
    print(f"   refresh token obtained: {_mask(refresh)}")

    print(f"2. listing organisations at {api_base} …")
    r2 = requests.get(f"{api_base}/inventory/v1/organizations",
                      headers={"Authorization": f"Zoho-oauthtoken {access}"},
                      timeout=30)
    orgs = (r2.json() or {}).get("organizations") or []
    if not orgs:
        print(f"   no organisations returned: {r2.text[:300]}")
        print("   -> the scope may be wrong. It must be "
              "ZohoInventory.FullAccess.all, and the account needs at least "
              "one Zoho Inventory organisation.")
        return 1
    for o in orgs:
        flag = "  <- default" if o.get("is_default_org") else ""
        print(f"   {o.get('organization_id')}  {o.get('name')}  "
              f"[{o.get('currency_code')}]{flag}")
    org_id = str(next((o for o in orgs if o.get("is_default_org")), orgs[0])
                 .get("organization_id"))

    lines = [
        f"ZOHO_CLIENT_ID={args.client_id}",
        f"ZOHO_CLIENT_SECRET={args.client_secret}",
        f"ZOHO_REFRESH_TOKEN={refresh}",
        f"ZOHO_ORG_ID={org_id}",
        f"ZOHO_DC={args.dc}",
        "OASIS_TEST_ZOHO=1",
    ]
    if args.write_env:
        existing = ""
        if os.path.exists(ENV_PATH):
            with open(ENV_PATH, "r", encoding="utf-8") as fh:
                existing = "".join(l for l in fh
                                   if not l.split("=")[0].strip().startswith(
                                       ("ZOHO_", "OASIS_TEST_ZOHO")))
        with open(ENV_PATH, "w", encoding="utf-8") as fh:
            fh.write(existing.rstrip("\n") + ("\n" if existing.strip() else ""))
            fh.write("\n".join(lines) + "\n")
        print(f"\n3. written to {ENV_PATH} (gitignored). "
              f"Using organisation {org_id}.")
    print("\nNow run:  OASIS_TEST_ZOHO=1 python -m pytest "
          "tests/test_erp_conformance.py -q")
    return 0


if __name__ == "__main__":
    sys.exit(main())
