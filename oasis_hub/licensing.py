"""
Online license issuer — the hub-side of OASIS licensing.

The on-prem ``OfflineLicenseManager`` already knows how to sign and verify keys;
the only thing that made issuing a "vendor-only" operation was possession of
``OASIS_LICENSE_SALT``. The hub IS the vendor: the salt lives in the hub
environment and never leaves it. This module wraps the manager to:

  * mint a signed key (in memory) and persist it to the hub ledger (hub_license),
  * revoke a tenant's active keys,
  * verify a presented key body against the salt,

so a client can request/renew a license over the network and receive only the
signed key file — exactly what ``oasis_license.key`` expects on disk.
"""

import json
import logging
from datetime import date, datetime
from typing import List, Optional

from sqlalchemy.orm import Session

from oasis.logic.license_manager import (
    OfflineLicenseManager, KNOWN_MODULES, BUNDLES,
)
from .models import HubLicense

logger = logging.getLogger("OASIS.Hub.Licensing")


class LicensingError(Exception):
    """Raised for bad issue requests (unknown module, bad salt, bad date)."""


def _resolve_modules(modules: Optional[List[str]], bundle: Optional[str]) -> List[str]:
    if bundle:
        if bundle not in BUNDLES:
            raise LicensingError(f"unknown bundle '{bundle}' "
                                 f"(choose from {', '.join(BUNDLES)})")
        return list(BUNDLES[bundle])
    if not modules:
        raise LicensingError("either 'modules' or 'bundle' is required")
    unknown = [m for m in modules if m not in KNOWN_MODULES]
    if unknown:
        raise LicensingError(f"unknown module(s): {', '.join(unknown)}")
    mods = list(dict.fromkeys(modules))         # dedup, keep order
    if "core" not in mods:                       # core is mandatory
        mods.insert(0, "core")
    return mods


def issue_license(
    db: Session,
    tenant_id: str,
    expiry_date: str,
    *,
    modules: Optional[List[str]] = None,
    bundle: Optional[str] = None,
    issued_by: str = "hub-admin",
) -> HubLicense:
    """Mint a signed key, record it in the ledger, supersede prior active keys."""
    mods = _resolve_modules(modules, bundle)
    mgr = OfflineLicenseManager()
    try:
        key = mgr.build_key(tenant_id, mods, expiry_date)
    except RuntimeError as e:                     # missing salt
        raise LicensingError(str(e)) from e
    except ValueError as e:                       # bad date format
        raise LicensingError(f"invalid expiry_date (want YYYY-MM-DD): {e}") from e

    # a new key for a tenant supersedes its previous active keys
    (db.query(HubLicense)
       .filter(HubLicense.tenant_id == tenant_id, HubLicense.revoked.is_(False))
       .update({"revoked": True, "revoked_at": datetime.utcnow()}))

    rec = HubLicense(
        tenant_id=tenant_id,
        modules=",".join(mods),
        expiry_date=expiry_date,
        key_json=json.dumps(key, indent=2),
        issued_by=issued_by,
    )
    db.add(rec)
    db.flush()
    logger.info("Issued license %s for %s [%s] exp %s",
                rec.id, tenant_id, ",".join(mods), expiry_date)
    return rec


def revoke_tenant(db: Session, tenant_id: str) -> int:
    """Revoke all active licenses for a tenant. Returns the count revoked."""
    n = (db.query(HubLicense)
           .filter(HubLicense.tenant_id == tenant_id, HubLicense.revoked.is_(False))
           .update({"revoked": True, "revoked_at": datetime.utcnow()}))
    logger.info("Revoked %d active license(s) for %s", n, tenant_id)
    return n


def verify_key_body(key_body: dict) -> dict:
    """Verify a presented key against the hub salt, module by module.

    Returns {tenant, expiry, modules: {name: ok_bool}, valid_overall, expired}.
    Mirrors the client-side check so a client can pre-flight a key with the hub.
    """
    mgr = OfflineLicenseManager()
    tenant = key_body.get("tenant_id", "")
    expiry = key_body.get("expiry_date", "")
    mods = key_body.get("authorized_modules", {}) or {}
    per_module = {
        name: (sig == mgr._fingerprint(tenant, name, expiry))
        for name, sig in mods.items()
    }
    expired = False
    try:
        expired = date.fromisoformat(expiry) < date.today()
    except ValueError:
        expired = True
    return {
        "tenant": tenant,
        "expiry": expiry,
        "modules": per_module,
        "valid_overall": bool(per_module) and all(per_module.values()) and not expired,
        "expired": expired,
    }
