"""
OASIS Cloud Hub
===============
The server-side data plane behind on-premise OASIS installs. It is a *separate*
deployable from the client-facing ``oasis`` package and is never shipped in a
client release ZIP.

Responsibilities (built incrementally):
  * **Licensing** — the online license issuer. The signing salt
    (``OASIS_LICENSE_SALT``) lives ONLY here; clients receive signed key files,
    never the salt. See ``oasis_hub.licensing``.
  * **Ingestion** — stores push opt-in stock-movement telemetry with a
    per-store token. See ``oasis_hub.routers.ingest``.
  * **Retail Central Intelligence portal** — suppliers log in and see movement
    of ONLY their own products, in ONLY the stores that consented to share.
    The privacy contract is enforced in ``oasis_hub.visibility``.
  * **Marketplace connectors** (Odoo first) plug into ingestion — a later pass.

Configuration (all via environment):
  OASIS_HUB_DB_URL       hub database URL (default sqlite:///oasis_hub.db)
  OASIS_LICENSE_SALT     license signing salt (issuing requires it)
  OASIS_HUB_ADMIN_KEY    admin API key (provisioning + license issuing)
  OASIS_HUB_TOKEN_SECRET HMAC secret for supplier session tokens
"""

__all__ = ["__version__"]
__version__ = "0.1.0"
