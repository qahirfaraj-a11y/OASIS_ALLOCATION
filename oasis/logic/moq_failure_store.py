"""
MOQ Failure Store
=================
Persistent record of SKUs that failed the Minimum Order Quantity gate in
Smart Ordering, consumed by Transfer Intelligence as pull triggers.

Replaces the old append-only moq_failures.json which grew without bound
(every dashboard render appended duplicates) and never expired entries —
items that failed MOQ once kept generating transfer recommendations long
after they were restocked.

Semantics:
- record_moq_failures(org, items) REPLACES that org's entries ON THIS SOURCE —
  the latest ordering run is the complete truth for that store in that data.
- Entries carry a timestamp and expire after ``max_age_days`` (default 7).
- Entries carry the SOURCE they were recorded against, and only entries from
  the active source are returned (see below).
- Legacy entries without a timestamp are dropped on first load (this
  self-cleans existing bloated files). Entries without a source are not
  triggers once the source is known, and are purged on the next write.
- Writes are atomic (temp file + os.replace) so a concurrent reader never
  sees a half-written file.

WHY FAILURES ARE SCOPED TO THEIR SOURCE
    Entries used to be keyed only by (org_cd, itm_cd). Org codes repeat across
    data sources -- ORG001 in the single-store POS and ORG001 in a multi-store
    network are different stores -- so failures written by an ordering run
    against one database became pull triggers when the transfer scan ran
    against another. Measured on the full-catalogue 5-store network: 26
    transfers existed only because of a foreign file, including milk pulled
    into a store already holding 185.8 days of it, from a donor holding 22.7.

    The source is worked out here, from the same resolution the adapter uses,
    so no call site has to remember to pass it. It is a fingerprint of the
    POS URL with credentials REMOVED BEFORE HASHING: a live URL carries a
    login, and a hash of a password is still a derivative of it. That also
    means rotating a password does not orphan a store's failures.
"""

import hashlib
import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("MoqFailureStore")

DEFAULT_MAX_AGE_DAYS = 7


def _strip_http_credentials(url: str) -> str:
    """``http://user:pass@host/x`` -> ``http://host/x``; anything else unchanged."""
    from urllib.parse import urlsplit, urlunsplit
    try:
        p = urlsplit(url)
    except ValueError:
        return url
    if not (p.username or p.password):
        return url
    host = p.hostname or ""
    if p.port:
        host = f"{host}:{p.port}"
    return urlunsplit((p.scheme, host, p.path, p.query, p.fragment))


def source_fingerprint(ident: str) -> str:
    """A stable, credential-free identity for one data source.

    Database URLs are rebuilt WITHOUT username and password before hashing.
    ``URL.set(username=None, password=None)`` does not do that in SQLAlchemy
    2.0 -- None there means "leave unchanged" -- so the URL is re-created from
    its non-secret parts instead. SQLite paths are normalised, so one file
    spelled with backslashes, forward slashes or a different case on Windows is
    one source.
    """
    text = str(ident or "").strip()
    try:
        from sqlalchemy.engine import URL, make_url
        u = make_url(text)
        db = u.database
        if u.drivername.startswith("sqlite") and db and db != ":memory:":
            db = os.path.normcase(os.path.abspath(db)).replace("\\", "/")
        text = URL.create(drivername=u.drivername, host=u.host, port=u.port,
                          database=db, query=u.query
                          ).render_as_string(hide_password=False)
    except Exception:
        # Not a database URL (an Odoo identity, say): still drop any
        # user:pass@ an http URL might carry.
        text = _strip_http_credentials(text)
    return hashlib.sha256(text.encode("utf-8")).hexdigest()[:16]


def current_source(root: Optional[str] = None) -> Optional[str]:
    """Fingerprint of the data source the ordering and transfer engines read.

    Mirrors oasis.desktop.data.get_adapter: Odoo when OASIS_ERP=odoo, else a
    distinct POS (env or wizard), else OASIS_DB_URL, else the resolved store
    file. None only if resolution itself fails -- then this call is unscoped
    and says so, rather than breaking Smart Ordering or the transfer scan.
    """
    try:
        if (os.getenv("OASIS_ERP") or "").strip().lower() == "odoo":
            url = _strip_http_credentials(
                os.getenv("ODOO_URL", "http://localhost:8069"))
            return source_fingerprint(
                "odoo:" + url + "/" + os.getenv("ODOO_DB", "oasis"))
        from . import db as oasis_db
        if oasis_db.has_distinct_pos():
            return source_fingerprint(oasis_db.get_pos_sqlalchemy_url())
        if os.getenv("OASIS_DB_URL"):
            return source_fingerprint(oasis_db.get_sqlalchemy_url())
        from .onboarding import resolved_db_path
        return source_fingerprint("sqlite:///" + resolved_db_path(root))
    except Exception as e:
        logger.warning("MOQ failure store could not identify the active data "
                       "source (%s); failures are NOT scoped for this call", e)
        return None


def _load_raw(path: str) -> List[dict]:
    if not os.path.exists(path):
        return []
    try:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return data if isinstance(data, list) else []
    except Exception as e:
        logger.warning("Could not read MOQ failure store %s: %s", path, e)
        return []


def _prune(entries: List[dict], max_age_days: int) -> List[dict]:
    """Drop expired and legacy (un-timestamped) entries, dedup by (src, org, itm).

    The source is part of the key: the same item failing on two sources is two
    facts, and de-duplicating across them would let one source's quantity
    silently replace the other's.
    """
    cutoff = datetime.now() - timedelta(days=max_age_days)
    seen = set()
    kept = []
    for e in entries:
        ts_raw = e.get("ts")
        if not ts_raw:
            continue  # legacy append-era entry — drop
        try:
            ts = datetime.fromisoformat(ts_raw)
        except (TypeError, ValueError):
            continue
        if ts < cutoff:
            continue
        key = (e.get("src"), e.get("org_cd"), e.get("itm_cd"))
        if key in seen:
            continue
        seen.add(key)
        kept.append(e)
    return kept


def _write_atomic(path: str, entries: List[dict]) -> None:
    d = os.path.dirname(os.path.abspath(path))
    os.makedirs(d, exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=d, suffix=".tmp")
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            json.dump(entries, f)
        os.replace(tmp, path)
    except Exception:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


def record_moq_failures(
    path: str,
    org_cd: str,
    items: List[dict],
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    source: Optional[str] = None,
) -> int:
    """
    Record the current MOQ failures for one store on one data source,
    replacing that store's previous entries on that source. ``items`` are
    recommendation dicts; item code is read from ``item_code`` or ``itm_cd``.

    ``source`` defaults to the active one (current_source()); pass a
    fingerprint to record against a specific source.

    Returns the number of entries now stored for this org on this source.
    """
    src = source if source is not None else current_source()
    now_iso = datetime.now().isoformat()
    fresh = []
    for it in items:
        itm = str(it.get("item_code", it.get("itm_cd", "")) or "")
        if not itm:
            continue
        fresh.append({
            "org_cd": org_cd,
            "itm_cd": itm,
            "qty": float(it.get("recommended_quantity", 0) or 0),
            "ts": now_iso,
            "src": src,
        })

    others = []
    for e in _prune(_load_raw(path), max_age_days):
        if e.get("org_cd") == org_cd and e.get("src") == src:
            continue  # this store on this source: replaced by `fresh`
        if src is not None and not e.get("src"):
            continue  # unsourced: never a trigger once sources are known
        others.append(e)
    try:
        _write_atomic(path, others + fresh)
    except Exception as e:
        logger.error("Failed to write MOQ failure store %s: %s", path, e)
    return len(fresh)


def load_moq_failures(
    path: str,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
    source: Optional[str] = None,
) -> Dict[str, Dict[str, float]]:
    """Return live (non-expired) MOQ failures for ONE source as {org_cd: {itm_cd: qty}}.

    ``source`` defaults to the active one (current_source()). Entries recorded
    against any other source -- or against none -- are not returned: a
    minimum-order failure in one database is not a reason to move stock in
    another.

    qty is the order quantity that failed the MOQ gate — i.e. how many units
    the store actually needs. Membership tests (``itm in failures[org]``)
    work the same as the old set-based shape.
    """
    src = source if source is not None else current_source()
    result: Dict[str, Dict[str, float]] = {}
    for e in _prune(_load_raw(path), max_age_days):
        if src is not None and e.get("src") != src:
            continue
        org = e.get("org_cd")
        itm = e.get("itm_cd")
        if org and itm:
            result.setdefault(org, {})[itm] = float(e.get("qty", 0) or 0)
    return result
