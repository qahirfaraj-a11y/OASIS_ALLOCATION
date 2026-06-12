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
- record_moq_failures(org, items) REPLACES that org's entries — the latest
  ordering run is the complete truth for that store.
- Entries carry a timestamp and expire after ``max_age_days`` (default 7).
- Legacy entries without a timestamp are dropped on first load (this
  self-cleans existing bloated files).
- Writes are atomic (temp file + os.replace) so a concurrent reader never
  sees a half-written file.
"""

import json
import logging
import os
import tempfile
from datetime import datetime, timedelta
from typing import Dict, List

logger = logging.getLogger("MoqFailureStore")

DEFAULT_MAX_AGE_DAYS = 7


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
    """Drop expired and legacy (un-timestamped) entries, dedup by (org, itm)."""
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
        key = (e.get("org_cd"), e.get("itm_cd"))
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
) -> int:
    """
    Record the current MOQ failures for one store, replacing that store's
    previous entries. ``items`` are recommendation dicts; item code is read
    from ``item_code`` or ``itm_cd``.

    Returns the number of entries now stored for this org.
    """
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
        })

    others = [e for e in _prune(_load_raw(path), max_age_days)
              if e.get("org_cd") != org_cd]
    try:
        _write_atomic(path, others + fresh)
    except Exception as e:
        logger.error("Failed to write MOQ failure store %s: %s", path, e)
    return len(fresh)


def load_moq_failures(
    path: str,
    max_age_days: int = DEFAULT_MAX_AGE_DAYS,
) -> Dict[str, Dict[str, float]]:
    """Return live (non-expired) MOQ failures as {org_cd: {itm_cd: qty}}.

    qty is the order quantity that failed the MOQ gate — i.e. how many units
    the store actually needs. Membership tests (``itm in failures[org]``)
    work the same as the old set-based shape.
    """
    result: Dict[str, Dict[str, float]] = {}
    for e in _prune(_load_raw(path), max_age_days):
        org = e.get("org_cd")
        itm = e.get("itm_cd")
        if org and itm:
            result.setdefault(org, {})[itm] = float(e.get("qty", 0) or 0)
    return result
