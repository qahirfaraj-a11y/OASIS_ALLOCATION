"""
Push supplier insight cards from this store to the OASIS Cloud Hub.

Closes the Insight Push loop (SUPPLIER_PORTAL_PLAN.md §4): the on-prem engine
already computes the methodology-grade numbers; ``insight_emitter`` shapes them
into supplier-safe cards; this module ships them.

    python entrypoint.py --mode push-insights

Configuration (env, same style as the rest of OASIS):
    OASIS_HUB_URL           e.g. https://hub.oasis-systems.example
    OASIS_HUB_INGEST_TOKEN  the per-store token the hub issued

Nothing is revealed by pushing: every card stays invisible to the supplier until
the retailer flips that kind on in the hub (default-deny exposure). Sending is
therefore safe to schedule.

Stdlib only for transport (urllib) — no new dependency on a client install.
"""

from __future__ import annotations

import json
import logging
import os
import urllib.error
import urllib.request
from typing import Any, Callable, Dict, List, Optional

from . import insight_emitter as IE

logger = logging.getLogger("OASIS.InsightPush")

DEFAULT_BATCH = 200


class InsightPushError(RuntimeError):
    pass


# ── transport ────────────────────────────────────────────────────────────
def _urllib_poster(url: str, headers: dict, body: dict):
    data = json.dumps(body).encode("utf-8")
    req = urllib.request.Request(url, data=data, headers=headers, method="POST")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return resp.getcode(), json.loads(resp.read().decode() or "{}")
    except urllib.error.HTTPError as e:
        txt = e.read().decode("utf-8", "replace")
        try:
            return e.code, json.loads(txt)
        except ValueError:
            return e.code, {"detail": txt}


def push_cards(cards: List[dict], *, hub_url: Optional[str] = None,
               token: Optional[str] = None,
               poster: Optional[Callable] = None,
               batch_size: int = DEFAULT_BATCH) -> dict:
    """POST cards to /ingest/insights in batches. Returns aggregate counts."""
    hub_url = (hub_url or os.getenv("OASIS_HUB_URL", "")).rstrip("/")
    token = token or os.getenv("OASIS_HUB_INGEST_TOKEN", "")
    if not hub_url or not token:
        raise InsightPushError(
            "OASIS_HUB_URL and OASIS_HUB_INGEST_TOKEN must be set to push insights")
    if not cards:
        return {"accepted": 0, "duplicates": 0, "batches": 0}

    post = poster or _urllib_poster
    headers = {"Authorization": f"Bearer {token}",
               "Content-Type": "application/json"}
    accepted = duplicates = batches = 0
    for i in range(0, len(cards), max(1, batch_size)):
        chunk = cards[i:i + batch_size]
        status, payload = post(f"{hub_url}/ingest/insights", headers,
                               {"insights": chunk})
        if status != 200:
            raise InsightPushError(
                f"hub rejected insights ({status}): {payload.get('detail')}")
        accepted += payload.get("accepted", 0)
        duplicates += payload.get("duplicates", 0)
        batches += 1
    logger.info("Insight push: %d accepted, %d duplicate, %d batch(es)",
                accepted, duplicates, batches)
    return {"accepted": accepted, "duplicates": duplicates, "batches": batches}


# ── card assembly from the on-prem engine ────────────────────────────────
def build_cards(data_dir: str, *, period: str = "",
                suppliers: Optional[List[str]] = None) -> List[dict]:
    """Compute supplier-safe cards from whatever this store has available.

    Degrades gracefully: a store without a scorecard or MANDE run simply emits
    fewer kinds rather than failing. ``period`` becomes part of source_ref so a
    daily/weekly run is idempotent within its period.
    """
    cards: List[dict] = []
    wanted = set(suppliers or [])

    # 1. Reliability — from the LATA/supplier scorecard.
    try:
        from .supplier_scorecard import load_patterns, scorecard_rows
        for row in scorecard_rows(load_patterns(data_dir)):
            code = row.get("supplier") or row.get("supplier_cd")
            if not code or (wanted and code not in wanted):
                continue
            cards.append(IE.reliability_card(
                code, row, source_ref=f"rel:{period}" if period else None))
    except Exception as e:                       # never let one source break the run
        logger.warning("reliability cards skipped: %s", e)

    # 2. SEI — from the MANDE purge report, when present.
    try:
        path = os.path.join(data_dir, "mande_purge_report.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                report = json.load(f) or {}
            rows = report if isinstance(report, list) else report.get("suppliers", [])
            for row in rows:
                code = row.get("supplier") or row.get("supplier_cd")
                if not code or (wanted and code not in wanted):
                    continue
                cards.append(IE.sei_card(
                    code, row, source_ref=f"sei:{period}" if period else None))
    except Exception as e:
        logger.warning("SEI cards skipped: %s", e)

    logger.info("Built %d insight card(s) from %s", len(cards), data_dir)
    return cards


def run(data_dir: str, *, period: str = "", hub_url: Optional[str] = None,
        token: Optional[str] = None, dry_run: bool = False) -> Dict[str, Any]:
    """Build + push in one call (the --mode push-insights entry point)."""
    cards = build_cards(data_dir, period=period)
    if dry_run:
        return {"built": len(cards), "pushed": False, "cards": cards}
    result = push_cards(cards, hub_url=hub_url, token=token)
    result["built"] = len(cards)
    return result
