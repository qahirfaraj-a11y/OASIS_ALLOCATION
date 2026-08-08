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

    # 3. Velocity — per-SKU sell-through for supplier products.
    try:
        for fname in ("velocity_report.json", "sku_velocity.json"):
            path = os.path.join(data_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                items_by_supp = data if isinstance(data, dict) else {}
                for code, rows in items_by_supp.items():
                    if wanted and code not in wanted:
                        continue
                    if isinstance(rows, list) and rows:
                        cards.append(IE.velocity_card(
                            code, rows, source_ref=f"vel:{period}" if period else None))
                break
    except Exception as e:
        logger.warning("Velocity cards skipped: %s", e)

    # 4. Halo — basket affinity anchor-attachment pairs.
    try:
        for fname in ("basket_affinity.json", "halo_pairs.json"):
            path = os.path.join(data_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                pairs_by_supp = data if isinstance(data, dict) else {}
                for code, pairs in pairs_by_supp.items():
                    if wanted and code not in wanted:
                        continue
                    if isinstance(pairs, list) and pairs:
                        cards.append(IE.halo_card(
                            code, pairs, source_ref=f"halo:{period}" if period else None))
                break
    except Exception as e:
        logger.warning("Halo cards skipped: %s", e)

    # 5. Reorder — forward-looking order lines.
    try:
        path = os.path.join(data_dir, "reorder_lines.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            lines_by_supp = data if isinstance(data, dict) else {}
            for code, lines in lines_by_supp.items():
                if wanted and code not in wanted:
                    continue
                if isinstance(lines, list) and lines:
                    cards.append(IE.reorder_card(
                        code, lines, source_ref=f"reorder:{period}" if period else None))
    except Exception as e:
        logger.warning("Reorder cards skipped: %s", e)

    # 6. Broken Halo — DHARAM broken affinity alerts.
    try:
        for fname in ("dharam_demand_patch.json", "broken_halo.json"):
            path = os.path.join(data_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                breaks_by_supp = data.get("broken_halos", {}) if isinstance(data, dict) else {}
                if not breaks_by_supp and isinstance(data, dict):
                    breaks_by_supp = data
                for code, breaks in breaks_by_supp.items():
                    if wanted and code not in wanted:
                        continue
                    if isinstance(breaks, list) and breaks:
                        cards.append(IE.broken_halo_card(
                            code, breaks, source_ref=f"bk_halo:{period}" if period else None))
                break
    except Exception as e:
        logger.warning("Broken Halo cards skipped: %s", e)

    # 7. Archetype — demand shape distribution.
    try:
        path = os.path.join(data_dir, "archetype_mix.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            mix_by_supp = data if isinstance(data, dict) else {}
            for code, mix in mix_by_supp.items():
                if wanted and code not in wanted:
                    continue
                if isinstance(mix, list) and mix:
                    cards.append(IE.archetype_card(
                        code, mix, source_ref=f"arch:{period}" if period else None))
    except Exception as e:
        logger.warning("Archetype cards skipped: %s", e)

    # 8. Capital Efficiency — relative index per category.
    try:
        path = os.path.join(data_dir, "capital_efficiency.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            stats_by_supp = data if isinstance(data, dict) else {}
            for code, stats in stats_by_supp.items():
                if wanted and code not in wanted:
                    continue
                if isinstance(stats, dict) and stats:
                    cards.append(IE.capital_efficiency_card(
                        code, stats, source_ref=f"capeff:{period}" if period else None))
    except Exception as e:
        logger.warning("Capital efficiency cards skipped: %s", e)

    # 9. Net Capital Position (NCP) — credit terms vs DIO.
    try:
        path = os.path.join(data_dir, "ncp_report.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            ncp_by_supp = data if isinstance(data, dict) else {}
            for code, ncp_val in ncp_by_supp.items():
                if wanted and code not in wanted:
                    continue
                if isinstance(ncp_val, dict) and ncp_val:
                    cards.append(IE.ncp_card(
                        code, ncp_val, source_ref=f"ncp:{period}" if period else None))
    except Exception as e:
        logger.warning("NCP cards skipped: %s", e)

    # 10. Cannibalization — range substitution overlap.
    try:
        path = os.path.join(data_dir, "cannibalization.json")
        if os.path.exists(path):
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f) or {}
            rows_by_supp = data if isinstance(data, dict) else {}
            for code, rows in rows_by_supp.items():
                if wanted and code not in wanted:
                    continue
                if isinstance(rows, list) and rows:
                    cards.append(IE.cannibalization_card(
                        code, rows, source_ref=f"cannibal:{period}" if period else None))
    except Exception as e:
        logger.warning("Cannibalization cards skipped: %s", e)

    # 11. Quality — Supplier Quality Score (Q_s) from the SQS store.
    try:
        for fname in ("supplier_quality_scores_2025.json", "supplier_quality_scores.json"):
            path = os.path.join(data_dir, fname)
            if os.path.exists(path):
                with open(path, "r", encoding="utf-8") as f:
                    data = json.load(f) or {}
                stats_by_supp = data if isinstance(data, dict) else {}
                for code, stats in stats_by_supp.items():
                    if wanted and code not in wanted:
                        continue
                    if isinstance(stats, dict) and stats:
                        cards.append(IE.quality_card(
                            code, stats, source_ref=f"quality:{period}" if period else None))
                break
    except Exception as e:
        logger.warning("Quality cards skipped: %s", e)

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
