"""
DHARAM — Demand, Halo, and Revenue Analytics (Pre-Flight Engine)
Chapter 11, Sub-Routine C: The Revenue Engine.

Understands Basket Affinity (Anchor/Attachment relationships).
Prevents "Ghost Demand" — where Attachment sales are artificially suppressed
because the Anchor was stocked out.

Usage:
    python -m oasis.logic.dharam_revenue --data-dir ./oasis/data --nn-path ./neutral_network_export

Output:
    oasis/data/dharam_demand_patch.json
"""

import csv
import json
import os
import logging
import argparse
from collections import defaultdict
from typing import Dict, Any, Set, Tuple

logger = logging.getLogger("OASIS.DHARAM")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

def _load_dharam_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load DHARAM parameters from the central config.

    Resolved via oasis.logic.engines_config, so an install with no tuned
    oasis_engines_config.json picks up the SHIPPED defaults instead of the
    literals below (deep-analysis finding S1).
    """
    from .engines_config import engine_params
    params = engine_params('dharam', data_dir)
    if params:
        return params
    logger.warning("No DHARAM config resolved — falling back to library defaults")
    return {
        "brand_loyalty_factor": 0.5,
        "stockout_fill_rate_threshold": 0.5,
        "max_recovery_multiplier": 2.5,
        "min_affinity_core_count": 2,
        "min_anchor_ads": 0.1
    }

# Minimum occurrences required in edges.csv to consider a link a valid affinity
MIN_AFFINITY_CORE_COUNT = 2
# Minimum ADS to qualify as an Anchor (lowered for graph-weighted discovery)
MIN_ANCHOR_ADS = 0.1


def _resolve_pos_db_path(data_dir: str) -> str:
    """Same POS resolution every console uses (onboarding.resolved_db_path),
    so the availability signal below is measured against whatever database
    this install is actually connected to -- never a hardcoded filename."""
    try:
        from .onboarding import resolved_db_path
        root = os.path.dirname(os.path.dirname(os.path.abspath(data_dir)))
        return resolved_db_path(root)
    except Exception as e:
        logger.warning(f"Could not resolve onboarded POS db ({e}); "
                       f"falling back to {data_dir}/oasis_store.db")
        return os.path.join(data_dir, "oasis_store.db")


def compute_anchor_availability(data_dir: str, config: Dict[str, Any] = None) -> Dict[str, Dict[str, Any]]:
    """Day-level anchor-absence signature -- replaces the broken store_fill_rate gate.

    store_fill_rate/rhapta_fill_rate is 0.0 or blank for every one of the
    23,511 SKU nodes on this install (rhapta_master_metrics.json no longer
    carries live_fill_rate). Treating a missing signal as "0% available"
    silently made every anchor with any graph affinity read as 100% stocked
    out -- DHARAM would have fired on nearly the whole catalogue the moment
    the weight bug below was fixed, which is the mass-false-positive version
    of the asymmetry trap, not a ghost-demand correction. This computes a
    real, weaker, day-level replacement instead: see
    basket_affinity.anchor_day_coverage for what it can and cannot see.

    config['max_recovery_window_days'] (deployed 90, previously dead -- never
    read anywhere) becomes the lookback window: the signal is measured over
    the trailing N days, so a resolved stockout ages out of the correction
    within that window on the next rebuild instead of boosting demand forever.
    """
    if config is None:
        config = {}
    from .basket_affinity import anchor_day_coverage
    import datetime as _dt

    window_days = int(config.get("max_recovery_window_days", 90) or 90)
    min_days_total = 14  # same floor probe_lead_time.py / basket_affinity use
    db_path = _resolve_pos_db_path(data_dir)
    if not os.path.exists(db_path):
        logger.warning(f"[DHARAM] Resolved POS db {db_path} does not exist; "
                       "no anchor-availability signal -- every anchor falls "
                       "back to 'unknown, assume available' (no correction).")
        return {}
    # Anchor the trailing window to the LATEST DATE ACTUALLY IN THE DB, not
    # wall-clock today. A live install syncing daily has max(BILL_DT) ~= today
    # so this changes nothing there; an install with a sync gap, or any
    # historical/offline dataset (every one available on this install has a
    # max date in the past), would otherwise see its window land entirely
    # after the data and silently measure zero SKUs -- exactly what happened
    # against every local POS db here before this fix (see probe).
    try:
        import sqlite3
        conn = sqlite3.connect(db_path, timeout=30.0)
        max_dt = conn.execute("SELECT MAX(BILL_DT) FROM POS_SALES_DTL").fetchone()[0]
        conn.close()
    except Exception:
        max_dt = None
    if max_dt:
        anchor_date = _dt.date.fromisoformat(str(max_dt)[:10])
    else:
        anchor_date = _dt.date.today()
    since = (anchor_date - _dt.timedelta(days=window_days)).isoformat()
    try:
        coverage = anchor_day_coverage(db_path, since=since, min_days_total=min_days_total)
    except Exception as e:
        logger.warning(f"[DHARAM] anchor_day_coverage failed against {db_path}: {e}")
        return {}
    logger.info(f"[DHARAM] Anchor availability measured from {db_path} "
               f"(trailing {window_days}d, min {min_days_total}d history): "
               f"{len(coverage)} SKUs with a usable signal.")
    return coverage


def load_nodes(nn_path: str) -> Dict[str, Dict[str, Any]]:
    """Load all SKU nodes from the neural network as a dict keyed by SKU id."""
    nodes_path = os.path.join(nn_path, "nodes.csv")
    if not os.path.exists(nodes_path):
        logger.error(f"nodes.csv not found at {nodes_path}")
        return {}

    nodes = {}
    with open(nodes_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("type") != "SKU":
                continue
            dept = row.get("department", "GENERAL").strip("[]").strip().upper()
            sku_id = row["id"]
            nodes[sku_id] = {
                "department": dept,
                "price": float(row.get("price", 0) or 0),
                "velocity_ads": float(row.get("velocity_ads", 0) or 0),
                "revenue": float(row.get("revenue", 0) or 0),
                "sales_rank": float(row.get("sales_rank", 99999) or 99999),
                "store_fill_rate": float(row.get("store_fill_rate", row.get("rhapta_fill_rate", 0)) or 0),
                "total_quantity": float(row.get("total_quantity", 0) or 0),
            }

    logger.info(f"Loaded {len(nodes)} SKU nodes.")
    return nodes


def load_edges(nn_path: str, nodes: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Dict[str, int]], Dict[str, Set[str]]]:
    """
    Load edges and build relationship maps:
    1. Affinity Map: SKU → { Target SKU: Count } (based on 'link' frequency)
    2. Substitution graph: SKU → set of substitute SKUs
    """
    edges_path = os.path.join(nn_path, "edges.csv")
    if not os.path.exists(edges_path):
        logger.error(f"edges.csv not found at {edges_path}")
        return {}, {}

    affinity_map: Dict[str, Dict[str, int]] = defaultdict(lambda: defaultdict(int))
    substitution_map: Dict[str, Set[str]] = defaultdict(set)

    with open(edges_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            source = row["source"]
            target = row["target"]
            relation = row["relation"]

            if relation == "link":
                # Ensure both are SKUs in our current node list
                s_id = source.strip()
                t_id = target.strip()
                if s_id in nodes and t_id in nodes:
                    # Honour the co-occurrence weight when present (basket_affinity
                    # emits one row per pair, weight = co-purchase count); fall back
                    # to +1 per row for legacy unweighted link edges.
                    try:
                        w = int(float(row.get("weight", 1) or 1))
                    except (TypeError, ValueError):
                        w = 1
                    w = max(1, w)
                    # Directional anchor -> attachment (Ch. 8.4 Broken Halo:
                    # affinity is not mutual). source is the anchor; do NOT mirror.
                    affinity_map[s_id][t_id] += w
            elif relation == "substitution":
                s_id = source.strip()
                t_id = target.strip()
                if s_id in nodes and t_id in nodes:
                    substitution_map[s_id].add(t_id)
                    substitution_map[t_id].add(s_id)

    logger.info(f"Built affinity map for {len(affinity_map)} SKUs and substitution map for {len(substitution_map)} SKUs.")
    return dict(affinity_map), dict(substitution_map)


def identify_anchors_and_attachments(
    nodes: Dict[str, Dict[str, Any]],
    affinity_map: Dict[str, Dict[str, int]],
    config: Dict[str, Any] = None
) -> Dict[str, Dict[str, int]]:
    """
    Identify Anchor → Attachment relationships using 100% Graph Discovery.
    
    Returns: { anchor_sku: { attachment_sku: weight } }
    """
    if config is None:
        config = {"min_anchor_ads": 0.1, "min_affinity_core_count": 2}
        
    min_anchor_ads = config.get("min_anchor_ads", 0.1)
    min_affinity = config.get("min_affinity_core_count", 2)
    
    anchor_map: Dict[str, Dict[str, int]] = {}

    for sku_id, data in nodes.items():
        # Candidate must be a high-velocity Anchor
        if data["velocity_ads"] < min_anchor_ads:
            continue

        # Check if this Anchor has any graph affinities
        if sku_id not in affinity_map:
            continue

        attachments = {}
        for target_id, weight in affinity_map[sku_id].items():
            if weight >= min_affinity:
                # Filter to active nodes only
                if nodes[target_id]["velocity_ads"] > 0:
                    attachments[target_id] = weight

        if attachments:
            anchor_map[sku_id] = attachments

    logger.info(f"Identified {len(anchor_map)} Anchor SKUs with {sum(len(v) for v in anchor_map.values())} total attachments.")
    return anchor_map


def calculate_ghost_demand_patches(
    nodes: Dict[str, Dict[str, Any]],
    anchor_map: Dict[str, Dict[str, int]],
    substitution_map: Dict[str, Set[str]] = None,
    config: Dict[str, Any] = None,
    availability: Dict[str, Dict[str, Any]] = None,
) -> Dict[str, float]:
    """
    Calculate demand recovery patches using Edge-Weighted Relative Affinity.
    Includes 'Substitution Offset' to prevent over-recovery if alternatives were available.

    `availability` (from compute_anchor_availability) is a day-level sale-
    presence rate per SKU, keyed the same way as `nodes`. It is preferred
    over `nodes[x]["store_fill_rate"]` because that field is 0.0/blank for
    every node on this install and would otherwise read as "0% available"
    for anything with no real signal at all -- see compute_anchor_availability's
    docstring. When neither signal exists for an anchor we assume it WAS
    available (fill_rate 1.0): absence of evidence is not evidence of a
    stockout, and defaulting the other way is what makes a one-directional
    correction into a mass false-positive over-ordering machine.
    """
    patches: Dict[str, float] = {}
    ghost_demand_events = 0
    offset_count = 0
    signal_counts = {"basket_day_coverage": 0, "legacy_store_fill_rate": 0, "unknown_assume_available": 0}
    ads_floor_skipped = 0

    if config is None:
        config = {"brand_loyalty_factor": 0.5, "stockout_fill_rate_threshold": 0.5, "max_recovery_multiplier": 2.5}
    availability = availability or {}

    loyalty_factor = config.get("brand_loyalty_factor", 0.5)
    stockout_threshold = config.get("stockout_fill_rate_threshold", 0.5)
    max_recovery_multiplier = config.get("max_recovery_multiplier", 2.5)
    # Dead config wired in (was declared, never read by any engine): a floor
    # on the ATTACHMENT's own baseline velocity, so a barely-selling SKU
    # cannot pick up a "ghost demand" boost off statistical noise. This also
    # bounds how many lines the correction can ever touch -- part of the
    # asymmetry-trap mitigation (a one-directional correction must be capped,
    # not just multiplied).
    ads_floor = config.get("ghost_demand_ads_floor", 0.0)

    def _availability(sku_id: str) -> float:
        obs = availability.get(sku_id)
        if obs is not None:
            signal_counts["basket_day_coverage"] += 1
            return obs["coverage"]
        legacy = nodes.get(sku_id, {}).get("store_fill_rate", 0.0) or 0.0
        if legacy > 0:
            signal_counts["legacy_store_fill_rate"] += 1
            return legacy
        signal_counts["unknown_assume_available"] += 1
        return 1.0

    for anchor_id, attachments in anchor_map.items():
        anchor = nodes.get(anchor_id)
        if not anchor:
            continue

        fill_rate = _availability(anchor_id)

        if fill_rate < stockout_threshold:
            # Substitution Offset (Chapter 11 Upgrade)
            substitutes = substitution_map.get(anchor_id, [])
            best_sub_fill = 0.0
            for sub_id in substitutes:
                if sub_id in nodes:
                    best_sub_fill = max(best_sub_fill, _availability(sub_id))

            # Composite fill rate: The customer's experience of 'availability' for this need.
            # v1.1 FIX: Use configurable Brand Loyalty factor
            composite_fill_rate = max(fill_rate, best_sub_fill * loyalty_factor)

            if composite_fill_rate >= stockout_threshold:
                # Stock-out is functionally mitigated by substitutes
                offset_count += 1
                continue

            ghost_demand_events += 1
            stockout_severity = 1.0 - composite_fill_rate

            # Base recovery multiplier (for strong links)
            max_recovery_factor = 1.0 + (stockout_severity * 1.5)

            # Weight Scaling
            max_weight = max(attachments.values()) if attachments else 1.0

            for att_id, weight in attachments.items():
                # Relative Affinity = Weight / Max Weight for this anchor
                affinity_ratio = weight / max_weight

                # Scaled recovery: Weak links get less recovery inflation
                recovery_multiplier = min(max_recovery_multiplier, 1.0 + ((max_recovery_factor - 1.0) * affinity_ratio))

                att = nodes.get(att_id)
                # Filter to active/valid nodes
                if not att or att["velocity_ads"] <= 0:
                    continue
                if att["velocity_ads"] < ads_floor:
                    ads_floor_skipped += 1
                    continue

                original_ads = att["velocity_ads"]
                patched_ads = round(original_ads * recovery_multiplier, 4)

                if patched_ads > original_ads * 1.05: # Lower threshold (5%) for precision model
                    if att_id in patches:
                        patches[att_id] = max(patches[att_id], patched_ads)
                    else:
                        patches[att_id] = patched_ads

    logger.info(f"[DHARAM] Detected {ghost_demand_events} Ghost Demand events.")
    logger.info(f"[DHARAM] Substitution Offset: Mitigated {offset_count} events due to substitute availability.")
    logger.info(f"[DHARAM] Availability signal sources: {signal_counts}")
    logger.info(f"[DHARAM] {ads_floor_skipped} attachment lines skipped below ghost_demand_ads_floor={ads_floor}.")
    logger.info(f"[DHARAM] Generated {len(patches)} demand recovery patches.")
    calculate_ghost_demand_patches.last_run_stats = {
        "ghost_demand_events": ghost_demand_events,
        "substitution_offset_count": offset_count,
        "availability_signal_sources": signal_counts,
        "ads_floor_skipped": ads_floor_skipped,
    }
    return patches

def run_dharam(nn_path: str, data_dir: str) -> Dict[str, Any]:
    """
    Execute the DHARAM Revenue Engine.
    
    1. Load neural network graph (nodes + edges).
    2. Identify Anchor/Attachment relationships.
    3. Detect Ghost Demand where Anchors had poor fill rates.
    4. Output demand recovery patches.
    """
    nodes = load_nodes(nn_path)
    if not nodes:
        logger.warning("No nodes loaded. DHARAM cannot execute.")
        return {"patches": 0, "ghost_events": 0}

    affinity_map, substitution_map = load_edges(nn_path, nodes)

    # Step 1: Identify Anchor/Attachment pairs (100% Discovery)
    # Load Config early to pass to anchor identification
    config = _load_dharam_config(data_dir)
    anchor_map = identify_anchors_and_attachments(nodes, affinity_map, config=config)

    # Step 1b: Anchor-absence signature -- replaces the broken store_fill_rate
    # gate (0.0/blank for every node). See compute_anchor_availability.
    pos_db_path = _resolve_pos_db_path(data_dir)
    availability = compute_anchor_availability(data_dir, config=config)

    # Step 2: Calculate Ghost Demand patches (Edge-Weighted with Substitution Offset)
    patches = calculate_ghost_demand_patches(nodes, anchor_map, substitution_map,
                                             config=config, availability=availability)
    run_stats = getattr(calculate_ghost_demand_patches, "last_run_stats", {})

    # Step 3: Build output
    try:
        from .onboarding import is_demo
        pos_provenance = "demo" if is_demo() else "connected"
    except Exception:
        pos_provenance = "unknown"

    output = {
        "demand_patches": patches,
        "stats": {
            "total_nodes_analyzed": len(nodes),
            "total_anchors_identified": len(anchor_map),
            "total_demand_patches": len(patches),
            "stockout_threshold": config.get("stockout_fill_rate_threshold", 0.5),
            "brand_loyalty_factor": config.get("brand_loyalty_factor", 0.5),
            "min_affinity_weight": config.get("min_affinity_core_count", 2),
            "min_anchor_ads": config.get("min_anchor_ads", 0.1),
            "max_recovery_multiplier": config.get("max_recovery_multiplier", 2.5),
            "ghost_demand_ads_floor": config.get("ghost_demand_ads_floor", 0.0),
            "anchors_with_availability_signal": run_stats.get("availability_signal_sources", {}),
            "ghost_demand_events": run_stats.get("ghost_demand_events", 0),
            "substitution_offset_count": run_stats.get("substitution_offset_count", 0),
            "ads_floor_skipped_lines": run_stats.get("ads_floor_skipped", 0),
            "pos_db_used": pos_db_path,
            "pos_db_provenance": pos_provenance,
        },
        "top_patches": sorted(
            [{"sku": k, "patched_ads": v, "original_ads": nodes[k]["velocity_ads"]}
             for k, v in patches.items() if k in nodes],
            key=lambda x: x["patched_ads"] - x["original_ads"],
            reverse=True,
        )[:20],
    }

    # Write output
    output_path = os.path.join(data_dir, "dharam_demand_patch.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    logger.info(f"[DHARAM] Demand patch written to {output_path}")

    return output


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="DHARAM Revenue Engine — Chapter 11 Sub-Routine C")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"), help="Path to oasis/data directory")
    parser.add_argument("--nn-path", default=os.path.join(os.path.dirname(__file__), "..", "..", "neutral_network_export"), help="Path to neural network export")
    args = parser.parse_args()

    result = run_dharam(args.nn_path, args.data_dir)
    print("\n=== DHARAM COMPLETE ===")
    print(f"Demand Patches Generated: {result['stats']['total_demand_patches']}")
    print(f"Anchors Identified: {result['stats']['total_anchors_identified']}")
    if result.get("top_patches"):
        print("\nTop 5 Ghost Demand Recoveries:")
        for p in result["top_patches"][:5]:
            delta = p["patched_ads"] - p["original_ads"]
            print(f"  {p['sku']}: {p['original_ads']:.3f} -> {p['patched_ads']:.3f} (+{delta:.3f} ADS)")
