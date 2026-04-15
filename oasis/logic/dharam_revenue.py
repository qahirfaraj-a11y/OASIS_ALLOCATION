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
from typing import Dict, List, Any, Set, Tuple

logger = logging.getLogger("OASIS.DHARAM")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

def _load_dharam_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load DHARAM parameters from the central config."""
    path = os.path.join(data_dir, 'oasis_engines_config.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config.get('engines', {}).get('dharam', {})
        except Exception as e:
            logger.warning(f"Failed to load DHARAM config: {e}")
    # Default Fallbacks
    return {
        "brand_loyalty_factor": 0.5,
        "stockout_fill_rate_threshold": 0.5,
        "max_recovery_multiplier": 2.5
    }

# Minimum occurrences required in edges.csv to consider a link a valid affinity
MIN_AFFINITY_CORE_COUNT = 2
# Minimum ADS to qualify as an Anchor (lowered for graph-weighted discovery)
MIN_ANCHOR_ADS = 0.1


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
                "rhapta_fill_rate": float(row.get("rhapta_fill_rate", 0) or 0),
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
                    affinity_map[s_id][t_id] += 1
                    affinity_map[t_id][s_id] += 1 # Bi-directional for affinity
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
) -> Dict[str, Dict[str, int]]:
    """
    Identify Anchor → Attachment relationships using 100% Graph Discovery.
    
    Returns: { anchor_sku: { attachment_sku: weight } }
    """
    anchor_map: Dict[str, Dict[str, int]] = {}

    for sku_id, data in nodes.items():
        # Candidate must be a high-velocity Anchor
        if data["velocity_ads"] < MIN_ANCHOR_ADS:
            continue

        # Check if this Anchor has any graph affinities
        if sku_id not in affinity_map:
            continue

        attachments = {}
        for target_id, weight in affinity_map[sku_id].items():
            if weight >= MIN_AFFINITY_CORE_COUNT:
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
    config: Dict[str, Any] = None
) -> Dict[str, float]:
    """
    Calculate demand recovery patches using Edge-Weighted Relative Affinity.
    Includes 'Substitution Offset' to prevent over-recovery if alternatives were available.
    """
    patches: Dict[str, float] = {}
    ghost_demand_events = 0
    offset_count = 0
    
    if config is None:
        config = {"brand_loyalty_factor": 0.5, "stockout_fill_rate_threshold": 0.5, "max_recovery_multiplier": 2.5}
    
    loyalty_factor = config.get("brand_loyalty_factor", 0.5)
    stockout_threshold = config.get("stockout_fill_rate_threshold", 0.5)
    max_recovery_multiplier = config.get("max_recovery_multiplier", 2.5)

    for anchor_id, attachments in anchor_map.items():
        anchor = nodes.get(anchor_id)
        if not anchor:
            continue

        fill_rate = anchor["rhapta_fill_rate"]

        if fill_rate < stockout_threshold:
            # Substitution Offset (Chapter 11 Upgrade)
            substitutes = substitution_map.get(anchor_id, [])
            best_sub_fill = 0.0
            for sub_id in substitutes:
                sub_node = nodes.get(sub_id)
                if sub_node:
                    best_sub_fill = max(best_sub_fill, sub_node.get("rhapta_fill_rate", 0.0))
            
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

                original_ads = att["velocity_ads"]
                patched_ads = round(original_ads * recovery_multiplier, 4)

                if patched_ads > original_ads * 1.05: # Lower threshold (5%) for precision model
                    if att_id in patches:
                        patches[att_id] = max(patches[att_id], patched_ads)
                    else:
                        patches[att_id] = patched_ads

    logger.info(f"[DHARAM] Detected {ghost_demand_events} Ghost Demand events.")
    logger.info(f"[DHARAM] Substitution Offset: Mitigated {offset_count} events due to substitute availability.")
    logger.info(f"[DHARAM] Generated {len(patches)} demand recovery patches.")
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
    anchor_map = identify_anchors_and_attachments(nodes, affinity_map)

    # Load Config
    config = _load_dharam_config(data_dir)

    # Step 2: Calculate Ghost Demand patches (Edge-Weighted with Substitution Offset)
    patches = calculate_ghost_demand_patches(nodes, anchor_map, substitution_map, config=config)

    # Step 3: Build output
    output = {
        "demand_patches": patches,
        "stats": {
            "total_nodes_analyzed": len(nodes),
            "total_anchors_identified": len(anchor_map),
            "total_demand_patches": len(patches),
            "stockout_threshold": config.get("stockout_fill_rate_threshold", 0.5),
            "brand_loyalty_factor": config.get("brand_loyalty_factor", 0.5),
            "min_affinity_weight": MIN_AFFINITY_CORE_COUNT,
            "max_recovery_multiplier": config.get("max_recovery_multiplier", 2.5),
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
    print(f"\n=== DHARAM COMPLETE ===")
    print(f"Demand Patches Generated: {result['stats']['total_demand_patches']}")
    print(f"Anchors Identified: {result['stats']['total_anchors_identified']}")
    if result.get("top_patches"):
        print(f"\nTop 5 Ghost Demand Recoveries:")
        for p in result["top_patches"][:5]:
            delta = p["patched_ads"] - p["original_ads"]
            print(f"  {p['sku']}: {p['original_ads']:.3f} -> {p['patched_ads']:.3f} (+{delta:.3f} ADS)")
