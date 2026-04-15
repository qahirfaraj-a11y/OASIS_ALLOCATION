"""
AMIT — Assortment & Margin Integration Tool (Pre-Flight Engine)
Chapter 11, Sub-Routine A: The Gatekeeper.

Prevents "Parasite" suppliers and "Cannibal" SKUs from re-infecting the store.
Enforces One-In, One-Out rule based on GMROI ranking per department.

Usage:
    python -m oasis.logic.amit_gatekeeper --data-dir ./oasis/data --nn-path ./neutral_network_export

Output:
    oasis/data/amit_enforcement.json
"""

import csv
import json
import os
import logging
import argparse
from collections import defaultdict
from typing import Dict, List, Any

logger = logging.getLogger("OASIS.AMIT")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

# Default department SKU caps (can be overridden via config)
# These represent the maximum healthy assortment width per department
# at a "Standard Supermarket" baseline (10M KES)
DEFAULT_DEPT_CAPS_BASELINE = {
    "WINES": 120,
    "SPIRITS": 80,
    "BEER": 100,
    "DIWALI ITEMS": 30,
    "PARTY ITEMS": 40,
    "AIR FRESHNERS": 50,
    "BISCUITS": 80,
    "SNACKS": 100,
    "CONFECTIONERY": 120,
    "BEVERAGES": 150,
    "CEREALS": 60,
    "COOKING OIL": 40,
    "RICE": 30,
    "SUGAR": 20,
    "FLOUR": 30,
    "FRESH MILK": 40,
    "DAIRY": 60,
    "BREAD": 30,
    "BAKERY": 50,
    "HOUSEHOLD": 150,
    "TOILETRIES": 150,
    "DETERGENT": 80,
    "BABY CARE": 60,
    "STATIONERY": 50,
    "COSMETICS": 100,
    "PET FOOD": 30,
}

# Fallback cap for departments not explicitly listed
DEFAULT_CAP_FALLBACK_BASELINE = 50
# Minimum floor cap to prevent total category erasure
MIN_DEPT_CAP_FLOOR = 5
# Reference budget for baseline caps (10M KES)
BASELINE_BUDGET = 10_000_000


def load_nodes(nn_path: str) -> List[Dict[str, Any]]:
    """Load all SKU nodes from the neural network export."""
    nodes_path = os.path.join(nn_path, "nodes.csv")
    if not os.path.exists(nodes_path):
        logger.error(f"nodes.csv not found at {nodes_path}")
        return []

    nodes = []
    with open(nodes_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("type") != "SKU":
                continue
            # Clean department name (remove [[ ]] wrappers)
            dept = row.get("department", "GENERAL").strip("[]").strip().upper()
            
            nodes.append({
                "id": row["id"],
                "department": dept,
                "supplier": row.get("supplier", "Unknown").strip("[]").strip().upper(),
                "price": float(row.get("price", 0) or 0),
                "margin_pct": float(row.get("margin_pct", 0) or 0),
                "revenue": float(row.get("revenue", 0) or 0),
                "gross_profit": float(row.get("gross_profit", 0) or 0),
                "sales_rank": float(row.get("sales_rank", 99999) or 99999),
                "velocity_ads": float(row.get("velocity_ads", 0) or 0),
                "total_quantity": float(row.get("total_quantity", 0) or 0),
                "rhapta_fill_rate": float(row.get("rhapta_fill_rate", 0) or 0),
            })

    logger.info(f"Loaded {len(nodes)} SKU nodes from neural network.")
    return nodes


def load_lata_patterns(data_dir: str) -> Dict[str, float]:
    """
    Search for and load LATA supplier patterns from all available versions.
    Returns: Mapping of { Supplier Name: lata_variance_multiplier }
    """
    patterns_files = [f for f in os.listdir(data_dir) if "supplier_patterns" in f and f.endswith(".json")]
    if not patterns_files:
        logger.warning("No LATA patterns found in data directory.")
        return {}

    # Sort files so we process them in a predictable order (e.g. (2) after (1))
    patterns_files.sort()
    
    multipliers = {}
    for fname in patterns_files:
        path = os.path.join(data_dir, fname)
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = json.load(f)
            
            count = 0
            for supplier, info in data.items():
                if isinstance(info, dict):
                    # Prioritize the LATA-specific multiplier if present
                    if "lata_variance_multiplier" in info:
                        multipliers[supplier.upper()] = float(info["lata_variance_multiplier"])
                        count += 1
            if count > 0:
                logger.info(f"Loaded {count} LATA multipliers from {fname}")
        except Exception as e:
            logger.error(f"Failed to load {fname}: {e}")
    
    return multipliers


def calculate_gmroi(node: Dict[str, Any], lata_multiplier: float = 1.0) -> float:
    """
    GMROI = Gross Margin Return on Inventory Investment
    Formula: Gross Profit / (30-day Average Inventory Cost * LATA Reliability Multiplier)
    
    If gross_profit is zero but we have margin_pct and revenue,
    we derive it: gross_profit = revenue * (margin_pct / 100)
    """
    gross_profit = node["gross_profit"]
    if gross_profit <= 0 and node["revenue"] > 0 and node["margin_pct"] > 0:
        gross_profit = node["revenue"] * (node["margin_pct"] / 100.0)

    # Average inventory cost proxy (30-day holding cost)
    # LATA Integration: Unreliable suppliers (multiplier > 1.0) increase the cost debt.
    avg_inventory_cost = (node["price"] * max(node["velocity_ads"], 0.001) * 30.0) * lata_multiplier

    if avg_inventory_cost <= 0:
        return 0.0

    return gross_profit / avg_inventory_cost


def calculate_dynamic_caps(base_caps: Dict[str, int], total_budget: float) -> Dict[str, int]:
    """
    Scale department caps based on the total budget.
    Formula: Base Cap * sqrt(Total Budget / Baseline Budget)
    Using sqrt ensures caps don't grow/shrink too aggressively.
    """
    scaling_factor = (total_budget / BASELINE_BUDGET) ** 0.5
    logger.info(f"[AMIT] Scaling caps with factor: {scaling_factor:.3f} (Budget: {total_budget:,.0f} KES)")
    
    dynamic_caps = {}
    for dept, base_cap in base_caps.items():
        scaled_cap = int(base_cap * scaling_factor)
        dynamic_caps[dept] = max(MIN_DEPT_CAP_FLOOR, scaled_cap)
        
    return dynamic_caps


def run_amit(nn_path: str, data_dir: str, dept_caps: Dict[str, int] = None, total_budget: float = None) -> Dict[str, Any]:
    """
    Execute the AMIT Gatekeeper logic.
    """
    nodes = load_nodes(nn_path)
    if not nodes:
        logger.warning("No nodes loaded. AMIT cannot execute.")
        return {"blacklist": [], "department_caps": {}, "lowest_gmroi": {}, "stats": {}}

    # LATA Loop Closure: Load supplier reliability multipliers
    lata_multipliers = load_lata_patterns(data_dir)

    # Determine the caps to use
    if dept_caps:
        caps = dept_caps
    elif total_budget:
        caps = calculate_dynamic_caps(DEFAULT_DEPT_CAPS_BASELINE, total_budget)
    else:
        caps = DEFAULT_DEPT_CAPS_BASELINE

    fallback_cap = DEFAULT_CAP_FALLBACK_BASELINE
    if total_budget:
        scaling_factor = (total_budget / BASELINE_BUDGET) ** 0.5
        fallback_cap = max(MIN_DEPT_CAP_FLOOR, int(DEFAULT_CAP_FALLBACK_BASELINE * scaling_factor))
    
    dept_skus: Dict[str, List[Dict]] = defaultdict(list)
    for node in nodes:
        dept = node["department"]
        supplier = node["supplier"].upper()
        
        # Get LATA multiplier or default to 1.0 (Neutral)
        multiplier = lata_multipliers.get(supplier, 1.0)
        
        node["gmroi"] = calculate_gmroi(node, lata_multiplier=multiplier)
        node["lata_multiplier"] = multiplier
        dept_skus[dept].append(node)

    blacklist = []
    lowest_gmroi_per_dept = {}
    dept_stats = {}

    for dept, skus in dept_skus.items():
        cap = caps.get(dept, fallback_cap)

        # Sort by GMROI descending (best performers first)
        skus.sort(key=lambda x: x["gmroi"], reverse=True)

        dept_stats[dept] = {
            "total_skus": len(skus),
            "cap": cap,
            "over_cap": max(0, len(skus) - cap),
        }

        if len(skus) > cap:
            # Keep the top `cap` SKUs, blacklist the rest
            rejects = skus[cap:]

            for rej in rejects:
                multiplier = rej["lata_multiplier"]
                gmroi = rej["gmroi"]
                
                reason = f"AMIT Trimming: Exceeds {dept} category cap ({len(skus)}/{cap})."
                
                if multiplier > 1.0:
                    reason += f" [LOGISTICAL RISK: {multiplier}x penalty applied to inventory cost]."
                elif multiplier < 1.0:
                    reason += f" [LOGISTICAL ALPHA: {multiplier}x bonus for high reliability]."
                
                reason += f" Reliability-Adjusted GMROI: {gmroi:.4f}"
                
                blacklist.append({
                    "sku": rej["id"],
                    "department": dept,
                    "gmroi": round(gmroi, 4),
                    "lata_multiplier": multiplier,
                    "reason": reason,
                })

            logger.info(f"[AMIT] {dept}: {len(skus)} SKUs → Cap {cap} → Blacklisted {len(rejects)} items.")
        else:
            logger.debug(f"[AMIT] {dept}: {len(skus)} SKUs within cap ({cap}).")

        # Record the lowest GMROI SKU in every department (for One-In-One-Out swaps)
        if skus:
            worst = skus[-1]
            lowest_gmroi_per_dept[dept] = {
                "sku": worst["id"],
                "gmroi": round(worst["gmroi"], 4),
                "sales_rank": worst["sales_rank"],
            }

    # Build the flat blacklist set for O(1) lookups in the engine
    blacklist_set = [item["sku"] for item in blacklist]

    enforcement = {
        "blacklist": blacklist_set,
        "blacklist_details": blacklist,
        "lowest_gmroi_per_dept": lowest_gmroi_per_dept,
        "department_caps_applied": {d: caps.get(d, fallback_cap) for d in dept_skus},
        "total_budget_scale": total_budget,
        "stats": {
            "total_skus_analyzed": len(nodes),
            "total_departments": len(dept_skus),
            "total_blacklisted": len(blacklist),
            "departments_over_cap": sum(1 for d in dept_stats.values() if d["over_cap"] > 0),
        },
    }

    # Write output
    output_path = os.path.join(data_dir, "amit_enforcement.json")
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(enforcement, f, indent=2)

    logger.info(f"[AMIT] Enforcement written to {output_path}")
    logger.info(f"[AMIT] Stats: {enforcement['stats']}")

    return enforcement


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="AMIT Gatekeeper — Chapter 11 Sub-Routine A")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"), help="Path to oasis/data directory")
    parser.add_argument("--nn-path", default=os.path.join(os.path.dirname(__file__), "..", "..", "neutral_network_export"), help="Path to neural network export directory")
    parser.add_argument("--budget", type=float, help="Total budget for dynamic cap scaling (e.g. 10000000 for 10M KES)")
    args = parser.parse_args()

    result = run_amit(args.nn_path, args.data_dir, total_budget=args.budget)
    print(f"\n=== AMIT COMPLETE ===")
    print(f"Total Blacklisted: {result['stats']['total_blacklisted']}")
    print(f"Departments Over Cap: {result['stats']['departments_over_cap']}")
