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
DEFAULT_DEPT_CAPS_BASELINE = {
    "WINES": 120, "SPIRITS": 80, "BEER": 100, "DIWALI ITEMS": 30,
    "PARTY ITEMS": 40, "AIR FRESHNERS": 50, "BISCUITS": 80, "SNACKS": 100,
    "CONFECTIONERY": 120, "BEVERAGES": 150, "CEREALS": 60, "COOKING OIL": 40,
    "RICE": 30, "SUGAR": 20, "FLOUR": 30, "FRESH MILK": 40, "DAIRY": 60,
    "BREAD": 30, "BAKERY": 50, "HOUSEHOLD": 150, "TOILETRIES": 150,
    "DETERGENT": 80, "BABY CARE": 60, "STATIONERY": 50, "COSMETICS": 100,
    "PET FOOD": 30,
}

# Reference constants (will be updated from config in run_amit)
BASELINE_BUDGET = 10_000_000
MIN_DEPT_CAP_FLOOR = 5
DEFAULT_CAP_FALLBACK_BASELINE = 50


def _load_amit_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load the whole central config (AMIT reads engines + category rules).

    Resolved via oasis.logic.engines_config, so an install with no tuned
    oasis_engines_config.json picks up the SHIPPED defaults rather than an
    empty dict (deep-analysis finding S1).
    """
    from .engines_config import load_engines_config
    return load_engines_config(data_dir)


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
    """Load LATA safety multipliers to adjust GMROI rankings."""
    path = os.path.join(data_dir, "supplier_patterns_2025.json")
    if not os.path.exists(path):
        logger.warning("supplier_patterns_2025.json not found. LATA weighting disabled.")
        return {}

    with open(path, "r", encoding="utf-8") as f:
        patterns = json.load(f)

    return {s: d.get("lata_variance_multiplier", 1.0) for s, d in patterns.items() if isinstance(d, dict)}


def calculate_gmroi(node: Dict[str, Any], lata_multiplier: float = 1.0) -> float:
    """
    Calculate the reliability-adjusted GMROI for a SKU.
    Formula: GMROI = GrossProfit / (Avg Inventory Value * LATA Risk Penalty)
    
    Since we don't have per-SKU inventory cost, we use a proxy based on velocity:
    Avg Inventory Value = Price * (Velocity ADS * 30 days coverage)
    """
    gross_profit = node["gross_profit"]
    ads = node["velocity_ads"]
    price = node["price"]
    
    # 30-day stock turn coverage as denominator proxy
    avg_inventory_value = price * ads * 30
    
    # Penalty: If a supplier is erratic (LATA > 1.0), it 'traps' more capital 
    # to maintain the same service level, effectively lowering its GMROI.
    risk_adjusted_denom = avg_inventory_value * lata_multiplier
    
    if risk_adjusted_denom == 0:
        return 0.0
        
    return gross_profit / risk_adjusted_denom


def calculate_dynamic_caps(baseline_caps: Dict[str, int], total_budget: float, baseline_budget: float, min_floor: int) -> Dict[str, int]:
    """Scale department caps based on total budget vs baseline budget."""
    scaling_factor = (total_budget / baseline_budget) ** 0.5  # Sub-linear scaling
    
    dynamic_caps = {}
    for dept, cap in baseline_caps.items():
        dynamic_caps[dept] = max(min_floor, int(cap * scaling_factor))
        
    return dynamic_caps


def run_amit(nn_path: str, data_dir: str, dept_caps: Dict[str, int] = None, total_budget: float = None) -> Dict[str, Any]:
    """Execute the AMIT Gatekeeper logic."""
    nodes = load_nodes(nn_path)
    if not nodes:
        return {"stats": {"total_blacklisted": 0, "departments_over_cap": 0}}

    lata_multipliers = load_lata_patterns(data_dir)
    logger.info(f"Loaded {len(lata_multipliers)} LATA patterns for risk-weighting.")

    # Load Central Config for AMIT settings
    config = _load_amit_config(data_dir)
    amit_conf = config.get("engines", {}).get("amit", {})
    
    baseline_budget = amit_conf.get("baseline_budget", BASELINE_BUDGET)
    min_floor = amit_conf.get("min_dept_cap_floor", MIN_DEPT_CAP_FLOOR)
    base_caps = amit_conf.get("default_dept_caps_baseline", DEFAULT_DEPT_CAPS_BASELINE).copy()

    category_rules = config.get('category_rules', {})
    for dept, rule in category_rules.items():
        if dept in base_caps:
            boost = rule.get("boost", 1.0)
            base_caps[dept] = int(base_caps[dept] * boost)
            logger.info(f"[AMIT] Config: Boosting {dept} cap by {boost}x.")

    if dept_caps:
        caps = dept_caps
    elif total_budget:
        caps = calculate_dynamic_caps(base_caps, total_budget, baseline_budget, min_floor)
    else:
        caps = base_caps

    fallback_cap = DEFAULT_CAP_FALLBACK_BASELINE
    if total_budget:
        scaling_factor = (total_budget / baseline_budget) ** 0.5
        fallback_cap = max(min_floor, int(DEFAULT_CAP_FALLBACK_BASELINE * scaling_factor))
    
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

            logger.info(f"[AMIT] {dept}: {len(skus)} SKUs -> Cap {cap} -> Blacklisted {len(rejects)} items.")
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
    print("\n=== AMIT COMPLETE ===")
    print(f"Total Blacklisted: {result['stats']['total_blacklisted']}")
    print(f"Departments Over Cap: {result['stats']['departments_over_cap']}")
