"""
MANDE — Market, Network, and Distribution Efficiency (Pre-Flight Engine)
Chapter 11, Sub-Routine D: The Negotiator.

Evaluates your Supplier Efficiency Index (SEI).
Generates the Purge Report — actionable data for supplier delisting negotiations.

Usage:
    python -m oasis.logic.mande_triage --data-dir ./oasis/data --nn-path ./neutral_network_export

Output:
    oasis/data/mande_purge_report.csv
    oasis/data/mande_purge_report.json
"""

import csv
import json
import os
import logging
import argparse
from collections import defaultdict
from typing import Dict, List, Any, Set

logger = logging.getLogger("OASIS.MANDE")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

def _load_mande_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load MANDE parameters from the central config."""
    path = os.path.join(data_dir, 'oasis_engines_config.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config.get('engines', {}).get('mande', {})
        except Exception as e:
            logger.warning(f"Failed to load MANDE config: {e}")
    return {"trapped_capital_days": 30.0}

def load_nodes_by_supplier(nn_path: str) -> Dict[str, List[Dict[str, Any]]]:
    """Load all SKU nodes grouped by supplier."""
    nodes_path = os.path.join(nn_path, "nodes.csv")
    if not os.path.exists(nodes_path):
        logger.error(f"nodes.csv not found at {nodes_path}")
        return {}

    supplier_skus: Dict[str, List[Dict]] = defaultdict(list)
    with open(nodes_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row.get("type") != "SKU":
                continue
            supplier = row.get("supplier", "Unknown").strip("[]").strip().upper()
            if supplier == "UNKNOWN" or not supplier:
                continue

            supplier_skus[supplier].append({
                "id": row["id"],
                "department": row.get("department", "GENERAL").strip("[]").strip().upper(),
                "price": float(row.get("price", 0) or 0),
                "margin_pct": float(row.get("margin_pct", 0) or 0),
                "revenue": float(row.get("revenue", 0) or 0),
                "gross_profit": float(row.get("gross_profit", 0) or 0),
                "velocity_ads": float(row.get("velocity_ads", 0) or 0),
                "sales_rank": float(row.get("sales_rank", 99999) or 99999),
            })

    logger.info(f"Loaded SKUs for {len(supplier_skus)} suppliers.")
    return dict(supplier_skus)


def load_substitution_counts(nn_path: str) -> Dict[str, int]:
    """
    Count how many substitution edges each SKU has.
    High substitution count = the SKU is easily replaceable.
    """
    edges_path = os.path.join(nn_path, "edges.csv")
    if not os.path.exists(edges_path):
        return {}

    sub_counts: Dict[str, int] = defaultdict(int)
    with open(edges_path, "r", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            if row["relation"] == "substitution":
                sub_counts[row["source"]] += 1

    return dict(sub_counts)


def calculate_supplier_efficiency_index(
    supplier: str,
    skus: List[Dict[str, Any]],
    sub_counts: Dict[str, int],
    trapped_days: float = 30.0
) -> Dict[str, Any]:
    """
    Calculate the Supplier Efficiency Index (SEI) for MANDE.
    
    SEI Components:
    1. Trapped Capital = sum(price * velocity_ads * 30) for all SKUs
       → How much working capital this supplier ties up
    2. Revenue Contribution = sum(revenue) for all SKUs
    3. Substitution Exposure = avg substitution edges across SKUs
       → How easily the supplier's products can be replaced
    4. Margin Quality = weighted average margin_pct
    
    SEI = Revenue / Trapped_Capital (capital efficiency ratio)
    
    A LOW SEI with HIGH Substitution Exposure = prime delisting candidate.
    """
    total_trapped_capital = 0.0
    total_revenue = 0.0
    total_gross_profit = 0.0
    total_substitution_edges = 0
    weighted_margin_sum = 0.0
    weighted_margin_denom = 0.0

    for sku in skus:
        # v1.1 FIX: Use configurable capital trap window
        trapped = sku["price"] * max(sku["velocity_ads"], 0.001) * trapped_days
        total_trapped_capital += trapped
        total_revenue += sku["revenue"]
        total_gross_profit += sku["gross_profit"]

        # Substitution exposure
        total_substitution_edges += sub_counts.get(sku["id"], 0)

        # Weighted margin
        if sku["revenue"] > 0:
            weighted_margin_sum += sku["margin_pct"] * sku["revenue"]
            weighted_margin_denom += sku["revenue"]

    avg_substitution = total_substitution_edges / max(len(skus), 1)
    weighted_margin = weighted_margin_sum / max(weighted_margin_denom, 1)

    # Capital Efficiency Ratio
    sei = total_revenue / max(total_trapped_capital, 1.0)

    # Net Capital Position improvement if delisted
    # Assumes volume can be moved to alternative suppliers
    capital_release_potential = total_trapped_capital

    # Estimate days improvement = trapped_capital / (total_revenue / 365)
    daily_revenue = total_revenue / 365.0
    days_improvement = capital_release_potential / max(daily_revenue, 1.0)

    return {
        "supplier": supplier,
        "sku_count": len(skus),
        "total_revenue": round(total_revenue, 2),
        "total_gross_profit": round(total_gross_profit, 2),
        "trapped_capital_kes": round(total_trapped_capital, 2),
        "trapped_days": trapped_days,
        "avg_substitution_edges": round(avg_substitution, 2),
        "weighted_margin_pct": round(weighted_margin, 2),
        "sei": round(sei, 4),
        "capital_release_potential_kes": round(capital_release_potential, 2),
        "net_capital_position_improvement_days": round(days_improvement, 1),
        "delisting_risk": "HIGH" if sei < 0.5 and avg_substitution > 3.0 else (
            "MEDIUM" if sei < 1.0 and avg_substitution > 2.0 else "LOW"
        ),
    }


def run_mande(nn_path: str, data_dir: str) -> Dict[str, Any]:
    """
    Execute the MANDE Network Triage.
    
    1. Load all supplier SKUs from the neural network.
    2. Load substitution edges.
    3. Calculate SEI for each supplier.
    4. Rank and generate the Purge Report.
    """
    supplier_skus = load_nodes_by_supplier(nn_path)
    if not supplier_skus:
        logger.warning("No supplier data. MANDE cannot execute.")
        return {"suppliers_analyzed": 0, "purge_candidates": 0}

    sub_counts = load_substitution_counts(nn_path)

    # Load Config
    config = _load_mande_config(data_dir)
    trapped_days = config.get("trapped_capital_days", 30.0)

    # Calculate SEI for each supplier
    all_sei = []
    for supplier, skus in supplier_skus.items():
        sei_data = calculate_supplier_efficiency_index(supplier, skus, sub_counts, trapped_days=trapped_days)
        all_sei.append(sei_data)

    # Sort by SEI ascending (worst performers first)
    all_sei.sort(key=lambda x: x["sei"])

    # Identify purge candidates
    purge_candidates = [s for s in all_sei if s["delisting_risk"] in ("HIGH", "MEDIUM")]
    high_risk = [s for s in all_sei if s["delisting_risk"] == "HIGH"]

    # Write CSV report
    csv_path = os.path.join(data_dir, "mande_purge_report.csv")
    with open(csv_path, "w", newline="", encoding="utf-8") as f:
        if all_sei:
            writer = csv.DictWriter(f, fieldnames=all_sei[0].keys())
            writer.writeheader()
            writer.writerows(all_sei)

    # Write JSON report (for UI integration)
    json_path = os.path.join(data_dir, "mande_purge_report.json")
    report = {
        "generated_at": __import__("datetime").datetime.now().isoformat(),
        "summary": {
            "total_suppliers_analyzed": len(all_sei),
            "purge_candidates_high": len(high_risk),
            "purge_candidates_medium": len(purge_candidates) - len(high_risk),
            "total_capital_release_potential": round(sum(s["capital_release_potential_kes"] for s in purge_candidates), 2),
            "avg_days_improvement": round(
                sum(s["net_capital_position_improvement_days"] for s in purge_candidates) / max(len(purge_candidates), 1), 1
            ),
        },
        "purge_candidates": purge_candidates,
        "all_suppliers": all_sei,
    }

    with open(json_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2)

    logger.info(f"[MANDE] Purge Report written to {csv_path} and {json_path}")
    logger.info(f"[MANDE] {len(purge_candidates)} purge candidates identified out of {len(all_sei)} suppliers.")

    if high_risk:
        logger.info(f"\n[MANDE] === TOP HIGH-RISK SUPPLIERS ===")
        for s in high_risk[:5]:
            logger.info(
                f"  {s['supplier']}: SEI={s['sei']:.4f}, "
                f"Trapped Capital={s['trapped_capital_kes']:,.0f} KES ({s['trapped_days']}d), "
                f"Subs={s['avg_substitution_edges']:.1f}, "
                f"Release Potential={s['capital_release_potential_kes']:,.0f} KES"
            )

    return report


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="MANDE Triage — Chapter 11 Sub-Routine D")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"), help="Path to oasis/data directory")
    parser.add_argument("--nn-path", default=os.path.join(os.path.dirname(__file__), "..", "..", "neutral_network_export"), help="Path to neural network export")
    args = parser.parse_args()

    result = run_mande(args.nn_path, args.data_dir)
    print(f"\n=== MANDE COMPLETE ===")
    print(f"Suppliers Analyzed: {result['summary']['total_suppliers_analyzed']}")
    print(f"High-Risk Purge Candidates: {result['summary']['purge_candidates_high']}")
    print(f"Total Capital Release Potential: {result['summary']['total_capital_release_potential']:,.0f} KES")
