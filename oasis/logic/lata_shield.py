"""
LATA — Lead-Time & Allocation Shield (Pre-Flight Engine)
Chapter 11, Sub-Routine B: The Logistical Shield.

Manages the most dangerous variable in Kenyan retail: Supplier Lead-Time variance.
Ignores the supplier's promise and uses Historical Variance from GRN logs.

Usage:
    python -m oasis.logic.lata_shield --data-dir ./oasis/data

Output:
    Updates supplier_patterns_2025.json with lata_variance_multiplier field.
"""

import json
import os
import logging
import argparse
import statistics
import csv
import glob
from collections import defaultdict
from typing import Dict, Any, List

logger = logging.getLogger("OASIS.LATA")
logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(name)s] %(levelname)s: %(message)s")

def _load_lata_config(data_dir: str) -> Dict[str, Any]:
    """Helper to load LATA parameters from the central config."""
    path = os.path.join(data_dir, 'oasis_engines_config.json')
    if os.path.exists(path):
        try:
            with open(path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            return config.get('engines', {}).get('lata', {})
        except Exception as e:
            logger.warning(f"Failed to load LATA config: {e}")
    return {}


def calculate_variance_multiplier(gap_days: List[int], stated_lead_time: float) -> Dict[str, Any]:
    """
    Calculate the LATA variance multiplier for a supplier.
    
    Logic:
    - If Historical Variance > 30% of stated Lead Time → Inflate safety stock (multiplier > 1.0)
    - If Historical Variance < 5% of stated Lead Time → Shrink safety stock (multiplier < 1.0)
    - Otherwise → Neutral (multiplier = 1.0)
    
    Returns a dict with the multiplier and diagnostic data.
    """
    if len(gap_days) < 2:
        return {
            "lata_variance_multiplier": 1.0,
            "lata_fulfillment_penalty": 1.0,
            "lata_confidence": "LOW",
            "lata_sample_size": len(gap_days),
            "lata_reason": "Insufficient data (< 2 delivery records)",
        }

    mean_gap = statistics.mean(gap_days)
    stdev_gap = statistics.stdev(gap_days)
    median_gap = statistics.median(gap_days)

    # Coefficient of variation (CV) — the core LATA metric
    cv = stdev_gap / mean_gap if mean_gap > 0 else 0.0

    # Variance ratio against stated lead time
    variance_ratio = stdev_gap / max(stated_lead_time, 1.0)

    # Calculate the multiplier
    if variance_ratio > 0.30:
        # Unreliable supplier — inflate safety stock
        # Scale: 30% variance → 1.2x, 60% → 1.5x, 100%+ → 2.0x (capped)
        multiplier = min(2.0, 1.0 + (variance_ratio * 1.5))
        reason = f"UNRELIABLE: Variance {variance_ratio:.1%} of LT. Inflating safety stock."
    elif variance_ratio < 0.05:
        # Very reliable supplier — shrink safety stock to free capital
        # Scale: 5% → 0.95x, 2% → 0.85x, 0% → 0.80x (floor)
        multiplier = max(0.80, 1.0 - ((0.05 - variance_ratio) * 4.0))
        reason = f"RELIABLE: Variance {variance_ratio:.1%} of LT. Releasing capital."
    else:
        # Normal variance — neutral
        multiplier = 1.0
        reason = f"NORMAL: Variance {variance_ratio:.1%} of LT. Standard safety."

    confidence = "HIGH" if len(gap_days) >= 10 else ("MEDIUM" if len(gap_days) >= 5 else "LOW")

    return {
        "lata_variance_multiplier": round(multiplier, 3),
        "lata_cv": round(cv, 4),
        "lata_stdev_days": round(stdev_gap, 2),
        "lata_mean_gap_days": round(mean_gap, 2),
        "lata_median_gap_days": round(median_gap, 1),
        "lata_variance_ratio": round(variance_ratio, 4),
        "lata_confidence": confidence,
        "lata_sample_size": len(gap_days),
        "lata_reason": reason,
    }


def load_supplier_fill_rates(nn_path: str) -> Dict[str, float]:
    """
    Load SKU fill rates from neural network export and aggregate by supplier.
    """
    nodes_path = os.path.join(nn_path, "nodes.csv")
    if not os.path.exists(nodes_path):
        logger.warning(f"nodes.csv not found at {nodes_path}. Fulfillment indexing skipped.")
        return {}

    supplier_rates = defaultdict(list)
    try:
        with open(nodes_path, "r", encoding="utf-8") as f:
            reader = csv.DictReader(f)
            for row in reader:
                supplier = row.get("supplier", "Unknown").strip("[]").strip().upper()
                try:
                    fr = float(row.get("rhapta_fill_rate", 1.0) or 1.0)
                    supplier_rates[supplier].append(fr)
                except (ValueError, TypeError):
                    continue
    except Exception as e:
        logger.error(f"Error reading nodes.csv for fill rates: {e}")
        return {}

    # Calculate average fill rate per supplier
    avg_rates = {}
    for supplier, rates in supplier_rates.items():
        if rates:
            avg_rates[supplier] = sum(rates) / len(rates)
    
    logger.info(f"Aggregated fulfillment data for {len(avg_rates)} suppliers.")
    return avg_rates


def load_prts_return_rates(data_dir: str) -> Dict[str, Dict[str, Any]]:
    """
    Load PRTS (Purchase Returns to Supplier) data and calculate per-supplier return rates.
    Specifically tracks 'Short Supply' events as supplier-failure indicators.
    
    Returns: { supplier_name_upper: { 'return_count': int, 'total_returns': int } }
    """
    prts_files = sorted(glob.glob(os.path.join(data_dir, 'prts_*.xlsx')))
    if not prts_files:
        logger.info("No PRTS files found. Return penalty will not be applied.")
        return {}
    
    try:
        import pandas as pd
    except ImportError:
        logger.warning("pandas not available. PRTS loading skipped.")
        return {}
    
    all_frames = []
    for f in prts_files:
        try:
            df = pd.read_excel(f)
            all_frames.append(df)
        except Exception as e:
            logger.warning(f"Failed to read PRTS file {f}: {e}")
    
    if not all_frames:
        return {}
    
    raw_prts = pd.concat(all_frames, ignore_index=True)
    
    # Extract vendor name and reason
    vendor_col = next((c for c in raw_prts.columns if 'ven' in c.lower() and 'code' in c.lower()), None)
    reason_col = next((c for c in raw_prts.columns if 'reason' in c.lower()), None)
    
    if not vendor_col:
        logger.warning("No vendor column found in PRTS data.")
        return {}
    
    supplier_returns = defaultdict(lambda: {'short_supply_count': 0, 'total_returns': 0})
    
    for _, row in raw_prts.iterrows():
        supplier = str(row[vendor_col]).strip().upper()
        # Normalize: strip vendor code prefix (e.g., 'SA0015 - ALISON PRODUCTS LTD' → 'ALISON PRODUCTS LTD')
        if ' - ' in supplier:
            supplier = supplier.split(' - ', 1)[1].strip()
        
        supplier_returns[supplier]['total_returns'] += 1
        
        if reason_col and 'short' in str(row.get(reason_col, '')).lower():
            supplier_returns[supplier]['short_supply_count'] += 1
    
    logger.info(f"Loaded PRTS return data for {len(supplier_returns)} suppliers from {len(prts_files)} files.")
    return dict(supplier_returns)


def run_lata(data_dir: str, nn_path: str = None) -> Dict[str, Any]:
    """
    Execute the LATA Shield logic.
    
    1. Load existing supplier_patterns.
    2. For each supplier with historical gap data, calculate variance multiplier.
    3. (Upgrade) Apply fulfillment rate penalty.
    4. Update the patterns JSON with LATA fields.
    5. Save back.
    """
    # Find the supplier patterns file
    patterns_path = None
    for fname in os.listdir(data_dir):
        if "supplier_patterns" in fname and fname.endswith(".json"):
            patterns_path = os.path.join(data_dir, fname)
            break

    if not patterns_path:
        logger.warning("No supplier_patterns JSON found. LATA cannot execute.")
        return {"updated": 0, "inflated": 0, "deflated": 0, "neutral": 0}

    with open(patterns_path, "r", encoding="utf-8") as f:
        patterns = json.load(f)

    logger.info(f"Loaded {len(patterns)} supplier patterns from {os.path.basename(patterns_path)}")

    # Load fulfillment data if path provided
    fill_rates = {}
    if nn_path:
        fill_rates = load_supplier_fill_rates(nn_path)

    # Load PRTS return data (Convergence with Pitch STI)
    prts_returns = load_prts_return_rates(data_dir)

    stats = {"updated": 0, "inflated": 0, "deflated": 0, "neutral": 0, "actual_data": 0, "synthetic_fallback": 0, "return_penalized": 0}

    # Load raw delivery gaps if available (v1.1 Upgrade)
    gaps_path = os.path.join(data_dir, "supplier_delivery_gaps.json")
    raw_gaps_db = {}
    if os.path.exists(gaps_path):
        try:
            with open(gaps_path, "r", encoding="utf-8") as f:
                raw_gaps_db = json.load(f)
            logger.info(f"Loaded raw delivery gaps for {len(raw_gaps_db)} suppliers.")
        except Exception as e:
            logger.warning(f"Failed to load raw delivery gaps: {e}")

    # Load LATA config
    lata_cfg = _load_lata_config(data_dir)
    min_records = lata_cfg.get("min_records_for_variance", 2)
    max_multiplier = lata_cfg.get("max_variance_multiplier", 2.0)

    for supplier, data in patterns.items():
        if not isinstance(data, dict):
            continue

        # Extract historical gap data
        median_gap = data.get("median_gap_days")
        avg_gap = data.get("average_gap_days") or data.get("avg_gap_days")
        total_orders = data.get("total_orders_2025") or data.get("total_orders", 0)
        stated_lt = float(data.get("estimated_delivery_days", lata_cfg.get("default_stated_lead_time", 7)))

        # v1.1 Upgrade: Prioritize actual gap data for variance calculation
        actual_gaps = raw_gaps_db.get(supplier)
        
        if actual_gaps and len(actual_gaps) >= min_records:
            # Use real historical data
            lata_result = calculate_variance_multiplier(actual_gaps, stated_lt)
            lata_result["lata_data_source"] = "ACTUAL"
            stats["actual_data"] += 1
        else:
            # v1.2 HALLUCINATION FIX: Remove Synthetic Fallback (random.gauss)
            # Instead of forging data, we default to neutral and flag the lack of data
            lata_result = {
                "lata_variance_multiplier": 1.0,
                "lata_confidence": "LOW",
                "lata_data_source": "INSUFFICIENT_DATA",
                "lata_reason": f"Records ({len(actual_gaps) if actual_gaps else 0}) < required min ({min_records}). Neutral safety applied."
            }
            stats["synthetic_fallback"] += 1 # Renamed to reflect 'bypassed'

        data.update(lata_result)

        # Apply fulfillment rate penalty (LATA Upgrade)
        avg_fr = fill_rates.get(supplier, 1.0)
        data["lata_avg_fulfillment_rate"] = round(avg_fr, 3)
        
        # Formula: Each 10% drop below 95% incurs a 10% multiplier penalty
        if avg_fr < 0.95:
            penalty = 1.0 + (0.95 - avg_fr)
            # Cap penalty at 1.5x
            penalty = min(1.5, penalty)
            data["lata_fulfillment_penalty"] = round(penalty, 3)
            data["lata_variance_multiplier"] = round(data["lata_variance_multiplier"] * penalty, 3)
            data["lata_reason"] += f" [Short-Delivery Penalty: {penalty:.2f}]"
        else:
            data["lata_fulfillment_penalty"] = 1.0

        # v2.0 UPGRADE: PRTS Return Penalty (Convergence with Pitch STI)
        # Short-supply returns indicate vendor reliability issues not captured by delivery gap variance
        supplier_upper = supplier.upper()
        prts_data = prts_returns.get(supplier_upper, {})
        short_supply_count = prts_data.get('short_supply_count', 0)
        total_returns_count = prts_data.get('total_returns', 0)
        
        if total_orders and total_orders > 0 and short_supply_count > 0:
            return_rate = short_supply_count / total_orders
            
            # Return penalty: Scaled by config rate and capped by config cap
            ret_cap = lata_cfg.get("return_penalty_cap", 0.50)
            ret_mult = lata_cfg.get("return_penalty_rate_multiplier", 1.5)
            
            return_mult_penalty = min(ret_cap, return_rate * ret_mult)
            data["lata_variance_multiplier"] = round(data["lata_variance_multiplier"] + return_mult_penalty, 3)
            data["lata_return_rate"] = round(return_rate, 4)
            data["lata_return_penalty"] = round(return_mult_penalty, 4)
            data["lata_short_supply_returns"] = short_supply_count
            data["lata_reason"] += f" [PRTS Return Penalty: +{return_mult_penalty:.3f} (rate={return_rate:.2%})]"
            stats["return_penalized"] += 1
        else:
            data["lata_return_rate"] = 0.0
            data["lata_return_penalty"] = 0.0
            data["lata_short_supply_returns"] = 0

        # Enforce global max multiplier cap
        data["lata_variance_multiplier"] = min(max_multiplier + 1.0, data["lata_variance_multiplier"])

        stats["updated"] += 1
        mult = lata_result["lata_variance_multiplier"]
        if mult > 1.0:
            stats["inflated"] += 1
        elif mult < 1.0:
            stats["deflated"] += 1
        else:
            stats["neutral"] += 1

    # Save updated patterns
    with open(patterns_path, "w", encoding="utf-8") as f:
        json.dump(patterns, f, indent=2)

    logger.info(f"[LATA] Updated {stats['updated']} suppliers. Actual Used: {stats['actual_data']}, Synthetic: {stats['synthetic_fallback']}, Return-Penalized: {stats['return_penalized']}")
    logger.info(f"[LATA] Multipliers -> Inflated: {stats['inflated']}, Deflated: {stats['deflated']}, Neutral: {stats['neutral']}")

    return stats


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="LATA Shield — Chapter 11 Sub-Routine B")
    parser.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "data"), help="Path to oasis/data directory")
    parser.add_argument("--nn-path", default=os.path.join(os.path.dirname(__file__), "..", "..", "neutral_network_export"), help="Path to neural network export")
    args = parser.parse_args()

    result = run_lata(args.data_dir, nn_path=args.nn_path)
    print("\n=== LATA COMPLETE ===")
    print(f"Suppliers Updated: {result['updated']}")
    print(f"Safety Inflated (unreliable): {result['inflated']}")
    print(f"Capital Released (reliable): {result['deflated']}")
