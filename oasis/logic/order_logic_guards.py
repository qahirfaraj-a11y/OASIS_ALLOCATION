from typing import List, Dict
from .rounding import apply_pack_rounding

def apply_safety_guards(recommendations: List[dict], products_map: Dict[str, dict], allocation_mode: str = "replenishment") -> List[dict]:
    """
    Apply strict Python-based safety guards to harmonized logic.
    Enforces caps, buffer zones, and fresh rules regardless of LLM output.
    """
    for rec in recommendations:
        if not isinstance(rec, dict): continue
        
        # Try itm_cd first, then product_name as fallback
        item_code = rec.get('itm_cd')
        p = products_map.get(str(item_code)) if item_code else None
        if not p:
            p = products_map.get(rec.get('product_name'))
            
        if not p: continue
        
        # Original LLM reasoning (keep for context)
        reason = rec.get('reasoning', '')
        
        # Logic Variables with safe casting
        try:
            days_since_delivery = int(p.get('last_days_since_last_delivery', 0) or 0)
            is_fresh = bool(p.get('is_fresh', False))
            current_stock = int(p.get('current_stocks', 0) or 0)
            pack_size = max(1, int(p.get('pack_size', 1) or 1))
            
            avg_daily_sales = float(p.get('avg_daily_sales', 0) or 0)
            avg_daily_sales_last_30d = float(p.get('avg_daily_sales_last_30d', 0) or 0)
            effective_daily_sales = max(0.01, avg_daily_sales_last_30d if avg_daily_sales_last_30d > 0 else avg_daily_sales)
            
            total_units_sold_last_90d = float(p.get('total_units_sold_last_90d', 0) or 0)
        except (ValueError, TypeError):
            continue # Skip malformed product data
        
        # --- HARMONIZED LOGIC ENFORCEMENT ---
        cap_qty = None
        cap_reason = ""
        
        if allocation_mode == "initial_load":
            base_rec = rec.get('recommended_quantity', 0) or 0
            if base_rec > 0 and base_rec < pack_size:
                rec['recommended_quantity'] = pack_size
                rec['reasoning'] = f"{reason} [GREENFIELD: Enforced MDQ (1 Pack)]"
        else:
            if is_fresh:
                if days_since_delivery > 180:
                    cap_qty, cap_reason = 0, "GUARD: Critical Stale Fresh (>180d). Blocked."
                elif days_since_delivery > 120:
                    if total_units_sold_last_90d == 0:
                        cap_qty, cap_reason = 0, "GUARD: Stale Fresh (>120d, No Sales). Blocked."
                    else:
                        max_order = max(0, int((7 * effective_daily_sales) - current_stock))
                        cap_qty, cap_reason = max_order, f"GUARD: Stale Fresh Watchlist (>120d). Capped at 7d coverage ({max_order})."
            elif days_since_delivery >= 200:
                if total_units_sold_last_90d == 0:
                    if not (p.get('abc_rank') == 'A' and current_stock == 0):
                        cap_qty, cap_reason = 0, "GUARD: Dead Stock (>200d, No Sales). Blocked."
                else:
                    max_order = max(0, int((21 * effective_daily_sales) - current_stock))
                    if max_order < (pack_size * 0.5): max_order = 0
                    cap_qty, cap_reason = max_order, f"GUARD: Slow Mover Steady (>200d). Capped at 21d coverage ({max_order})."
            elif 160 <= days_since_delivery < 200:
                current_rec = rec.get('recommended_quantity', 0) or 0
                if current_rec > 0:
                    rec['recommended_quantity'] = int(current_rec * 0.8)
                    rec['reasoning'] = f"{reason} [GUARD: Buffer Zone 160-200d, reduced 20%]"

        if cap_qty is not None and (rec.get('recommended_quantity', 0) or 0) > cap_qty:
            rec['recommended_quantity'] = cap_qty
            rec['reasoning'] = f"{rec.get('reasoning', reason)} [{cap_reason}]"
        
        # Bulk Ordering for Packaging
        dept = str(p.get('department') or p.get('product_category') or 'general').upper()
        name = str(p.get('product_name', '')).upper()
        if any(kw in dept for kw in ['PACKAGING', 'BAGS', 'CONTAINER']) or \
           any(kw in name for kw in ['PACKAGING', 'BAGS', 'CONTAINER']):
            bulk_target = 60 * effective_daily_sales
            curr_rec = rec.get('recommended_quantity', 0) or 0
            if 0 < curr_rec < bulk_target:
                rec['recommended_quantity'] = bulk_target
                rec['reasoning'] = f"{rec.get('reasoning', reason)} [BULK: Packaging item, rounded to 60d coverage ({bulk_target:.0f} units)]"

        # --- PACK ROUNDING (Final Step) ---
        coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
        risk_level = "high" if (current_stock <= 0 or coverage_days < 3) else ("low" if coverage_days > 20 else "medium")
            
        rounding_info = apply_pack_rounding(
            base_qty=rec.get('recommended_quantity', 0) or 0,
            pack_size=pack_size,
            is_key_sku=bool(p.get('is_key_sku', False)),
            stockout_risk=risk_level,
            max_overage_ratio=0.25
        )
        
        rec['recommended_quantity'] = rounding_info['rounded_qty']
        rec['pack_rounding'] = rounding_info
        if rounding_info['rounding_direction'] != 'none':
            rec['reasoning'] = f"{rec.get('reasoning', '')} [Pack Rounding: {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']})]"
                
    return recommendations
