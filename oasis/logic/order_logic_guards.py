from typing import List, Dict
from .rounding import apply_pack_rounding

def apply_safety_guards(recommendations: List[dict], products_map: Dict[str, dict], allocation_mode: str = "replenishment") -> List[dict]:
    """
    Apply strict Python-based safety guards to harmonized logic.
    Enforces caps, buffer zones, and fresh rules regardless of LLM output.
    """
    for rec in recommendations:
        p = products_map.get(rec['product_name'])
        if not p: continue
        
        # Original LLM reasoning (keep for context)
        reason = rec.get('reasoning', '')
        
        # Logic Variables
        product_name = rec['product_name']
        p_upper = product_name.strip().upper()
        dept_upper = str(p.get('product_category', '')).upper()
        days_since_delivery = int(p.get('last_days_since_last_delivery', 0))
        
        # v10.0 Freshness Parity
        from .department_constants import FRESH_DEPARTMENTS
        is_fresh_dept = any(x in dept_upper for x in FRESH_DEPARTMENTS)
        is_fresh_token = any(x in p_upper for x in ['BREAD', 'MILK', 'YOGHURT', 'YOGURT', 'EGGS', 'CREAM'])
        is_fresh = is_fresh_dept or is_fresh_token or p.get('is_fresh', False)
        
        current_stock = int(p.get('current_stocks', 0))
        pack_size = int(p.get('pack_size', 1))
        
        # Sales metrics (v10.0 Blended Velocity)
        avg_daily_sales = float(p.get('avg_daily_sales', 0.1))
        avg_daily_sales_last_30d = float(p.get('avg_daily_sales_last_30d', 0.0))
        
        # Formula: 70% Recent (30d) + 30% Historical (Total Avg)
        if avg_daily_sales_last_30d > 0:
            effective_daily_sales = (0.7 * avg_daily_sales_last_30d) + (0.3 * avg_daily_sales)
        else:
            effective_daily_sales = avg_daily_sales
        
        effective_daily_sales = max(0.01, effective_daily_sales)
        
        total_units_sold_last_90d = p.get('total_units_sold_last_90d', 0)
        
        # --- HARMONIZED LOGIC ENFORCEMENT ---
        cap_qty = None
        cap_reason = ""
        
        # GREENFIELD BYPASS (Day 1 Allocation)
        if allocation_mode == "initial_load":
            # Skip aging checks, but enforce MDQ (Minimum Display Quantity)
            base_rec = rec.get('recommended_quantity', 0)
            
            # FIX H5: Fresh items in greenfield still need a spoilage ceiling
            if is_fresh and base_rec > 0:
                max_fresh_greenfield = max(pack_size, int(effective_daily_sales * 3.0))
                if base_rec > max_fresh_greenfield:
                    rec['recommended_quantity'] = max_fresh_greenfield
                    rec['reasoning'] = reason + f" [GREENFIELD: Fresh Spoilage Cap ({max_fresh_greenfield})]"
            
            # v10.1: Only upgrade to pack size if it's a key SKU or if base_rec is substantial
            base_rec = rec.get('recommended_quantity', 0)  # Re-read after potential fresh cap
            if base_rec > 0 and base_rec < pack_size:
                is_key = p.get('is_key_sku', False) or p.get('is_staple', False)
                # If it's a key SKU, we always want at least one pack
                # If not, we only upgrade if it's at least 25% of a pack
                if is_key or (base_rec / pack_size >= 0.25):
                    rec['recommended_quantity'] = pack_size
                    rec['reasoning'] = reason + " [GREENFIELD: Enforced MDQ (1 Pack)]"
                else:
                    # Keep the small allocation (Break Bulk)
                    rec['reasoning'] = reason + " [GREENFIELD: Small Allocation Maintained (No MDQ Upgrade)]"
        else:
            # 1. Tiered Fresh Logic (Golden State Request 2 Parity)
            if is_fresh:
                if days_since_delivery > 120 and effective_daily_sales > 0:
                    # Specific Golden State Cap: Max 1 Pack for aging fresh
                    rec['recommended_quantity'] = min(rec.get('recommended_quantity', 0), float(pack_size))
                    rec['reasoning'] = reason + " [GUARD: Fresh Aging Cap (>120d)]"
                elif days_since_delivery > 180:
                    rec['recommended_quantity'] = 0.0
                    rec['reasoning'] = reason + " [GUARD: Critical Stale Fresh (>180d). Blocked.]"

            # 2. Slow Mover Logic (Dry)
            elif days_since_delivery >= 200:
                if total_units_sold_last_90d == 0:
                    # Dead Stock
                    if p.get('abc_rank') == 'A' and current_stock == 0:
                         pass 
                    else:
                        cap_qty = 0
                        cap_reason = "GUARD: Dead Stock (>200d, No Sales). Blocked."
                else:
                    # Steady Slow Mover: Cap at 21 days coverage
                    max_stock = 21 * effective_daily_sales
                    max_order = max(0, int(max_stock - current_stock))
                    if max_order < (pack_size * 0.5): max_order = 0
                    cap_qty = max_order
                    cap_reason = f"GUARD: Slow Mover Steady (>200d). Capped at 21d coverage ({max_order})."

            # 3. Buffer Zone (160-200d)
            elif 160 <= days_since_delivery < 200:
                current_rec = rec.get('recommended_quantity', 0)
                if current_rec > 0:
                    new_qty = int(current_rec * 0.8)
                    rec['recommended_quantity'] = new_qty
                    rec['reasoning'] = reason + " [GUARD: Buffer Zone 160-200d, reduced 20%]"

        # Apply Hard Caps
        if cap_qty is not None:
            if rec.get('recommended_quantity', 0) > cap_qty:
                rec['recommended_quantity'] = cap_qty
                rec['reasoning'] = reason + f" [{cap_reason}]"
        
        # v10.0: Global SAFETY CAP (Mode-Aware)
        # Prevents data-driven over-ordering beyond logical stock targets.
        # FIX 12: Completely bypass this cap in 'initial_load' mode. The engine's programmatic
        # rules (Pass 1 MDQ, Pass 4 Mop-Up) are explicitly designed to deploy capital beyond
        # minimal targets. Capping them here causes 30%+ under-allocation.
        if allocation_mode != 'initial_load':
            current_rec = rec.get('recommended_quantity', 0)
            logical_target = effective_daily_sales * 21.0
            
            cap_multiplier = 3.0
            max_safe_order = max(float(pack_size) * 3.0, logical_target * cap_multiplier)
            
            if current_rec > max_safe_order:
                rec['recommended_quantity'] = max_safe_order
                rec['reasoning'] += f" [GUARD: Global {cap_multiplier:.0f}x Cap ({current_rec} -> {max_safe_order:.0f})]"
        
        # --- PACK ROUNDING (Final Step) ---
        base_qty = rec.get('recommended_quantity', 0)
        coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
        risk_level = "high" if (current_stock <= 0 or coverage_days < 3) else ("low" if coverage_days > 20 else "medium")
            
        rounding_info = apply_pack_rounding(
            base_qty=base_qty,
            pack_size=pack_size,
            is_key_sku=p.get('is_key_sku', False),
            stockout_risk=risk_level,
            max_overage_ratio=0.25,
            abc_rank=str(p.get('abc_rank', 'B')).strip().upper()
        )
        
        rec['recommended_quantity'] = rounding_info['rounded_qty']
        rec['pack_rounding'] = rounding_info
        if rounding_info['rounding_direction'] != 'none':
            rec['reasoning'] += f" [Pack Rounding: {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']})]"
                
    return recommendations
