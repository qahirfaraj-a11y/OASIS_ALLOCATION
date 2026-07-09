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
        
        # v10.5 Greenfield Trust: Use the engine's 'is_fresh' determination
        # The engine already carefully excludes UHT/Longlife from 'is_fresh'
        is_fresh = p.get('is_fresh', False)
        
        current_stock = int(p.get('current_stocks', 0))
        pack_size = int(p.get('pack_size', 1))
        
        # Sales metrics (v10.9 Context Parity: Use seasonally scaled engine demand if provided)
        avg_daily_sales = float(rec.get('avg_daily_sales', p.get('avg_daily_sales', 0.1)))
        avg_daily_sales_last_30d = float(rec.get('avg_daily_sales_last_30d', p.get('avg_daily_sales_last_30d', 0.0)))
        
        # v10.5: In Greenfield mode, we lack 30d recents for New Stores. 
        # MUST use 100% historical ADS to avoid 70% capital suppression.
        if allocation_mode == "initial_load":
            effective_daily_sales = avg_daily_sales
        elif avg_daily_sales_last_30d > 0:
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
            base_rec = rec.get('recommended_quantity', 0)
            
            # User Directive: Maintain strict fresh ceiling
            if is_fresh and base_rec > 0:
                # BUG 7 FIX: Skip if engine already applied a safety clamp in Pass 1
                # to avoid wasting Pass 2 depth cycles on items that will be re-capped.
                already_clamped = "SAFETY CLAMP" in reason or "JIT FRESH" in reason
                is_uht = any(x in p_upper for x in ["UHT", "LONG LIFE", "ESL"])
                # 1 Day + Buffer (3.0d total) for daily [Sync with Engine], 7d for Long-Life
                fresh_limit = 7.0 if is_uht else 3.0
                max_fresh_greenfield = float(effective_daily_sales * fresh_limit)
                
                if base_rec > max_fresh_greenfield and not already_clamped:
                    # v10.9: Ensure we never round to 0 in Greenfield if the engine recommended stock
                    final_q = max(1, int(round(max_fresh_greenfield)))
                    rec['recommended_quantity'] = final_q
                    rec['reasoning'] = reason + f" [GREENFIELD: Strict Fresh Cap ({max_fresh_greenfield:.1f} -> {final_q})]"
            
            # v10.9: DRY GOODS SYNC (90d to 120d Relief)
            # If the engine specifically requested stock beyond 90d (due to stranded capital),
            # we allow up to 120d before hard clamping.
            elif not is_fresh and base_rec > (effective_daily_sales * 90.0):
                max_dry_greenfield = float(effective_daily_sales * 120.0)
                if base_rec > max_dry_greenfield:
                    final_q = int(round(max_dry_greenfield))
                    rec['recommended_quantity'] = final_q
                    rec['reasoning'] = reason + f" [GREENFIELD: Hard 120d Ceiling ({base_rec} -> {final_q})]"
            
            # v10.4: Removed Pack-Size force for Greenfield
            # (Matches user directive: "let us not implement pack rounding")
            if rec.get('recommended_quantity', 0) > 0:
                if "[PASS" not in rec.get('reasoning', ''):
                    rec['reasoning'] = reason + " [GREENFIELD: Precision Quantity Maintained]"
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
                    rec['reasoning'] = reason + f" [GUARD: Buffer Zone 160-200d, reduced 20% ({current_rec} -> {new_qty})]"

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
        # v10.4: NO PACK ROUNDING in Initial Load mode (Break Bulk allowed for 100% spend)
        if allocation_mode == "initial_load":
                # v10.9: Ensure we never round to zero in Greenfield if the engine intended to buy the SKU
                raw_q = rec.get('recommended_quantity', 0)
                rounded_q = int(round(raw_q))
                if raw_q > 0 and rounded_q == 0:
                    rounded_q = 1
                
                rounding_info = {
                    "rounded_qty": rounded_q,
                    "rounding_direction": "none",
                    "rounding_reason": "Greenfield Mode: Integer Precision (Min-1 Floor)"
                }
        else:
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
        
        rec['recommended_quantity'] = rounding_info.get('rounded_qty', rec.get('recommended_quantity', 0))
        rec['pack_rounding'] = rounding_info
        if rounding_info.get('rounding_direction', 'none') != 'none':
            rec['reasoning'] += f" [Pack Rounding: {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']})]"
                
    return recommendations
