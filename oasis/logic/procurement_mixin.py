from .department_constants import ESSENTIAL_DEPARTMENTS, FRESH_DEPARTMENTS, FAST_FIVE_DEPARTMENTS
from .allocation_strategies import AllocationConfig, GreenfieldPipeline
import logging
import math
from typing import List, Dict, Optional, Any
# Pass 1 Golden Pack Rounding (No external apply_pack_rounding)

logger = logging.getLogger("OrderEngine.Procurement")

class ProcurementMixin:
    """
    ProcurementMixin handles the allocation of budget across products and suppliers,
    implementing multi-pass logic for greenfield scenarios and replenishment.
    """
    # Type hints for attributes and methods provided by the base OrderEngine or other Mixins
    data_dir: str
    profile_manager: Any
    budget_manager: Any
    databases: Dict[str, Any]
    total_budget: float
    grn_frequency_map: Dict[str, float]

    # --- Type Hints for Base Engine ---
    data_dir: str
    profile_manager: Any
    budget_manager: Any
    databases: Dict[str, Any]
    total_budget: float
    grn_frequency_map: Dict[str, float]

    # FIX C1: _get_actual_cost_price is defined in IntelligenceMixin (single source of truth).
    # The duplicate here had divergent margin logic and was removed to prevent latent bugs.

    def apply_greenfield_allocation(self, recommendations: List[dict], total_budget: float = 300000.0, seasonal_demand_map: Optional[Dict[str, float]] = None) -> Dict:
        """
        Phase 1 & 2: Initial Stock Allocation (The "Greenfield" Scenario).
        Now supports Hybrid Seasonal "Guiding".

        Delegates to GreenfieldPipeline, which runs the ``_gf_*`` pass
        methods in their proven order. Each pass communicates through a
        shared allocation context. Behavior is locked by
        tests/test_allocation_snapshot.py — regenerate that baseline only
        for deliberate logic changes.
        """
        return GreenfieldPipeline(self).execute(
            recommendations,
            budget=total_budget,
            seasonal_demand_map=seasonal_demand_map,
        )

    def _gf_preprocess(self, ctx) -> None:
        """Pass 0 pre-processing: seasonal blending, tier profile, wallets, priority sorting, and supplier consolidation."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        seasonal_demand_map = ctx.seasonal_demand_map

        logger.info(f"Starting Greenfield Allocation. Budget: ${total_budget:,.2f}")
        
        # --- FIX 5: REMOVED Global Demand Scaling (100M Baseline) ---
        # The 100M baseline artificially inflated demand, distorted cost estimates,
        # and caused the engine to hit pack caps prematurely while draining budget.
        # Allocation now uses real ADS from the scorecard/enrichment pipeline.
        # Preserve original ADS for audit trail.
        for r in recommendations:
            r['original_ads'] = float(r.get('avg_daily_sales', 0))
        
        
        # --- HYBRID DEMAND BLENDING (Guide Strategy) ---
        # v10.9: Restored Seasonal Blending.
        
        if seasonal_demand_map:
            logger.info("Applying Hybrid Seasonal Blending (Scorecard + Monthly Cache)...")
            common_vol_scorecard: float = 0.0
            common_vol_seasonal: float = 0.0
            
            for r in recommendations:
                p_name = r.get('product_name', '').upper()
                if p_name in seasonal_demand_map:
                    common_vol_scorecard += float(r.get('avg_daily_sales', 0))
                    common_vol_seasonal += float(seasonal_demand_map[p_name]) / 30.0
                    
            scale_factor = 1.0
            if common_vol_seasonal > 0:
                scale_factor = float(common_vol_scorecard) / float(common_vol_seasonal)
                scale_factor = max(0.5, min(1.5, scale_factor)) 
                logger.info(f"Derived Seasonal Scale Factor (Intersection): {scale_factor:.4f}")
            else:
                scale_factor = 1.0
            
            blended_count = 0
            for rec in recommendations:
                p_name = rec.get('product_name', '').upper()
                if p_name in seasonal_demand_map:
                    monthly_total = seasonal_demand_map[p_name]
                    seasonal_daily = (monthly_total / 30.0) * scale_factor
                    
                    core_daily = rec.get('avg_daily_sales', 0)
                    blended_daily = (core_daily + seasonal_daily) / 2.0
                    
                    rec['avg_daily_sales'] = blended_daily
                    rec['is_seasonally_adjusted'] = True
                    blended_count += 1
            
            logger.info(f"Blended demand for {blended_count} items based on seasonal cache.")

        summary = {
            'total_budget': total_budget,
            'pass1_cash': 0.0,
            'pass1_consignment': 0.0,
            'pass2_cash': 0.0,
            'pass2b_cash': 0.0,
            'total_skipped': 0,
            'skip_reasons': {},
            'dept_utilization': {}
        }
        
        profile = self.profile_manager.get_profile(total_budget)
        is_small = profile['is_small']
        is_micro = total_budget < 200_000.0  # Align with Duka Tier threshold
        
        fast_five_depts = FAST_FIVE_DEPARTMENTS
        depth_cap_days = profile['depth_days']
        max_total_packs = profile['max_packs']
        price_ceiling = profile['price_ceiling']
        min_display_qty = profile['min_display_qty']
        allow_c_class = profile['allow_c_class']
        
        # Use AllocationConfig if available on the engine, else create default
        alloc_cfg = getattr(self, 'allocation_config', None) or AllocationConfig()
        
        pass2_staple_share = alloc_cfg.get_staple_share(is_small, is_micro)
        
        logger.info(f"Tier Profile: {profile['tier_name']} | Ceiling: {price_ceiling} | Depth: {depth_cap_days}d")
        wallets = self.budget_manager.initialize_wallets(total_budget, buffer_pct=profile['wallet_buffer_pct'])
        
        def staple_priority_sort(x):
            is_staple = self.budget_manager.is_staple(x['product_name'], x.get('product_category'), x.get('avg_daily_sales', 0))
            dept = x.get('product_category', 'GENERAL').upper()
            priority = 3
            if is_staple and dept in fast_five_depts:
                priority = 0
            elif is_staple:
                priority = 1
            elif dept in ['SUGAR', 'SALT', 'FLOUR', 'RICE', 'COOKING OIL', 'FRESH MILK', 'BREAD', 'EGGS']:
                priority = 2
            return (priority, -x.get('avg_daily_sales', 0))
        
        recommendations.sort(key=staple_priority_sort)
        recommendations = [r for r in recommendations if str(r.get('product_name', '')).upper() != 'TOTAL']
        
        allowed_suppliers = {}
        supplier_cap = profile.get('supplier_cap', 999)
            
        if supplier_cap < 999:
            consolidation_depts = list(alloc_cfg.consolidation_depts)
            supplier_sales = {d: {} for d in consolidation_depts}
            for rec in recommendations:
                dept = rec.get('product_category', 'GENERAL').upper()
                if dept in consolidation_depts:
                    if not self.budget_manager.is_staple(rec['product_name'], rec.get('product_category'), rec.get('avg_daily_sales', 0)):
                        continue
                    supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    if not supp or supp == 'NON': supp = 'UNKNOWN'
                    revenue = rec.get('avg_daily_sales', 0) * rec.get('selling_price', 0)
                    supplier_sales[dept][supp] = supplier_sales[dept].get(supp, 0) + revenue
            
            for dept in consolidation_depts:
                ranked = sorted(supplier_sales[dept].items(), key=lambda x: x[1], reverse=True)
                if len(ranked) > supplier_cap:
                    top_n = [s[0] for s in ranked[:supplier_cap]]
                    allowed_suppliers[dept] = set(top_n)
                    logger.info(f"Consolidated {dept} Suppliers (Top {supplier_cap}/{len(ranked)}): {top_n}")
                elif ranked:
                    allowed_suppliers[dept] = set([s[0] for s in ranked])

        pass1_cost = 0.0
        pass1_consignment_val = 0.0
        sku_counts_per_dept = {} 
        
        # Injection 7: Global Capital Stress Multiplier
        ideal_unconstrained = sum((r.get('avg_daily_sales', 0) * 14.0) * float(self._get_actual_cost_price(r, float(r.get('selling_price', 0)))) for r in recommendations)
        capital_stress = max(0.0, min(1.0, 1.0 - (total_budget / ideal_unconstrained))) if ideal_unconstrained > 0 else 0.0
        logger.info(f"Capital Stress Index: {capital_stress:.2f} (Ideal: KES {ideal_unconstrained:,.2f})")
 
        
        # v10.2: Removed shadow SKU limits to allow full category width in Supermarket/Mega tiers
        sku_counts_per_supplier = {}
        supplier_sku_limit = 9999 

        ctx.recommendations = recommendations
        ctx.summary = summary
        ctx.profile = profile
        ctx.is_small = is_small
        ctx.is_micro = is_micro
        ctx.fast_five_depts = fast_five_depts
        ctx.depth_cap_days = depth_cap_days
        ctx.max_total_packs = max_total_packs
        ctx.price_ceiling = price_ceiling
        ctx.min_display_qty = min_display_qty
        ctx.allow_c_class = allow_c_class
        ctx.alloc_cfg = alloc_cfg
        ctx.pass2_staple_share = pass2_staple_share
        ctx.wallets = wallets
        ctx.allowed_suppliers = allowed_suppliers
        ctx.pass1_cost = pass1_cost
        ctx.pass1_consignment_val = pass1_consignment_val
        ctx.sku_counts_per_dept = sku_counts_per_dept
        ctx.sku_counts_per_supplier = sku_counts_per_supplier
        ctx.supplier_sku_limit = supplier_sku_limit
        ctx.capital_stress = capital_stress

    def _gf_pass1_width(self, ctx) -> None:
        """Pass 1 (Width): list every viable SKU at minimum display depth."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        summary = ctx.summary
        is_small = ctx.is_small
        is_micro = ctx.is_micro
        max_total_packs = ctx.max_total_packs
        price_ceiling = ctx.price_ceiling
        min_display_qty = ctx.min_display_qty
        allow_c_class = ctx.allow_c_class
        wallets = ctx.wallets
        allowed_suppliers = ctx.allowed_suppliers
        pass1_cost = ctx.pass1_cost
        pass1_consignment_val = ctx.pass1_consignment_val
        sku_counts_per_dept = ctx.sku_counts_per_dept
        sku_counts_per_supplier = ctx.sku_counts_per_supplier
        supplier_sku_limit = ctx.supplier_sku_limit

        
        # v10.2: Track consolidation to ensure 'Zero-Idle Capital' efficiency
        
        for rec in recommendations:
            p_name = str(rec.get('product_name', ''))
            dept = str(rec.get('product_category', 'GENERAL')).upper()
            is_staple = bool(self.budget_manager.is_staple(p_name, dept, float(rec.get('avg_daily_sales', 0))))
            pack_size = float(rec.get('pack_size', 1))
            price = float(rec.get('selling_price', 0.0))
            is_consignment = bool(rec.get('is_consignment', False))
            
            abc_class = str(rec.get('ABC_Class', 'A'))
            is_essential_dept = dept in ESSENTIAL_DEPARTMENTS
            
            if not is_essential_dept:
                p_u = p_name.upper()
                if any(k in p_u for k in ["YOGHURT", "YOGURT", "SODA", "COKE", "ALVARO", "VIMTO", "GHEE", "LENTIL", "BEAN", "NDENGU", "POJO", "DAIRY"]):
                    is_essential_dept = True

            should_list = True
            reason_tag = ""

            # === CHAPTER 11: AMIT Gatekeeper ===
            # If AMIT engine is enabled, check if this SKU is blacklisted
            # (exceeds department cap with low GMROI). O(1) set lookup.
            # FIX H4: Normalize product name for robust matching against NN node IDs.
            amit_blacklist = self.databases.get('amit_enforcement', set())
            if amit_blacklist:
                p_name_upper = p_name.strip().upper()
                if p_name in amit_blacklist or p_name_upper in amit_blacklist:
                    should_list = False
                    reason_tag = "[AMIT: BLACKLISTED - Low GMROI, exceeds dept cap]"

            # === CHAPTER 11: MANDE Purge Enforcement ===
            supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
            if not supp or supp == 'NON': supp = 'UNKNOWN'
            mande_purge_list = self.databases.get('mande_purge_list', set())
            if should_list and supp in mande_purge_list:
                # Unless it's an essential or staple, block it entirely
                if not (is_staple or is_essential_dept):
                    should_list = False
                    reason_tag = "[MANDE: PURGE CANDIDATE - CAPITAL TRAP]"


            # v10.2: Align strict essentials-only filter to Micro-Duka threshold (200k KES)
            if total_budget < 200_000.0:
                # Micro stores ONLY allow Essentials/Staples/A-Class in Pass 1 to prevent "Filler"
                if not (is_staple or is_essential_dept) and abc_class != 'A':
                    should_list = False
                    reason_tag = "[PASS 1: STRICT MICRO FILTERING (No Filler)]"
            
            cost_price_est = float(self._get_actual_cost_price(rec, price))
            
            if dept not in sku_counts_per_dept: sku_counts_per_dept[dept] = 0
            
            if should_list and dept in allowed_suppliers:
                supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                if not supp or supp == 'NON': supp = 'UNKNOWN'
                if supp not in allowed_suppliers[dept]:
                    should_list = False
                    reason_tag = "[PASS 1: SUPPLIER CONSOLIDATION]"

            if dept in ['BAKERY FOODPLUS', 'BALERY FOODPLU']:
                should_list = False
                reason_tag = "[PASS 1: INTERNAL PRODUCTION - NOT PURCHASED]"
            
            # Definitions moved up to line 166
            
            p_name_upper = p_name.upper()
            is_bulk_item = any(x in p_name_upper for x in ['5KG', '5L', '5LT', '10KG', '10L', '20L', '25KG', '5 KG', '5 L', '10 KG'])
            
            effective_ceiling = price_ceiling * 3.0 if (is_essential_dept and is_bulk_item) else (price_ceiling * 2.0 if is_essential_dept else price_ceiling)
            
            if price > effective_ceiling:
                if is_staple:
                    reason_tag = "[PASS 1: ANCHOR OVERRIDE]"
                    should_list = True
                else:
                    reason_tag = f"[PASS 1: BLOCKED - PRICE > {effective_ceiling:.0f}]"
                    should_list = False
            
            if should_list and not allow_c_class and abc_class == 'C':
                avg_daily = float(rec.get('avg_daily_sales', 0))
                dead_stock_threshold = 0.02 if is_micro else 0.20
                if avg_daily < dead_stock_threshold and not is_essential_dept:
                    should_list = False
                    reason_tag = f"[PASS 1: DEAD STOCK < {dead_stock_threshold}]"

            # === MASTERCLASS: Halo & Affinity Guardian ===
            halo_list = self.databases.get('halo_protection_list', set())
            if halo_list:
                # If skipped for ANY reason other than AMIT/MANDE hard bans, protect Halo links
                if not should_list and "BLACKLISTED" not in reason_tag and "MANDE" not in reason_tag:
                    p_norm = p_name.upper().strip().replace('  ', ' ')
                    if p_norm in halo_list:
                        should_list = True
                        reason_tag = "[HALO: ANCHOR/AFFINITY PROTECTED]"


            # v9.5: Global Supplier SKU Limit Check
            if should_list and supplier_sku_limit < 999:
                supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                if not supp or supp == 'NON': supp = 'UNKNOWN'
                current_supp_count = sku_counts_per_supplier.get(supp, 0)
                if current_supp_count >= supplier_sku_limit:
                    if not is_staple: # Staples are always protected
                        should_list = False
                        reason_tag = f"[PASS 1: SKU LIMIT REACHED ({supplier_sku_limit} per Supplier)]"

            if should_list:
                mega_budget = 114_000_000.0
                budget_ratio = float(total_budget) / mega_budget
                # v10.2: Extreme conservativity in Pass 1 to prioritize 20,000+ SKU width
                mega_demand_proxy = float(rec.get('avg_daily_sales', 0)) * 7.0 # Reduced from 14.0 for Pass 1 Width
                scaled_demand_pre = mega_demand_proxy * budget_ratio
                
                has_lookalike = float(rec.get('lookalike_demand', 0)) > 0
                is_new_product = float(rec.get('avg_daily_sales', 0)) == 0
                scaled_threshold = max(0.01, 0.5 * math.sqrt(abs(float(budget_ratio))))
                             
                if is_small:
                    # v10.2: Align Mini-Mart threshold for variety expansion
                    eff_threshold = scaled_threshold * 0.1 if total_budget >= 1000000 else scaled_threshold
                    if scaled_demand_pre >= eff_threshold: pass
                    elif is_staple or is_essential_dept: reason_tag = "[PASS 1: ESSENTIAL BYPASS]"
                    elif is_new_product or has_lookalike: reason_tag = "[PASS 1: NEW PRODUCT - PROVISIONAL]"
                    else:
                        should_list = False
                        reason_tag = f"[SCALED DROP] Demand: {scaled_demand_pre:.2f} < {eff_threshold:.2f}"
                else:
                    # Standard / Mega tiers: Always allow valid SKUs to build full assortment (Shelf Fill)
                    pass
            
                if should_list:
                    # v10.2: BUG FIX - Use local variable for scaled ADS to avoid overwriting ground truth
                    # This ensures Pass 2 (Depth) sees the real demand for boosting.
                    rec['p1_scaled_ads'] = scaled_demand_pre
                    rec['reasoning'] = str(rec.get('reasoning', '')) + f" [ALLOCATION SHARE: {scaled_demand_pre:.1f}]"
                lead_time = float(rec.get('estimated_delivery_days', 4.0))
                daily_ads = float(rec.get('p1_scaled_ads', rec.get('avg_daily_sales', 0)))
                # BUG 3 FIX: Preserve original ADS before mutation so downstream passes
                # and safety guards can access the raw scorecard demand for auditing.
                if 'original_avg_daily_sales' not in rec:
                    rec['original_avg_daily_sales'] = float(rec.get('avg_daily_sales', 0))
                # v10.9: Intelligence Persistence — procurement loop ADS used for budget math
                rec['avg_daily_sales'] = round(daily_ads, 4)
                
                # FIX 1: Pass 1 Width + Launch Buffer for high-velocity items.
                # Guide spec: "If Demand > 5/day, adds Cycle + 2 Days to prevent Day 1 stockout."
                # Previously only staples/fresh got demand-based buffer — now high-velocity
                # non-staples also get a launch buffer proportional to their demand.
                is_fresh = bool(rec.get('is_fresh', False))
                if is_staple or is_fresh:
                    needed_days = min(lead_time + 0.5, 2.0) 
                    if is_fresh:
                         needed_days = min(lead_time + 1.2, 2.5)
                         if 'MILK' in dept: needed_days = 3.0
                    needed_qty = daily_ads * needed_days
                elif daily_ads > 5.0:
                    # FIX 1: High-velocity non-staples get launch buffer (Cycle + 2 days)
                    needed_days = min(lead_time + 2.0, 4.0)
                    needed_qty = daily_ads * needed_days
                elif daily_ads > 1.0:
                    # Medium-velocity items get a small buffer to prevent Day 1 stockout
                    needed_days = min(lead_time + 0.5, 2.0)
                    needed_qty = daily_ads * needed_days
                else:
                    needed_qty = 0 # Low-velocity items get MDQ only (Shelf Fill)
                    needed_days = 0 
                
                if bool(rec.get('is_fresh', False)):
                     try:
                         cycle_days = float(getattr(self, 'get_grn_cycle_days', lambda x: 2.0)(p_name))
                     except:
                         cycle_days = 2.0
                     if daily_ads > 10.0: needed_days = max(3.0, cycle_days + 2.0)
                     else: needed_days = cycle_days + 1.2 
                     if any(k in p_name_upper for k in ['UHT', 'ESL', 'LONG LIFE']):
                         needed_days = max(7.0, needed_days) 
                         
                # FIX 1 (cont): All items with demand-based buffer use the larger of MDQ or needed_qty.
                # Only truly low-velocity items (needed_qty == 0) fall back to MDQ shelf-fill.
                if needed_qty > 0:
                    raw_mdq = max(float(min_display_qty), needed_qty)
                else:
                    raw_mdq = float(min_display_qty)
                
                if bool(rec.get('is_fresh', False)):
                     # Fresh items should never have huge MDQ to avoid spoilage
                     raw_mdq = min(raw_mdq, needed_qty * 1.5) if needed_qty > 0 else raw_mdq
                    
                if not is_small and not is_micro:
                    velocity = daily_ads
                    if not (is_staple or is_essential_dept):
                        # v10.2: Preserve MDQ for very low velocity items to ensure "Look Full"
                        if velocity < 0.05: raw_mdq = max(1.0, float(raw_mdq) * 0.2) 
                        elif velocity < 0.2: raw_mdq = max(2.0, float(raw_mdq) * 0.50)
                
                # v10.1: Fix fallback logic for zero-valued MDQs to ensure raw_mdq is used
                mdq_base = float(rec.get('mdq') or raw_mdq)
                
                # Injection 3: Value-Density ($V_d$) Thresholds
                # If an item exceeds a specific Capital Threshold and its velocity is low, hard-cap it.
                weekly_vel = daily_ads * 7.0
                if cost_price_est > 5000.0 and weekly_vel < 1.0:
                    mdq_base = min(mdq_base, 2.0)
                    if "PREMIUM" not in reason_tag:
                        reason_tag += " [PREMIUM VALUE-DENSITY CAP]"
                
                mdq_base = float(math.ceil(mdq_base))
                
                # FIX 9: Relaxed Pass 1 budget limit for mid-range stores.
                # Previously 70% for 12M-50M stores starved Pass 1 width.
                # Now: Mega 95%, Standard 85%, Small 85% — aligned with guide spec.
                limit_pct = 0.95 if total_budget > 50_000_000 else 0.85
                pass1_limit = float(total_budget * limit_pct)
                
                if pass1_cost > pass1_limit:
                    if is_staple and not is_small: mdq_base = max(pack_size, float(mdq_base * 0.5))
                    else:
                        rec['recommended_quantity'] = 0
                        rec['reasoning'] = f"[PASS 1: BUDGET EXHAUSTED] Cap {limit_pct:.0%}. Width Cut."
                        rec['pass1_allocated'] = False
                        continue
                
                rec_qty_units = max(float(rec.get('moq_floor', 0)), float(mdq_base))
                is_break_bulk = (is_micro or is_small) and (rec_qty_units < pack_size or is_staple)
                
                # Value-Density Interception
                unit_cost = rec.get('cost_est', 0)
                if unit_cost > 5000:
                    rec_qty_units = min(rec_qty_units, 2.0)
                
                if is_break_bulk:
                    # R20 Fix: Fresh items should NOT be floor-capped to min_display_qty (3/4/6) 
                    # as it causes extreme overstocking for low-velocity fresh items.
                    if bool(rec.get('is_fresh', False)):
                        rec_qty_final = max(1, int(math.ceil(rec_qty_units))) # Shelf presence of at least 1pc
                    else:
                        rec_qty_final = max(int(rec.get('min_display_qty', 3)), int(math.ceil(rec_qty_units)))
                    if "BREAK BULK" not in reason_tag:
                        if 'reasoning' in rec: rec['reasoning'] += " [MICRO BREAK BULK]"
                        else: reason_tag += " [MICRO BREAK BULK]"
                else:
                    # Injection 1: Minimum Viable Depth (MVD) & Pack Size Snapping
                    pack_s = max(1, int(rec.get('pack_size', 1)))
                    # Value-Density Pack Size Adjust
                    if unit_cost > 2500 and pack_s >= 6:
                        pack_s = max(1, pack_s // 2)
                        if 'reasoning' in rec: rec['reasoning'] += " [VD PACK ADJUST]"
                        else: reason_tag += " [VD PACK ADJUST]"
                        
                    moq_f = max(pack_s, int(rec.get('moq_floor', 0)))
                    if rec_qty_units > 0 and rec_qty_units < moq_f and not is_staple:
                        rec_qty_final = 0
                        reason_tag += f" [MVD GATE: {rec_qty_units:.1f} < {moq_f}]"
                    else:
                        rec_qty_final = max(1, int(math.ceil(rec_qty_units / pack_s) * pack_s))
                
                max_allowed_units = max_total_packs * pack_size
                
                # v10.9: Total Guard Sync - Clamp Pass 1 Anchors to Retail Safety Limits (3.0d fresh, 90.0d dry)
                # This ensures we don't spend budget on "Display Stock" that will be pruned later.
                is_uht = any(x in p_name_upper for x in ["UHT", "LONG LIFE", "ESL"])
                # Greenfield Safe Caps (3.0d for fresh, 7.0d for UHT, 90.0d for dry)
                safe_limit_days = (7.0 if is_uht else 3.0) if rec.get('is_fresh', False) else 90.0
                safety_ceiling_units = max(1.0, float(daily_ads * safe_limit_days))
                
                # Combine physical shelf cap with retail safety cap
                effective_max_units = min(max_allowed_units, safety_ceiling_units)
                
                # v7.0 GAP-F FIX: Anchor override in Pass 1 (matches Pass 2 behavior)
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR'] and is_staple:
                    effective_max_units = safety_ceiling_units # Anchor override ignores shelf cap but respects safety
                    
                # v7.9 Fix: Fresh Items Exempt from Shelf Cap IN PASS 1
                if rec.get('is_fresh', False):
                     effective_max_units = max(effective_max_units, int(needed_qty * 1.1))
                    
                if rec_qty_final > effective_max_units:
                    rec_qty_final = max(1, int(round(effective_max_units)))
                    reason_tag += f" [PASS 1: SAFETY CLAMP @ {effective_max_units:.1f} Units]"
                # v10.9: Integer-Aware Quantity Sync (Ensures 100% precision between Engine and UI)
                rec['recommended_quantity'] = float(round(rec_qty_final))
                cost = round(rec['recommended_quantity'] * cost_price_est, 2)
                check_cost = 0 if is_consignment else cost
                if (pass1_cost + check_cost) > total_budget:
                    # v10.9: Partial Fill instead of Binary Zeroing
                    remaining_liquidity = max(0, total_budget - pass1_cost)
                    if remaining_liquidity < cost_price_est:
                         rec['recommended_quantity'] = 0
                         rec['reasoning'] = "[PASS 1: BUDGET EXHAUSTED]"
                         rec['pass1_allocated'] = False
                         continue
                    
                    affordable_q = int(remaining_liquidity // cost_price_est)
                    rec['recommended_quantity'] = affordable_q
                    rec['reasoning'] = f"[PASS 1: PARTIAL FILL] Only {affordable_q} units affordable."
                    cost = round(affordable_q * cost_price_est, 2)
                    check_cost = cost
                
                if not is_consignment:
                    rec['cost_price'] = cost_price_est
                    self.budget_manager.spend_from_wallet(wallets, dept, cost)
                    pass1_cost = round(pass1_cost + cost, 2)
                else:
                    pass1_consignment_val = round(pass1_consignment_val + cost, 2)
                
                # BUG 2 FIX: Only set default reasoning if the budget check didn't already
                # set a PARTIAL FILL or BUDGET EXHAUSTED tag (those paths set quantity directly).
                final_qty = rec['recommended_quantity']
                if "[PARTIAL FILL]" not in rec.get('reasoning', '') and "[BUDGET EXHAUSTED]" not in rec.get('reasoning', ''):
                    rec['recommended_quantity'] = rec_qty_final
                    final_qty = rec_qty_final
                    rec['reasoning'] = f"[PASS 1: WIDTH] Target: {mdq_base:.0f} -> {final_qty} Units"
                if is_consignment: rec['reasoning'] += " [CONSIGNMENT]"
                
                rec['pass1_allocated'] = True
                sku_counts_per_dept[dept] += 1
                
                # Track supplier SKU count
                supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                if not supp or supp == 'NON': supp = 'UNKNOWN'
                sku_counts_per_supplier[supp] = sku_counts_per_supplier.get(supp, 0) + 1
            else:
                rec['recommended_quantity'] = 0
                rec['reasoning'] = f"[PASS 1: SKIPPED] {reason_tag}"
                rec['pass1_allocated'] = False
                summary['total_skipped'] += 1
                logger.debug(f"Skipped {p_name} in Pass 1: {reason_tag}")
                skip_category = "price_ceiling" if "PRICE >" in reason_tag else ("dead_stock" if "DEAD STOCK" in reason_tag else ("low_demand" if "SCALED DROP" in reason_tag else ("supplier_consolidation" if "SUPPLIER CONSOLIDATION" in reason_tag else "other")))
                summary['skip_reasons'][skip_category] = summary['skip_reasons'].get(skip_category, 0) + 1

        logger.info(f"Pass 1 Complete. Committed: ${pass1_cost:,.2f}")

        ctx.pass1_cost = pass1_cost
        ctx.pass1_consignment_val = pass1_consignment_val

    def _gf_pass1_5_liquidity_prune(self, ctx) -> None:
        """Pass 1.5: prune the Pass-1 tail to recover liquidity for critical depth (small stores only)."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        is_small = ctx.is_small
        pass1_cost = ctx.pass1_cost

        
        remaining_liquidity = total_budget - pass1_cost
        critical_depth_need = (total_budget * 0.05 if total_budget < 12_000_000 else total_budget * 0.15) if is_small else 0.0
        shortfall = critical_depth_need - remaining_liquidity
        
        if shortfall > 0 and is_small:
            logger.warning(f"Pass 1.5: Liquidity Shortfall ${shortfall:,.2f}. Pruning Pass 1 Tail.")
            prune_candidates = [r for r in recommendations if r.get('pass1_allocated') and not self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0)) and r.get('product_category', 'GENERAL').upper() not in ESSENTIAL_DEPARTMENTS]
            prune_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0))
            pruned_count = 0
            reclaimed_cash = 0.0
            for rec in prune_candidates:
                if reclaimed_cash >= shortfall: break
                # v2.9: Precise cost calculation (Logic Ghost Fix #19)
                cost_est = rec['recommended_quantity'] * self._get_actual_cost_price(rec, float(rec.get('selling_price', 0.0)))
                rec['recommended_quantity'] = 0
                rec['pass1_allocated'] = False
                rec['reasoning'] += " [PRUNED: LIQUIDITY RECOVERY]"
                if not rec.get('is_consignment', False):
                    reclaimed_cash = round(reclaimed_cash + cost_est, 2)
                    pass1_cost = round(pass1_cost - cost_est, 2) 
                pruned_count += 1
            logger.info(f"Pass 1.5 Pruning Complete. Pruned {pruned_count} items. Reclaimed ${reclaimed_cash:,.2f}")

        ctx.pass1_cost = pass1_cost

    def _gf_pass2_depth(self, ctx) -> None:
        """Pass 2 (Depth): round-robin depth allocation — Fast Five reservation, staples, then discretionary."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        profile = ctx.profile
        is_small = ctx.is_small
        is_micro = ctx.is_micro
        fast_five_depts = ctx.fast_five_depts
        pass2_staple_share = ctx.pass2_staple_share
        wallets = ctx.wallets
        pass1_cost = ctx.pass1_cost
        capital_stress = ctx.capital_stress

        
        pass2_cost = 0.0
        candidates = [r for r in recommendations if r.get('pass1_allocated') and r['recommended_quantity'] > 0]
        fast_five_candidates = [r for r in candidates if is_small and r.get('product_category','').upper() in fast_five_depts and self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0))]
        other_staple_candidates = [r for r in candidates if self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0)) and r not in fast_five_candidates]
        discretionary_candidates = [r for r in candidates if not self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0))]
        
        fast_five_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        other_staple_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        discretionary_candidates.sort(key=lambda x: x.get('avg_daily_sales', 0), reverse=True)
        
        total_remaining_budget = total_budget - pass1_cost
        
        # R18: Fast Five Reservation (Golden Parity)
        fast_five_reservation = 0.0
        if is_small:
            target_fast_five_total = total_budget * 0.60
            current_fast_five_spend = sum([wallets[d]['spent'] for d in fast_five_depts if d in wallets])
            fast_five_reservation = max(0, target_fast_five_total - current_fast_five_spend)
            if fast_five_reservation > 0:
                logger.info(f"Duka Mode: Reserving ${fast_five_reservation:,.2f} for Fast Five Depth.")
        
        # R19: Pass 2 budget distribution logging (Golden Parity)
        staple_allocation_target = total_remaining_budget * pass2_staple_share
        discretionary_hard_cap = total_remaining_budget * (1.0 - pass2_staple_share)
        logger.info(f"Pass 2 Budget: ${total_remaining_budget:,.2f} (Staples Target: ${staple_allocation_target:,.2f}, Discretionary Cap: ${discretionary_hard_cap:,.2f})")
        
        def allocate_list_constrained(candidate_list, phase_cap, phase_name, t_profile, is_grn_mode=False):
            queue = []
            for rec in candidate_list:
                dept = rec.get('product_category', 'GENERAL').upper()
                avg_sales = rec.get('avg_daily_sales', 0.0)
                effective_avg_sales = avg_sales
                new_product_mode = False
                if avg_sales <= 0:
                    lookalike = rec.get('lookalike_demand', 0.0)
                    if lookalike > 0:
                        effective_avg_sales = lookalike * 0.5
                    else:
                        c_price = float(rec.get('selling_price', 150.0)) or 150.0
                        # Dynamic MVP: Scrutinize price to estimate synthetic base velocity
                        base_vel = max(0.1, min(2.5, 60.0 / c_price))
                        is_itm_staple = self.budget_manager.is_staple(rec['product_name'], dept, 0)
                        if is_itm_staple:
                            base_vel *= 1.4
                        if rec.get('is_fresh', False):
                            base_vel = max(0.3, base_vel * 1.2)
                        effective_avg_sales = base_vel
                    new_product_mode = True
                    rec['reasoning'] = str(rec.get('reasoning', '')) + " [NEW PRODUCT]"

                smart_target_days = self.calculate_replenishment_target_stock(rec, t_profile)
                effective_days = min(smart_target_days, 14.0) if new_product_mode else smart_target_days
                if rec.get('is_fresh', False): rec['reasoning'] = str(rec.get('reasoning', '')) + " [JIT FRESH]"
                
                pack_size = int(rec.get('pack_size', 1))
                ideal_qty = max(1, math.ceil(effective_avg_sales * effective_days))  # FIX 2: ceil() prevents truncation of low-velocity demand
                min_pack_floor = (12 if float(rec.get('selling_price', 0)) < 50 else 6) if (is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']) else 1
                if dept in FRESH_DEPARTMENTS: min_pack_floor = 1 
                
                # R20 Fix: Fresh items should NOT be floor-capped to min_display_qty (3/4/6) 
                # as it causes extreme overstocking for low-velocity fresh items.
                if bool(rec.get('is_fresh', False)):
                    floor_qty = min_pack_floor
                else:
                    floor_qty = max(min_pack_floor, int(rec.get('min_display_qty', 3))) if (is_micro or is_small) else (min_pack_floor * pack_size)
                max_total_packs_val = int(t_profile.get('max_packs', 10))
                max_allowed_units = max_total_packs_val * pack_size
                is_fast_five = dept in FAST_FIVE_DEPARTMENTS and self.budget_manager.is_staple(rec['product_name'], dept, avg_sales)
                
                if rec['avg_daily_sales'] > 2.0: max_allowed_units = int(max_allowed_units * 1.5)
                if rec.get('is_fresh', False): max_allowed_units = max(max_allowed_units, int(ideal_qty * 1.1)) 
                if (is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']) or is_fast_five: max_allowed_units = max(max_allowed_units, 9999)
                elif total_budget >= 20_000_000: max_allowed_units = 99999999
                elif effective_avg_sales > 1.0: max_allowed_units = max(max_allowed_units, int(effective_avg_sales * effective_days))
                
                final_target = min(max(ideal_qty, floor_qty), max_allowed_units)
                if rec['recommended_quantity'] < final_target:
                    c_est = self._get_actual_cost_price(rec, float(rec.get('selling_price', 0.0)))
                    # BUG 8 FIX: For small/micro stores, use 1-unit increments (break-bulk)
                    # to prevent depth overshoot by up to (pack_size - 1) units.
                    effective_pk = 1 if (is_small or is_micro) else pack_size
                    queue.append({'rec': rec, 'dept': dept, 'pack_size': effective_pk, 'cost_per_pack': effective_pk * c_est, 'target_qty': final_target, 'cost_est': c_est})

            p_cost = 0.0
            p_consignment = 0.0
            active = True
            while active and queue:
                active = False
                for i in range(len(queue) - 1, -1, -1):
                    item = queue[i]
                    rec = item['rec']
                    dept = item['dept']
                    pk_cost = item['cost_per_pack']
                    pk_size = item['pack_size']
                    is_it_consignment = rec.get('is_consignment', False)
                    if not is_it_consignment and (p_cost + pk_cost) > phase_cap:
                        rec['reasoning'] += f" [{phase_name} CAP]"
                        queue.pop(i)
                        continue
                    is_priority = (phase_name == "PRIORITY")
                    if not is_priority:
                        # v10.9: Elastic Share Caps
                        # For large store builds, we allow higher category penetration 
                        # to ensure capital is fully deployed into variety.
                        # Injection 7: Capital Stress multiplier on Category Ceilings
                        base_limit = 0.40 if is_small else 0.65
                        if total_budget >= 20_000_000:
                             w_limit = 0.85 # Relaxed Cap for Scale
                        else:
                             w_limit = base_limit
                        
                        w_limit = w_limit * (1.0 - capital_stress)
                        
                        max_item_spend = wallets.get(dept, {}).get('allocated_budget', 0) * w_limit if dept in wallets else 99999999.0
                        current_item_spend = rec['recommended_quantity'] * item['cost_est']
                        if rec['recommended_quantity'] > 0 and (current_item_spend + pk_cost) > max_item_spend:
                            if rec.get('pass1_allocated'): rec['reasoning'] += " [SHARE CAP]"
                            queue.pop(i)
                            continue
                    can_spend = True
                    if not is_priority and dept in wallets:
                        # FIX 8: Wallet spillover — if department wallet is empty, try GENERAL pool
                        if not self.budget_manager.check_wallet_availability(wallets, dept, pk_cost):
                            if self.budget_manager.check_wallet_availability(wallets, 'GENERAL', pk_cost):
                                dept = 'GENERAL'  # Spillover to general pool
                            else:
                                can_spend = False

                    if can_spend:
                        # v10.9: Greenfield uses integer precision (1 unit at a time)
                        # whereas replenishment uses pack rounding floor/ceiling logic.
                        rec['recommended_quantity'] = float(round(rec.get('recommended_quantity', 0) + pk_size))
                        # v10.7: Consolidated Firewall. ALL non-consignment items must spend from category wallets to prevent drift.
                        if not is_it_consignment and dept in wallets: 
                            # v10.9: Cache Cost Price for Audit Sync
                            rec['cost_price'] = item['cost_est']
                            self.budget_manager.spend_from_wallet(wallets, dept, pk_cost)
                        
                        if not is_it_consignment:
                            p_cost = round(p_cost + pk_cost, 2)
                        else:
                            p_consignment = round(p_consignment + pk_cost, 2)
                        
                        rec['pass2_allocated'] = True
                        if "[PASS 2]" not in rec.get('reasoning', ''): 
                            rec['reasoning'] = rec.get('reasoning', '') + " [PASS 2]"
                        
                        active = True
                        if rec['recommended_quantity'] >= item['target_qty']: queue.pop(i)
            return p_cost, p_consignment
        added_ff_cost, added_ff_cons = allocate_list_constrained(fast_five_candidates, total_remaining_budget, "PRIORITY", profile, is_grn_mode=True)
        remaining_after_ff = total_remaining_budget - added_ff_cost
        
        # v10.7: Allow Staple spillover into Discretionary (Zero-Waste Logic)
        staple_target = remaining_after_ff * pass2_staple_share
        added_other_staple_cost, added_other_staple_cons = allocate_list_constrained(other_staple_candidates, staple_target, "STAPLE", profile, is_grn_mode=True)
        
        # Discretionary gets what's left of the total remaining pool
        remaining_for_disc = remaining_after_ff - added_other_staple_cost
        added_disc_cost, added_disc_cons = allocate_list_constrained(discretionary_candidates, remaining_for_disc, "DISC", profile, is_grn_mode=True)
        
        pass2_cost = added_ff_cost + added_other_staple_cost + added_disc_cost
        p2_consignment_val = added_ff_cons + added_other_staple_cons + added_disc_cons
        logger.info(f"Pass 2 Complete. Added Depth: ${pass2_cost:,.2f}")

        ctx.pass2_cost = pass2_cost
        ctx.p2_consignment_val = p2_consignment_val
        ctx.candidates = candidates

    def _gf_premium_trim(self, ctx) -> None:
        """Premium trim: enforce per-category capital ceilings on high-cost items, with demand transfer to substitutes (Injections 5-7)."""
        recommendations = ctx.recommendations
        wallets = ctx.wallets
        capital_stress = ctx.capital_stress
        pass2_cost = ctx.pass2_cost

        
        # Injection 5 & 7: Hard Budget Ceilings per Category (Premium Trim Loop)
        premium_cap_base = 0.20
        dynamic_premium_cap = premium_cap_base * (1.0 - capital_stress)
        for d, w in wallets.items():
            cat_budget = w.get('allocated_budget', 0)
            if cat_budget > 0:
                premium_limit = cat_budget * dynamic_premium_cap
                cat_items = [r for r in recommendations if r.get('product_category', 'GENERAL').upper() == d and r.get('cost_price', 0) > 5000.0 and r.get('recommended_quantity', 0) > 0 and not r.get('is_consignment')]
                cat_items.sort(key=lambda x: float(x.get('avg_daily_sales', 0))) # Weakest first
                
                premium_spend = sum(r['recommended_quantity'] * r['cost_price'] for r in cat_items)
                
                while premium_spend > premium_limit and cat_items:
                    weakest = cat_items[0]
                    pk_s = max(1, int(weakest.get('pack_size', 1)))
                    moq_floor = max(pk_s, int(weakest.get('moq_floor', 0)))
                    if weakest['recommended_quantity'] > moq_floor:
                        weakest['recommended_quantity'] -= pk_s
                        premium_spend -= (pk_s * weakest['cost_price'])
                        pass2_cost -= (pk_s * weakest['cost_price'])
                        weakest['reasoning'] += " [PREMIUM TRIM]"
                    else:
                        premium_spend -= (weakest['recommended_quantity'] * weakest['cost_price'])
                        pass2_cost -= (weakest['recommended_quantity'] * weakest['cost_price'])
                        
                        # Injection 6: Demand Transfer (Consolidation)
                        # Transfer 60% of demand to the primary substitute (the item with highest ADS in the same category)
                        subs = [s for s in recommendations if s.get('product_category', 'GENERAL').upper() == d and s != weakest and s.get('recommended_quantity', 0) > 0]
                        if subs:
                            best_sub = max(subs, key=lambda x: float(x.get('avg_daily_sales', 0)))
                            best_sub['avg_daily_sales'] += float(weakest.get('avg_daily_sales', 0)) * 0.60
                            best_sub['reasoning'] += " [DEMAND TRANSFERRED]"
                            
                        weakest['recommended_quantity'] = 0
                        weakest['reasoning'] += " [PREMIUM CAP ZEROED]"
                        cat_items.pop(0)

        ctx.pass2_cost = pass2_cost

    def _gf_pass2b_flex_pool(self, ctx) -> None:
        """Pass 2B: redistribute surplus liquidity to high-ROI items (Flex Pool)."""
        total_budget = ctx.total_budget
        summary = ctx.summary
        depth_cap_days = ctx.depth_cap_days
        pass1_cost = ctx.pass1_cost
        pass2_cost = ctx.pass2_cost
        candidates = ctx.candidates


        # Pass 2B: Flex Pool Redistribution [Golden v10.0 / Gap-11 Fix]
        actual_spent = pass1_cost + pass2_cost
        true_unused = total_budget - actual_spent
        unused_pct = (true_unused / total_budget * 100) if total_budget > 0 else 0
        
        p2b_cost = 0.0
        p2b_consignment_val = 0.0
        flex_pool_transactions = [] 
        items_enhanced = 0
        
        if true_unused > (total_budget * 0.005): # v10.7: Lowered from 5% to 0.5% to capture small surpluses
            logger.info(f"Pass 2B: Flex Pool Active. Liquidity Surplus: ${true_unused:,.2f} ({unused_pct:.1f}%).")
            
            flex_candidates = []
            for r in candidates:
                if r['recommended_quantity'] > 0:
                    # FIX 4: Allow fresh items in Flex Pool with shelf-life guard.
                    # Previously ALL fresh items were excluded, blocking high-velocity
                    # perishables (milk, bread) from receiving depth boosts.
                    # Now: fresh items can participate but capped at cycle + 2 days.
                    is_item_fresh = r.get('is_fresh', False)
                         
                    is_staple = self.budget_manager.is_staple(r['product_name'], r.get('product_category'), r.get('avg_daily_sales', 0))
                    abc_class = r.get('ABC_Class', 'B')
                    # FIX 10: Broadened Flex Pool eligibility. Previously only staple/A-class
                    # could receive depth boost, leaving 40%+ of budget stranded.
                    # Now: B-class items and any item with meaningful demand (ADS > 0.5) can participate.
                    item_ads = float(r.get('avg_daily_sales', 0))
                    is_priority = is_staple or abc_class in ('A', 'B') or item_ads > 0.5
                    
                    if is_priority:
                        avg_sales = float(r.get('avg_daily_sales', 0.0))
                        if avg_sales <= 0:
                            p_price = float(r.get('selling_price', 150.0)) or 150.0
                            avg_sales = max(0.1, min(2.5, 60.0 / p_price))
                            if is_staple: avg_sales *= 1.4
                        
                        ideal_days = depth_cap_days
                        dept_upper = r.get('product_category', 'GENERAL').upper()
                        if is_item_fresh or dept_upper in FRESH_DEPARTMENTS:
                             # FIX 4: Fresh items get cycle + 2 days as flex pool depth cap
                             lead_time = int(r.get('estimated_delivery_days', 1))
                             ideal_days = min(ideal_days, lead_time + 2.0)
                        else:
                             shelf_life = r.get('shelf_life_days', 365)
                             if shelf_life < 30:
                                 ideal_days = min(ideal_days, max(1, shelf_life - 2))
                        
                        ideal_qty = int(avg_sales * ideal_days)
                        current_qty = r['recommended_quantity']
                        
                        if current_qty < ideal_qty:
                            headroom = max(0, ideal_qty - current_qty)
                            roi_score = avg_sales * (float(r.get('margin_pct', 20.0)) / 100.0)
                            
                            flex_candidates.append({
                                'rec': r,
                                'headroom': headroom,
                                'roi_score': roi_score,
                                'cost_price': self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                            })
            
            # Sort by ROI Score (Highest value first)
            flex_candidates.sort(key=lambda x: x['roi_score'], reverse=True)
            
            f_pool = true_unused
            # v10.0 Summary Metrics
            summary['flex_pool_available'] = true_unused
            
            for cand in flex_candidates:
                if f_pool <= 0: break # v10.8: Removed 1% buffer to achieve 100% utilization
                
                rec = cand['rec']
                p_s = int(rec.get('pack_size', 1))
                c_p = cand['cost_price']
                
                # Add packs until headroom filled or budget exhausted
                add_q = 0
                # v10.7: Consignment Firewall - Free stock does not consume the Flex surplus (f_pool)
                is_c = cand['rec'].get('is_consignment', False)
                while (add_q + p_s) <= cand['headroom'] and (is_c or (p_s * c_p) <= f_pool):
                    add_q += p_s
                    if not is_c:
                        f_pool -= (p_s * c_p)
                
                if add_q > 0:
                    rec['recommended_quantity'] += add_q
                    new_cost = add_q * c_p
                    if not is_c:
                        p2b_cost = round(p2b_cost + new_cost, 2)
                    else:
                        p2b_consignment_val = round(p2b_consignment_val + new_cost, 2)
                    rec['reasoning'] += f" [FLEX POOL: +{add_q}]"
                    items_enhanced += 1
                    # Track all transactions for audit
                    flex_pool_transactions.append({"item": rec['product_name'], "boost": add_q, "cost": round(add_q * c_p, 2)})
            
            summary['pass2b_items_enhanced'] = items_enhanced
            
            # Log top 5 beneficiaries for audit trail
            top_beneficiaries = sorted(flex_pool_transactions, key=lambda x: x['cost'], reverse=True)[:5]
            if top_beneficiaries:
                logger.info("Top Flex Pool Beneficiaries:")
                for b in top_beneficiaries:
                    logger.info(f"  - {b['item']}: +{b['boost']} units (${b['cost']:,.2f})")

            logger.info(f"Pass 2B Complete. Depth Boosted for {items_enhanced} items. Cost: ${p2b_cost:,.2f}")

        ctx.p2b_cost = p2b_cost
        ctx.p2b_consignment_val = p2b_consignment_val

    def _gf_pass3_anchor_mov(self, ctx) -> None:
        """Pass 3 (Anchor MOV): prune sub-MOV suppliers and redeploy the reclaimed capital to anchor suppliers (small stores only)."""
        recommendations = ctx.recommendations
        is_small = ctx.is_small
        is_micro = ctx.is_micro
        alloc_cfg = ctx.alloc_cfg
        pass1_cost = ctx.pass1_cost
        pass2_cost = ctx.pass2_cost


        if is_small:
            mov_threshold = alloc_cfg.mov_threshold_micro if is_micro else alloc_cfg.mov_threshold_small
            supp_spend = {}
            for rec in recommendations:
                if rec['recommended_quantity'] > 0:
                    s = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    supp_spend[s] = supp_spend.get(s, 0) + (self._get_actual_cost_price(rec, float(rec.get('selling_price', 0))) * rec['recommended_quantity'])
            
            pruned_val = 0.0
            for rec in recommendations:
                if rec['recommended_quantity'] > 0 and not rec.get('is_consignment') and not rec.get('is_fresh'):
                    s = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                    if supp_spend.get(s, 0) < mov_threshold:
                        c_est = self._get_actual_cost_price(rec, float(rec.get('selling_price', 0))) * rec['recommended_quantity']
                        rec['recommended_quantity'] = 0
                        rec['reasoning'] += f" [ANCHOR PRUNE: < ${mov_threshold}]"
                        pruned_val += c_est
                        if rec.get('pass1_allocated'): pass1_cost = round(pass1_cost - c_est, 2)
                        else: pass2_cost = round(pass2_cost - c_est, 2)
            
            if pruned_val > 0:
                viable = {k: v for k, v in supp_spend.items() if v >= mov_threshold}
                anchors = sorted(viable.items(), key=lambda x: x[1], reverse=True)[:3]
                anchor_names = [x[0] for x in anchors]
                if anchor_names:
                    a_cands = []
                    for rec in recommendations:
                        supp = str(rec.get('supplier_name', 'UNKNOWN')).upper().strip()
                        if supp in anchor_names and rec['recommended_quantity'] > 0 and not rec.get('is_fresh', False):
                            avg_sales = rec.get('avg_daily_sales', 0.1)
                            h_room = max(0, int(avg_sales * 45) - rec['recommended_quantity'])
                            if h_room > 0: 
                                a_cands.append({
                                    'rec': rec, 
                                    'headroom': h_room, 
                                    'roi_score': avg_sales * (float(rec.get('margin_pct', 20.0)) / 100.0)
                                })
                    
                    # Allocate to Anchors by ROI Score
                    a_cands.sort(key=lambda x: x['roi_score'], reverse=True)
                    for cand in a_cands:
                        if pruned_val <= 0: break
                        rec = cand['rec']
                        c_p = self._get_actual_cost_price(rec, float(rec.get('selling_price', 0)))
                        add_q = min(cand['headroom'], int(pruned_val / c_p) if c_p > 0 else 0)
                        if add_q > 0:
                            rec['recommended_quantity'] += add_q
                            pruned_val = round(pruned_val - (add_q * c_p), 2)
                            rec['reasoning'] += f" [ANCHOR BOOST: +{add_q}]"

        ctx.pass1_cost = pass1_cost
        ctx.pass2_cost = pass2_cost

    def _gf_pass4_mop_up(self, ctx) -> None:
        """Pass 4 (Mop-Up): zero-idle capital — universal re-entry, depth absorption, then zero-cent exhaustion."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        profile = ctx.profile


        # Pass 4: Mop-Up [FIX 3 + FIX 11: Zero-Idle Capital]
        # Guide: "Every cent should be deployed." No percentage gate.
        mop_up_cost = 0.0
        mop_up_consignment_val = 0.0
        
        # v10.9: PERFECT SYNC - Recalculate 'final_unused' from the ACTUAL product list 
        # instead of drifting accumulators. This prevents counter-leakage.
        current_spent = 0.0
        for r in recommendations:
            q = r.get('recommended_quantity', 0)
            if q > 0 and not r.get('is_consignment', False):
                current_spent += round(q * self._get_actual_cost_price(r, float(r.get('selling_price', 0))), 2)
        
        final_unused = round(total_budget - current_spent, 2)
        
        if final_unused > 100:  # Any meaningful capital remaining
            logger.info(f"Pass 4 (Mop-Up): ${final_unused:,.2f} remaining. Deploying to high-ROI items.")
            
            # --- PHASE 4A: UNIVERSAL RE-ENTRY (Initial Load Assurance) ---
            # v10.7: Expanded to reconsider ALL non-blacklisted items with 0 qty.
            # This captures items orphaned by Pass 1 budget caps or Pass 1.5 liquidity pruning.
            if final_unused > 100:
                logger.info(f"Universal Re-entry: Re-evaluating blocked/pruned SKUs. Surplus: ${final_unused:,.2f}")
                reentry_candidates = []
                for r in recommendations:
                    if r.get('recommended_quantity', 0) == 0:
                        reasoning = r.get('reasoning', '')
                        avg_sales = float(r.get('avg_daily_sales', 0.0))
                        
                        # Eligible for re-entry if not blacklisted/purged
                        if avg_sales > 0 and "[BLOCKED]" not in reasoning and "[PURGE]" not in reasoning:
                            # 1. Price Ceiling blocks: Only re-enter if surplus is high (>0.5%)
                            is_price_blocked = "PRICE >" in reasoning
                            if is_price_blocked and final_unused <= (total_budget * 0.005):
                                continue
                                
                            reentry_candidates.append(r)
                
                # Sort by ROI Score
                reentry_candidates.sort(key=lambda x: (float(x.get('avg_daily_sales', 0)) * (float(x.get('margin_pct', 20)) / 100.0)), reverse=True)
                
                for r in reentry_candidates:
                    p_size = int(r.get('pack_size', 1))
                    c_p = self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                    min_cost = p_size * c_p
                    if min_cost <= final_unused:
                        is_price_blocked = "PRICE >" in r.get('reasoning', '')
                        # Give it a safe initial load (MDQ or 7 days)
                        # Price blocked items get a more conservative 3-day load to buy variety
                        depth = 3 if is_price_blocked else 7
                        target_q = max(p_size, int(math.ceil(float(r.get('avg_daily_sales', 0)) * depth)))
                        target_q = min(target_q, profile.get('max_packs', 10) * p_size)
                        
                        is_re_consignment = r.get('is_consignment', False)
                        # v10.9: Cache Cost Price for Audit Sync
                        r['cost_price'] = c_p
                        cost = round(target_q * c_p, 2)
                        if is_re_consignment or cost <= final_unused:
                            r['recommended_quantity'] = target_q
                            r['reasoning'] = f"[PASS 4: RE-ENTRY] Assortment Expansion. Target: {target_q}"
                            if not is_re_consignment:
                                final_unused = round(final_unused - cost, 2)
                                mop_up_cost = round(mop_up_cost + cost, 2)
                            else:
                                mop_up_consignment_val = round(mop_up_consignment_val + cost, 2)
                            logger.info(f"  + Universal Re-entry: {r['product_name']} ({target_q} units, ${cost:,.2f})")

            # --- PHASE 4B: DEPTH ABSORPTION ---
            mop_candidates = []
            for r in recommendations:
                # FIX 11: Broadened from 0.5 to 0.1 ADS
                is_fresh_mop = r.get('is_fresh', False)
                avg_sales = float(r.get('avg_daily_sales', 0.0))
                
                # FIX 11: Broadened from 0.1 to 0.01 ADS (Retail Tail Inclusion)
                thresh = 1.0 if is_fresh_mop else 0.01
                
                if r['recommended_quantity'] > 0 and avg_sales >= thresh:
                    is_staple = self.budget_manager.is_staple(r['product_name'], r.get('product_category'), avg_sales)
                    
                    if avg_sales <= 0: # Safety fallback for synthetic demand
                        p_price = float(r.get('selling_price', 150.0)) or 150.0
                        avg_sales = max(0.1, min(2.5, 60.0 / p_price))
                    
                    roi_score = avg_sales * (float(r.get('margin_pct', 20.0)) / 100.0)
                    # v10.9: PERFECT SYNC - Aligned Fresh Mop-Up with Guard logic (3.0d)
                    is_uht = any(x in r.get('product_name', '').upper() for x in ["UHT", "LONG LIFE", "ESL"])
                    mop_limit_days = (7.0 if is_uht else 3.0) if is_fresh_mop else 90
                    
                    headroom = max(0, int(avg_sales * mop_limit_days) - r['recommended_quantity'])
                    
                    if headroom > 0:
                        mop_candidates.append({
                            'rec': r,
                            'headroom': headroom,
                            'roi_score': roi_score,
                            'cost_price': self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                        })
            
            # Sort by ROI Score
            mop_candidates.sort(key=lambda x: x['roi_score'], reverse=True)
            
            # --- PHASE 4B: DEPTH MOP-UP (Targeting 90 Days) ---
            for cand in mop_candidates:
                if final_unused <= 100: break
                
                rec = cand['rec']
                c_p = cand['cost_price']
                
                is_m_consignment = rec.get('is_consignment', False)
                if c_p > 0 and (is_m_consignment or c_p <= final_unused):
                    # Take as many units as possible within headroom and budget
                    target_units = int(cand['headroom'])
                    if not is_m_consignment:
                        max_units_possible = int(final_unused / c_p)
                        units_to_take = min(max_units_possible, target_units)
                    else:
                        units_to_take = target_units
                    
                    if units_to_take > 0:
                        item_cost = round(units_to_take * c_p, 2)
                        rec['recommended_quantity'] += units_to_take
                        if not is_m_consignment:
                            final_unused = round(final_unused - item_cost, 2)
                            mop_up_cost = round(mop_up_cost + item_cost, 2)
                        else:
                            mop_up_consignment_val = round(mop_up_consignment_val + item_cost, 2)
                        rec['reasoning'] += f" [MOP-UP: +{units_to_take}]"

            logger.info(f"Pass 4 (Mop Depth) Complete. Cash Invested: ${mop_up_cost:,.2f}. Surplus: ${final_unused:,.2f}")
            
            # --- PHASE 4C: ZERO-CENT EXHAUSTION (Force 100% Utilization) ---
            # v10.6: If capital remains stranded after 90-day targets, ignore depth caps
            # and iteratively add 1 unit to top ROI staples until budget hits zero.
            if final_unused > 100:
                logger.info(f"Phase 4C: Executing Zero-Cent Exhaustion on ${final_unused:,.2f} surplus.")
                # Focus on a tight pool of high-demand staples for maximum ROI on over-stock
                # v10.9: Lowered ROI threshold from 0.1 to 0.01 to prevent capital starvation
                exhaust_pool = [c for c in mop_candidates if c['roi_score'] > 0.01 or c['rec'].get('is_key_sku')]
                
                if not exhaust_pool:
                    # Emergency recovery: Select any allocated item with non-zero sales
                    for r in recommendations:
                        if r.get('recommended_quantity', 0) > 0:
                            avg_s = float(r.get('avg_daily_sales', 0.1))
                            roi = avg_s * (float(r.get('margin_pct', 20.0)) / 100.0)
                            if roi > 0.1:
                                exhaust_pool.append({
                                    'rec': r,
                                    'roi_score': roi,
                                    'cost_price': self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                                })
                    exhaust_pool.sort(key=lambda x: x['roi_score'], reverse=True)
                    # v10.9: Scale pool size for Mega-tier stores (10M+ budget)
                    max_pool = 1000 if total_budget > 10_000_000 else 250
                    exhaust_pool = exhaust_pool[:max_pool]
                
                safety_limit = 0
                while final_unused > 0 and safety_limit < 30000:
                    added_in_round = False
                    for cand in exhaust_pool:
                        is_ex_consignment = cand['rec'].get('is_consignment', False)
                        c_p = cand['cost_price']
                        
                        # v10.9: DYNAMIC CEILING RELAXATION (Per User Instruction)
                        # Relax 90d to 120d for Dry goods if budget is still stranded (>1%)
                        residual_pct = (final_unused / total_budget) if total_budget > 0 else 0
                        # BUG 1 FIX: Derive fresh/UHT/qty/sales from the candidate record (were undefined)
                        is_f = cand['rec'].get('is_fresh', False)
                        is_u = any(x in cand['rec'].get('product_name', '').upper() for x in ['UHT', 'LONG LIFE', 'ESL'])
                        current_q = float(cand['rec'].get('recommended_quantity', 0))
                        avg_s = max(0.01, float(cand['rec'].get('avg_daily_sales', 0.01)))
                        baseline_limit = (7.0 if is_u else 3.0) if is_f else 90.0
                        
                        # Apply 120d relaxation only for non-fresh items in Greenfield Exhaustion
                        limit_days = 120.0 if (not is_f and residual_pct > 0.01) else baseline_limit
                        
                        if (current_q + 1) > (avg_s * limit_days):
                            continue # Skip - will be pruned by guards anyway
                            
                        if c_p > 0 and (is_ex_consignment or c_p <= final_unused):
                            cand['rec']['recommended_quantity'] += 1
                            # v10.9: Cache Cost Price for Audit Sync
                            cand['rec']['cost_price'] = c_p
                            item_cost = c_p
                            if not is_ex_consignment:
                                final_unused = round(final_unused - item_cost, 2)
                                mop_up_cost = round(mop_up_cost + item_cost, 2)
                            else:
                                mop_up_consignment_val = round(mop_up_consignment_val + item_cost, 2)
                            added_in_round = True
                        if final_unused <= 0: break
                    
                    if not added_in_round:
                        # v10.9: UNIVERSAL SWEEP - Final attempt to find ANY item that fits
                        for r in recommendations:
                            if final_unused <= 0: break
                            # v10.9: EXHAUSTIVE UNIVERSAL SWEEP (BREAK BULK)
                            # Per instruction: Allow 1-unit increments in Greenfield to hit 100%
                            if r.get('recommended_quantity', 0) > 0 and not r.get('is_consignment'):
                                c_p = r.get('cost_price')
                                if c_p is None:
                                    c_p = self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                                
                                if 0 < c_p <= final_unused:
                                    r['recommended_quantity'] += 1
                                    r['cost_price'] = c_p
                                    final_unused = round(final_unused - c_p, 2)
                                    mop_up_cost = round(mop_up_cost + c_p, 2)
                                    added_in_round = True
                        
                        if not added_in_round: break # Truly no items fit
                    safety_limit += 1
            
            logger.info(f"Pass 4 Complete. Final Cash Invested: ${mop_up_cost:,.2f}")

        ctx.mop_up_cost = mop_up_cost
        ctx.mop_up_consignment_val = mop_up_consignment_val

    def _gf_final_audit(self, ctx) -> Dict:
        """Final audit: recompute realized spend from physical quantities (absolute sum parity) and build the result summary."""
        recommendations = ctx.recommendations
        total_budget = ctx.total_budget
        summary = ctx.summary
        wallets = ctx.wallets


        # v10.9: ABSOLUTE SUM PARITY - Final Audit Layer
        # Standardize: We do NOT trust the running accumulators. 
        # We calculate the final realization by summing the physical quantities.
        total_cash_invested = 0.0
        total_consignment_value = 0.0
        
        pass1_final = 0.0
        pass2_final = 0.0
        p2b_final = 0.0
        mop_final = 0.0
        
        for r in recommendations:
            qty = float(r.get('recommended_quantity', 0))
            if qty > 0:
                cost_p = r.get('cost_price')
                if cost_p is None:
                    # Emergency snapshot if a pass missed it
                    cost_p = self._get_actual_cost_price(r, float(r.get('selling_price', 0)))
                    r['cost_price'] = cost_p
                
                item_total = round(float(qty) * float(cost_p), 4)
                
                if r.get('is_consignment', False):
                    total_consignment_value = round(total_consignment_value + item_total, 4)
                else:
                    total_cash_invested = round(total_cash_invested + item_total, 4)
                    
                    # Attribution (for reporting)
                    reasoning = str(r.get('reasoning', ''))
                    if r.get('pass1_allocated'): pass1_final = round(pass1_final + item_total, 2)
                    elif "FLEX POOL" in reasoning or "BOOST" in reasoning: p2b_final = round(p2b_final + item_total, 2)
                    elif "MOP-UP" in reasoning or "RE-ENTRY" in reasoning: mop_final = round(mop_final + item_total, 2)
                    else: pass2_final = round(pass2_final + item_total, 2)

        # Final Summary Pack
        dept_util = {d: round(w['spent'] / w['allocated_budget'] * 100, 1) if w['allocated_budget'] > 0 else 0 for d, w in wallets.items() if d != 'GENERAL'}

        summary.update({
            "pass1_cash": round(pass1_final, 2),
            "pass2_cash": round(pass2_final, 2),
            "pass2b_cash": round(p2b_final, 2),
            "mop_up_cash": round(mop_final, 2),
            "total_cash_used": round(total_cash_invested, 2),
            "total_consignment_value": round(total_consignment_value, 2),
            "budget_target": total_budget,
            "utilization_pct": round((total_cash_invested / total_budget) * 100, 4) if total_budget > 0 else 0,
            "total_items": len([r for r in recommendations if r.get('recommended_quantity', 0) > 0]),
            "dept_utilization": dept_util
        })
        
        logger.info(f"Greenfield Allocation Audit Complete. Realized Spend: ${total_cash_invested:,.2f} / ${total_budget:,.2f}")
        return {"recommendations": recommendations, "summary": summary}
