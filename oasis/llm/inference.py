import json
import logging
import asyncio
from ..logic.rounding import apply_pack_rounding

logger = logging.getLogger("LLMInference")

class BaseLLM:
    async def analyze(self, products: list) -> list:
        raise NotImplementedError

class RuleBasedLLM(BaseLLM):
    """
    Deterministic rule-based engine that uses enriched database data 
    (sales history, trends, delivery estimates) to calculate order quantities.
    """
    async def analyze(self, products: list) -> list:
        logger.info("RULE-BASED ENGINE: Analyzing products...")
        await asyncio.sleep(0.5) # Slight delay for UX feel
        
        results = []
        for p in products:
            product_name = p.get('product_name', 'Unknown')
            
            # 0. Blocked Check
            if p.get('blocked_open_for_order') == 'blocked':
                results.append({
                    "product_name": product_name,
                    "recommended_quantity": 0,
                    "reasoning": "BLOCKED: Product is marked as blocked.",
                    "confidence": "HIGH",
                    "historical_avg": p.get('historical_avg_order_qty', 0),
                    "est_cost": 0
                })
                continue

            # Gather Data
            current_stock = int(p.get('current_stocks', 0))
            avg_daily_sales = p.get('avg_daily_sales', 0)
            selling_price = p.get('selling_price', 0.0)
            is_fresh = p.get('is_fresh', False)
            days_since_delivery = int(p.get('last_days_since_last_delivery', 0))
            historical_avg = p.get('historical_avg_order_qty', 0)
            trend = p.get('sales_trend', 'stable')
            trend_pct = p.get('sales_trend_pct', 0.0)
            expiry_returns = p.get('supplier_expiry_returns', 0)
            pack_size = p.get('pack_size', 1)

            # Sales Activity Metrics
            days_since_last_sale = p.get('days_since_last_sale', 999)
            total_units_sold_last_90d = p.get('total_units_sold_last_90d', 0)
            avg_daily_sales_last_30d = p.get('avg_daily_sales_last_30d', 0.0)
            
            # Use blending for effective sales to avoid division by zero
            effective_daily_sales = max(0.01, avg_daily_sales)
            if avg_daily_sales_last_30d > 0:
                 effective_daily_sales = avg_daily_sales_last_30d

            # Strategic Classification
            abc = p.get('abc_rank', 'C')
            xyz = p.get('xyz_rank', 'Z')
            strategy = f"{abc}{xyz}"
            is_sunset = p.get('is_sunset', False)

            # --- SUNSET LOGIC (High Priority) ---
            if is_sunset:
                if current_stock > 0:
                    results.append({
                        "product_name": product_name,
                        "recommended_quantity": 0,
                        "reasoning": f"SUNSET WIND-DOWN: Item flagged for end-of-life. Stock ({current_stock}) covers existing needs.",
                        "confidence": "HIGH",
                        "historical_avg": historical_avg,
                        "est_cost": 0
                    })
                    continue
                else:
                    rec_qty = 3 if abc in ['A', 'B'] else 0
                    results.append({
                        "product_name": product_name,
                        "recommended_quantity": rec_qty,
                        "reasoning": f"SUNSET: Minimal fill for {abc}-rank item (No stock).",
                        "confidence": "MEDIUM",
                        "historical_avg": historical_avg,
                        "est_cost": rec_qty * selling_price * 0.75
                    })
                    continue

            # Anti-Overstock Guard (Soft Guard with Formula-Based Dynamic Bounds)
            # Formula: upper_bound = base_coverage_days + k * demand_std * lead_time_factor
            
            # Base upper bound from enrichment
            base_upper_bound = p.get('upper_coverage_days', 45)
            
            # CZ items: Use supplier frequency instead of hard 14-day limit
            if strategy == 'CZ':
                supplier_freq_days = p.get('supplier_frequency_days', 7)
                
                # Frequency-based base for CZ items
                if supplier_freq_days <= 7:  # Weekly or more frequent
                    base_upper_bound = 21  # 3 weeks
                elif supplier_freq_days <= 14:  # Bi-weekly
                    base_upper_bound = 28  # 4 weeks
                else:  # Monthly or less frequent
                    base_upper_bound = 35  # 5 weeks

            # Dynamic adjustment components
            demand_cv = p.get('demand_cv', 0.5)
            lead_time_days = p.get('estimated_delivery_days', 1)
            reliability_score = p.get('reliability_score', 90)
            
            # Intelligent Zero-Sales Handling (Blended Floor calculated earlier as effective_daily_sales)
            months_active = p.get('months_active', 0)
            
            # Calculate demand standard deviation from CV
            demand_std = demand_cv * effective_daily_sales if effective_daily_sales > 0 else 0
            
            # Lead time factor
            reliability_factor = 1.0 + (100 - reliability_score) / 100.0
            lead_time_factor = (lead_time_days / 7.0) * reliability_factor
            
            # Tuning coefficient k
            k = 1.0
            if is_fresh: k = 0.5
            
            # Calculate dynamic adjustment
            dynamic_adjustment = k * demand_std * lead_time_factor
            
            # Final upper bound
            upper_bound = int(base_upper_bound + dynamic_adjustment)
            upper_bound = max(base_upper_bound, min(upper_bound, base_upper_bound * 2))
            
            # Margin-Based Modulation
            margin_pct = p.get('margin_pct', 0.0)
            if margin_pct > 30: margin_multiplier = 1.20
            elif margin_pct > 15: margin_multiplier = 1.10
            else: margin_multiplier = 1.0
            
            upper_bound = int(upper_bound * margin_multiplier)
            
            # Calculate coverage
            coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
            
            # Define zones
            soft_zone_threshold = upper_bound * 1.2
            hard_zone_threshold = upper_bound * 1.2
            
            # Check for exceptions
            strong_reason = p.get('is_promo', False) or p.get('moq_floor', 0) > 0
            
            # Red Zone: Hard Block
            if coverage_days > hard_zone_threshold and not strong_reason:
                results.append({
                    "product_name": product_name,
                    "recommended_quantity": 0,
                    "reasoning": f"ANTI-OVERSTOCK GUARD (RED): {strategy} item covers {coverage_days:.1f} days (Dynamic Max {upper_bound}, Hard Limit {hard_zone_threshold:.1f}).",
                    "confidence": "HIGH",
                    "historical_avg": historical_avg,
                    "est_cost": 0
                })
                continue
            
            # Yellow Zone: Soft Guard
            elif coverage_days > upper_bound and not strong_reason:
                is_strategic = (
                    p.get('blocked_open_for_order') == 'open' and
                    not p.get('is_sunset', False) and
                    (p.get('is_key_sku', False) or p.get('is_top_sku', False) or strategy in ['AX', 'AY', 'BX']) and
                    days_since_delivery <= 60
                )
                
                if is_strategic:
                    moq = p.get('moq_floor', 1)
                    p['health_check_min_order'] = max(1, moq)
                    p['soft_guard_active'] = True
                    p['soft_guard_target'] = upper_bound
                else:
                    p['soft_guard_active'] = True
                    p['soft_guard_target'] = upper_bound
            
            # Green Zone: Normal operation

            # --- HARMONIZED SLOW MOVER & FRESH LOGIC ---
            
            # Default state
            post_order_coverage_cap = None
            slow_mover_reason = ""
            is_slow_mover = False

            # 1. TIERED FRESH LOGIC
            if is_fresh:
                if days_since_delivery > 120:
                    # Check recent sales to distinguish "Dead Fresh" vs "Long-Life Chilled"
                    if total_units_sold_last_90d == 0:
                         # Ultra-Perishable / Dead Fresh
                        results.append({
                            "product_name": product_name,
                            "recommended_quantity": 0,
                            "reasoning": f"STALE FRESH (DEAD): {days_since_delivery}d old, 0 sales in 90d. Blocked.",
                            "confidence": "HIGH",
                            "historical_avg": historical_avg,
                            "est_cost": 0
                        })
                        continue
                    elif days_since_delivery > 180:
                        # Even if selling, 180d is too old for fresh
                        results.append({
                            "product_name": product_name,
                            "recommended_quantity": 0,
                            "reasoning": f"STALE FRESH (CRITICAL): {days_since_delivery}d old. Exceeds 180d limit.",
                            "confidence": "HIGH",
                            "historical_avg": historical_avg,
                            "est_cost": 0
                        })
                        continue
                    else:
                        # 120-180d with sales: "Long-Life Chilled" logic
                        # Cap at very low coverage (e.g. 7 days)
                        post_order_coverage_cap = 7
                        is_slow_mover = True
                        slow_mover_reason = f"STALE FRESH (WATCHLIST): {days_since_delivery}d old but selling. Capped at {post_order_coverage_cap}d coverage."

            # 2. DRY SLOW MOVER LOGIC
            else: # Not fresh
                # 2A. Buffer Zone (160 - 200 days)
                if 160 <= days_since_delivery < 200:
                    # Watchlist: Not a hard cap, but apply aggression reduction
                    p['buffer_zone_active'] = True
                    # We will apply a multiplier at the end, not a hard cap yet
                    slow_mover_reason = f"SLOW MOVER WATCHLIST: {days_since_delivery}d (Approaching 200d limit)."
                
                # 2B. True Slow Mover (> 200 days)
                elif days_since_delivery >= 200:
                    is_slow_mover = True
                    
                    # Distinguish Dead Stock vs Steady Mover
                    if total_units_sold_last_90d == 0:
                        # Dead Stock
                        if current_stock > 0:
                            results.append({
                                "product_name": product_name,
                                "recommended_quantity": 0,
                                "reasoning": f"DEAD STOCK: {days_since_delivery}d old, 0 sales in 90d. Blocked.",
                                "confidence": "HIGH",
                                "historical_avg": historical_avg,
                                "est_cost": 0
                            })
                            continue
                        else:
                            # If stock is 0 and sales are 0, checking if we should re-activate is risky.
                            # Only if it's a known strategy item
                            if abc == 'A':
                                slow_mover_cap = pack_size # Min fill
                                post_order_coverage_cap = 14 
                                slow_mover_reason = f"SLOW MOVER (A-Brand): {days_since_delivery}d. Min fill {pack_size} units."
                            else:
                                results.append({
                                    "product_name": product_name,
                                    "recommended_quantity": 0,
                                    "reasoning": f"DEAD STOCK: {days_since_delivery}d old. No sales.",
                                    "confidence": "HIGH",
                                    "historical_avg": historical_avg,
                                    "est_cost": 0
                                })
                                continue
                    else:
                        # Steady Slow Mover (Has Sales)
                        # Harmonized Rule: Cap at 21 days coverage (Post-Order)
                        post_order_coverage_cap = 21
                        slow_mover_reason = f"SLOW MOVER (STEADY): {days_since_delivery}d. Capped at {post_order_coverage_cap}d coverage."

            # --- STANDARD CALCULATION PHASE ---
            
            # Determine Base Quantity
            # If we have a historical baseline, start there
            if historical_avg > 0:
                rec_qty = historical_avg
                base_reason = f"Historical Baseline ({historical_avg})"
                
                # Apply Trends
                if trend == 'growing' and trend_pct > 10:
                    rec_qty = int(rec_qty * 1.15)
                    base_reason += " + Trend Boost"
                elif trend == 'declining':
                    rec_qty = int(rec_qty * 0.9)
                    base_reason += " - Trend Reduction"

            # Otherwise calculate from daily sales
            elif avg_daily_sales > 0:
                d_days = int(p.get('estimated_delivery_days', 1))
                f_days = int(p.get('supplier_frequency_days', 7))
                buffer = 3 if d_days >= 4 else 1
                
                target_coverage = d_days + f_days + buffer
                target_stock = avg_daily_sales * target_coverage * 1.5 # Safety factor
                
                rec_qty = max(0, int(target_stock - current_stock))
                base_reason = f"Calculated (Daily {avg_daily_sales:.2f})"
            else:
                 rec_qty = 0 # No history, no sales
                 base_reason = "No data"

            # --- APPLY HARMONIZED LIMITS ---

            final_qty = rec_qty
            final_reason = base_reason

            # 1. Apply Buffer Zone Reduction (if active)
            if p.get('buffer_zone_active'):
                final_qty = int(final_qty * 0.8) # 20% reduction
                final_reason += f". {slow_mover_reason} (-20%)"

            # 2. Apply Post-Order Coverage Cap (if active)
            if is_slow_mover and post_order_coverage_cap is not None:
                # Logic: (Current + Order) / Daily_Sales <= Cap_Days
                # Order <= (Cap_Days * Daily_Sales) - Current
                
                max_stock_allowed = post_order_coverage_cap * effective_daily_sales
                max_order = max(0, int(max_stock_allowed - current_stock))
                
                # Enforce Pack Size (don't order 0.5 units, order 0 or pack_size)
                # But if max_order is tiny relative to pack_size, round DOWN to 0
                if max_order < (pack_size * 0.5):
                    max_order = 0
                else: 
                     # Round to nearest pack size logic could go here, but simple int is safer for now
                     pass

                if final_qty > max_order:
                    final_qty = max_order
                    final_reason = f"{slow_mover_reason} (Limit {max_order})"
                else:
                    final_reason += f". {slow_mover_reason}"

            # 2.5 Apply Soft Guard Cap (General Overstock)
            if p.get('soft_guard_active', False):
                target_coverage = p.get('soft_guard_target', 45)
                
                # Calculate post-order coverage
                max_stock_allowed = target_coverage * effective_daily_sales
                max_order = max(0, int(max_stock_allowed - current_stock))
                
                # Health Check for strategic items
                health_check = p.get('health_check_min_order', 0)
                
                if final_qty > max_order:
                     # Respect health check if strategic
                     final_qty = max(max_order, health_check)
                     if final_qty == health_check and health_check > 0:
                         final_reason += f" [SOFT GUARD: Capped to {max_order}, but held at Health Min {health_check}]"
                     else:
                         final_reason += f" [SOFT GUARD: Capped to {max_order} to maintain {target_coverage}d coverage]"

            # 3. Min/Max Constraints
            # Only apply if we are actually ordering something
            if final_qty > 0:
                # MOQ Floor
                moq = p.get('moq_floor', 0)
                if final_qty < moq:
                    # Only respect MOQ if we aren't strict slow mover capped
                    if not is_slow_mover:
                        final_qty = moq
                        final_reason += f" (MOQ {moq})"
            
            # 4. Quality Penalty
            if expiry_returns > 1000:
                final_qty = int(final_qty * 0.9)
                final_reason += " (Quality Penalty)"

            # Cost Calculation (Pre-Rounding Est)
            est_cost = final_qty * selling_price * 0.75

            # --- PACK ROUNDING (Final Step) ---
            # Derive stockout risk
            coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
            risk_level = "medium"
            if current_stock <= 0 or coverage_days < 3:
                risk_level = "high"
            elif coverage_days > 20:
                risk_level = "low"
            
            rounding_info = apply_pack_rounding(
                base_qty=final_qty,
                pack_size=pack_size,
                is_key_sku=p.get('is_key_sku', False),
                stockout_risk=risk_level,
                max_overage_ratio=0.25
            )
            
            final_qty = rounding_info['rounded_qty']
            if rounding_info['rounding_direction'] != 'none':
                final_reason += f" [Pack Rounding: {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']})]"

            results.append({
                "product_name": product_name,
                "recommended_quantity": final_qty,
                "reasoning": final_reason,
                "confidence": "HIGH" if historical_avg > 0 else "MEDIUM",
                "historical_avg": historical_avg,
                "est_cost": est_cost
            })
        
        return results

class LocalLLM(BaseLLM):
    def __init__(self, model_path: str):
        self.model_path = model_path
        self.llm = None
        
    def load_model(self):
        try:
            from llama_cpp import Llama
            self.llm = Llama(
                model_path=self.model_path,
                n_ctx=4096,  # Context window
                n_gpu_layers=-1, # All to GPU if available
                verbose=False
            )
            logger.info(f"Loaded model from {self.model_path}")
        except ImportError:
            logger.warning("Optional dependency 'llama-cpp-python' not found. Local AI will be unavailable, falling back to Rule Engine.")
        except Exception as e:
            logger.error(f"Failed to load model: {e}")

    async def analyze(self, products: list) -> list:
        if not self.llm:
            logger.warning("Model not loaded, falling back to mock.")
            # Fallback to simple logic if model fails
            return await RuleBasedLLM().analyze(products)

        # Prompt construction (simplified for smaller local models)
        prompt = f"""
You are an inventory assistant. Analyze these products and recommend order quantities.
Return ONLY valid JSON array.

Products:
{json.dumps(products, indent=2)}

Format:
[
  {{ "product_name": "...", "recommended_quantity": 0, "reasoning": "..." }}
]
"""
        # Run inference in thread pool to avoid blocking asyncio
        output = await asyncio.to_thread(
            self.llm.create_completion,
            prompt,
            max_tokens=2000,
            stop=["```"],
            temperature=0.1
        )
        
        text = output['choices'][0]['text']
        try:
            # clean partial json
            text = text.strip()
            if text.startswith("```json"): text = text[7:]
            if text.endswith("```"): text = text[:-3]
            return json.loads(text)
        except Exception as e:
            logger.error(f"LLM JSON Parse Error: {e}")
            return []
