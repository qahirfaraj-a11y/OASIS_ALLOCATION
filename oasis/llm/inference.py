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
        logger.info("RULE-BASED ENGINE: Analyzing products with Phased Reasoning Trace...")
        await asyncio.sleep(0.5) 
        
        results = []
        for p in products:
            product_name = p.get('product_name', 'Unknown')
            trace = [] # Phased reasoning accumulator
            
            # --- PHASE 1: DIAGNOSTIC PRE-FLIGHT ---
            
            # 0. Blocked Check
            if p.get('blocked_open_for_order') == 'blocked':
                results.append({
                    "product_name": product_name,
                    "recommended_quantity": 0,
                    "reasoning": "PHASE 1: BLOCKED. Product is marked as blocked in procurement master.",
                    "confidence": "HIGH",
                    "historical_avg": p.get('historical_avg_order_qty', 0),
                    "est_cost": 0
                })
                continue

            # Gather Core Data
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
            abc = p.get('abc_rank', 'C')
            xyz = p.get('xyz_rank', 'Z')
            strategy = f"{abc}{xyz}"

            # 1. Discontinued Logic
            if p.get('is_discontinued'):
                results.append({
                    "product_name": product_name,
                    "recommended_quantity": 0,
                    "reasoning": f"PHASE 1: DISCONTINUED. {p.get('discontinued_reason', 'Item Discontinued')}.",
                    "confidence": "HIGH",
                    "historical_avg": historical_avg,
                    "est_cost": 0
                })
                continue

            # 2. Sunset Logic
            if p.get('is_sunset'):
                if current_stock > 0:
                    results.append({
                        "product_name": product_name,
                        "recommended_quantity": 0,
                        "reasoning": f"PHASE 1: SUNSET WIND-DOWN. Stock ({current_stock}) covers existing needs.",
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
                        "reasoning": f"PHASE 1: SUNSET. Minimal fill for {abc}-rank item (No stock).",
                        "confidence": "MEDIUM",
                        "historical_avg": historical_avg,
                        "est_cost": rec_qty * selling_price * 0.75
                    })
                    continue

            # 3. Anti-Overstock Guard (RED)
            avg_daily_sales_last_30d = p.get('avg_daily_sales_last_30d', 0.0)
            effective_daily_sales = max(0.01, avg_daily_sales)
            if avg_daily_sales_last_30d > 0:
                 effective_daily_sales = avg_daily_sales_last_30d

            coverage_days = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
            
            # Dynamic Upper Bound Calculation
            base_upper_bound = p.get('upper_coverage_days', 45)
            if strategy == 'CZ':
                supplier_freq_days = p.get('supplier_frequency_days', 7)
                if supplier_freq_days <= 7: base_upper_bound = 21
                elif supplier_freq_days <= 14: base_upper_bound = 28
                else: base_upper_bound = 35

            demand_cv = p.get('demand_cv', 0.5)
            lead_time_days = p.get('estimated_delivery_days', 1)
            reliability_score = p.get('reliability_score', 90)
            reliability_factor = 1.0 + (100 - reliability_score) / 100.0
            lead_time_factor = (lead_time_days / 7.0) * reliability_factor
            k = 0.5 if is_fresh else 1.0
            dynamic_adj = k * (demand_cv * effective_daily_sales) * lead_time_factor
            upper_bound = int(base_upper_bound + dynamic_adj)
            upper_bound = max(base_upper_bound, min(upper_bound, base_upper_bound * 2))
            
            # v10.10: Tighter Golden Guards (1.2x Hard / 1.1x Soft)
            soft_zone_threshold = upper_bound * 1.1
            hard_zone_threshold = upper_bound * 1.2
            
            strong_reason = p.get('is_promo', False) or p.get('moq_floor', 0) > 0
            
            if coverage_days > hard_zone_threshold and not strong_reason:
                results.append({
                    "product_name": product_name,
                    "recommended_quantity": 0,
                    "reasoning": f"PHASE 1: ANTI-OVERSTOCK GUARD (RED). {strategy} item covers {coverage_days:.1f} days (Dynamic Max {upper_bound}, Hard Limit {hard_zone_threshold:.1f}).",
                    "confidence": "HIGH",
                    "historical_avg": historical_avg,
                    "est_cost": 0
                })
                continue

            # --- PHASE 2: VELOCITY BASELINE ---
            
            is_blended = p.get('is_velocity_blended', False)
            target_days = float(p.get('target_coverage_days', 0.0))
            
            if is_blended:
                # We need the original historical ADS which was blended in IntelligenceMixin
                # Since we don't have it here easily, let's just show the blended result
                trace.append(f"PHASE 2: VELOCITY BASELINE (BLENDED). Target {target_days}d at {p.get('avg_daily_sales', 0):.2f} ADS")
            else:
                trace.append(f"PHASE 2: VELOCITY BASELINE ({'HISTORICAL' if historical_avg > 0 else 'CALCULATED'}). Target {target_days}d at {avg_daily_sales:.2f} ADS")
            
            import math
            target_stock = p.get('avg_daily_sales', 0) * target_days
            
            # v10.12: Avoid truncation for low-velocity items (ADS 0.32)
            # Using math.ceil for baseline requirement if stock is low
            if current_stock == 0 and target_stock > 0:
                rec_qty = max(1, math.ceil(target_stock))
                trace.append(f"PHASE 2: VELOCITY BASELINE (STOCKOUT RESCUE). Target {target_days}d at {p.get('avg_daily_sales', 0):.2f} ADS -> Min 1 Unit.")
            else:
                rec_qty = max(0, int(round(target_stock - current_stock)))
                trace.append(f"PHASE 2: VELOCITY BASELINE. Target {target_days}d -> {rec_qty} units needed.")
            
            # --- PHASE 3: STRATEGIC MODULATORS ---
            
            mod_trace = []
            # Trends
            if trend == 'growing' and trend_pct > 10:
                rec_qty = max(1, int(round(rec_qty * 1.15)))
                mod_trace.append("Trend Boost (1.15x)")
            elif trend == 'declining':
                # v10.12: Don't kill a stockout rescue order entirely
                new_qty = int(round(rec_qty * 0.9))
                if current_stock == 0 and new_qty < 1:
                    new_qty = 1
                    mod_trace.append("Trend Reduction (Capped at 1 for stockout)")
                else:
                    rec_qty = new_qty
                    mod_trace.append("Trend Reduction (0.9x)")
            
            # Margin
            if p.get('margin_pct', 0.0) > 30: 
                rec_qty = max(1, int(round(rec_qty * 1.20)))
                mod_trace.append("Margin Boost (1.2x)")
            
            # Quality
            if expiry_returns > 1000:
                new_qty = int(round(rec_qty * 0.9))
                if current_stock == 0 and new_qty < 1:
                    new_qty = 1
                    mod_trace.append("Quality Penalty (Capped at 1 for stockout)")
                else:
                    rec_qty = new_qty
                    mod_trace.append("Quality Penalty (0.9x)")
                
            if mod_trace:
                trace.append(f"PHASE 3: STRATEGIC MODULATORS. Applied: {', '.join(mod_trace)}")
            else:
                trace.append("PHASE 3: STRATEGIC MODULATORS. No active trend/margin/quality multipliers.")

            # --- PHASE 4: LIFECYCLE & FRESH AGING ---
            
            lifecycle_clamp = False
            if is_fresh:
                if 120 < days_since_delivery <= 180:
                    max_order = pack_size
                    if rec_qty > max_order:
                        rec_qty = max_order
                        lifecycle_clamp = True
                        trace.append(f"PHASE 4: FRESH AGING CLAMP. {days_since_delivery}d old, capped at 1 Pack ({max_order} units)")
                elif days_since_delivery > 180:
                     # This should have been caught in diagnostic, but just in case
                     rec_qty = 0
                     trace.append(f"PHASE 4: CRITICAL STALE FRESH. {days_since_delivery}d old. Blocked.")
            elif days_since_delivery >= 200:
                # Steady Slow Mover cap
                max_stock = 21 * effective_daily_sales
                max_order = max(0, int(max_stock - current_stock))
                if rec_qty > max_order:
                    rec_qty = max_order
                    lifecycle_clamp = True
                    trace.append(f"PHASE 4: SLOW MOVER CLAMP. {days_since_delivery}d old, capped at 21d cover ({max_order} units)")

            # --- PHASE 5: INVENTORY GUARDRAILS ---
            
            guard_trace = []
            
            # v10.15: Health Check Minimum for Strategic AX items
            # If a top-tier item is in the yellow zone (10-15 days), we force a small MOQ order
            # to verify supplier health and maintain pipeline, even if velocity doesn't strictly require it.
            is_strategic_ax = (abc == 'A' and xyz == 'X')
            days_coverage = current_stock / effective_daily_sales if effective_daily_sales > 0 else 999
            
            if is_strategic_ax and p.get('is_key_sku') and not p.get('is_sunset'):
                if 10 <= days_coverage <= 15 and rec_qty == 0 and days_since_delivery < 60:
                    rec_qty = max(1, p.get('moq_floor', 1))
                    guard_trace.append("HEALTH CHECK MINIMUM (Strategic AX in Yellow Zone)")

            # Soft Guard (Yellow Zone)
            if coverage_days > upper_bound and not strong_reason:
                target_stock_capped = upper_bound * effective_daily_sales
                max_order = max(0, int(target_stock_capped - current_stock))
                if rec_qty > max_order:
                    rec_qty = max_order
                    guard_trace.append(f"Soft Guard ({upper_bound}d cap)")
            
            # MOQ
            moq = p.get('moq_floor', 0)
            if 0 < rec_qty < moq and not lifecycle_clamp:
                rec_qty = moq
                guard_trace.append(f"MOQ Floor ({moq})")
            
            if guard_trace:
                trace.append(f"PHASE 5: INVENTORY GUARDRAILS. {', '.join(guard_trace)}")

            # --- PHASE 6: FINAL DEPLOYMENT (ROUNDING) ---
            
            risk_level = "high" if (current_stock <= 0 or coverage_days < 3) else ("low" if coverage_days > 20 else "medium")
            rounding_info = apply_pack_rounding(
                base_qty=rec_qty,
                pack_size=pack_size,
                is_key_sku=p.get('is_key_sku', False),
                stockout_risk=risk_level,
                max_overage_ratio=0.25,
                abc_rank=abc
            )
            
            final_qty = rounding_info['rounded_qty']
            if rounding_info['rounding_direction'] != 'none':
                trace.append(f"PHASE 6: FINAL ROUNDING. {rounding_info['rounding_direction'].upper()} ({rounding_info['rounding_reason']}) -> {final_qty}")
            else:
                trace.append(f"PHASE 6: FINAL ROUNDING. Precision maintained at {final_qty} units.")
            
            results.append({
                "product_name": product_name,
                "recommended_quantity": final_qty,
                "reasoning": ". ".join(trace),
                "confidence": "HIGH" if historical_avg > 0 else "MEDIUM",
                "historical_avg": historical_avg,
                "est_cost": final_qty * float(p.get('cost_price', selling_price * 0.75))
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
