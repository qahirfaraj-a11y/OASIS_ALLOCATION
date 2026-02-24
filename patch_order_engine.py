
import os

target_file = r"C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\order_engine.py"

with open(target_file, "r", encoding="utf-8") as f:
    content = f.read()

# Define start and end markers
start_marker = "def fill_depth_constrained(candidate_list, global_spending_cap=999999999.0):"
end_marker = "logger.info(f\"Pass 2 Complete. Added Depth: ${pass2_cost:,.2f} (Fast5: ${added_fast_five_cost:,.0f}, OtherStaples: ${added_other_staple_cost:,.0f}, Discretionary: ${added_disc_cost:,.0f})\")"

start_idx = content.find(start_marker)
end_idx = content.find(end_marker)

if start_idx == -1:
    print("ERROR: Start marker not found!")
    # partial match check
    print(f"Contains 'fill_depth_constrained'? {'fill_depth_constrained' in content}")
    exit(1)

if end_idx == -1:
    print("ERROR: End marker not found!")
    exit(1)

# Include the end marker in the replacement zone (we will rewrite it)
end_idx += len(end_marker)

print(f"Replacing block from char {start_idx} to {end_idx}...")

# New Code Block (Correctly Indented)
new_code = """        # v5.4 FIX: Clean Internal Helper for Priority Allocation
        def allocate_list_constrained(candidate_list, phase_cap, phase_name):
            
            # 1. Build Calculation Queue
            queue = []
            for rec in candidate_list:
                dept = rec.get('product_category', 'GENERAL').upper()
                avg_sales = rec.get('avg_daily_sales', 0.0)
                
                # --- v2.5 NEW PRODUCT HYBRID LOGIC ---
                effective_avg_sales = avg_sales
                new_product_mode = False
                
                if avg_sales <= 0:
                    lookalike = rec.get('lookalike_demand', 0.0)
                    if lookalike > 0:
                        effective_avg_sales = lookalike * 0.5 
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']: rec['reasoning'] += " [NEW PRODUCT: Lookalike]"
                    else:
                        is_fresh = rec.get('is_fresh', False)
                        effective_avg_sales = 0.3 if is_fresh else 0.5
                        new_product_mode = True
                        if "[NEW PRODUCT" not in rec['reasoning']: rec['reasoning'] += " [NEW PRODUCT: Baseline]"

                # --- DEPTH CALCULATION ---
                effective_days = depth_cap_days
                
                # Risk Logic (simplified)
                if new_product_mode:
                    is_fresh = rec.get('is_fresh', False)
                    max_new_product_days = 7 if is_fresh else 14
                    effective_days = min(effective_days, max_new_product_days)

                # v5.3 FIX: Dynamic Depth for Fresh (Lead Time + Buffer)
                # User Feedback: "Don't stock 3 days if daily supplier".
                if dept in ['FRESH MILK', 'BREAD']:
                     lead_time = int(rec.get('estimated_delivery_days', 1))
                     # We want to cover Lead Time + Small Buffer (0.6 days)
                     target_days = lead_time + 0.6
                     target_days = min(target_days, 3.0) 
                     effective_days = min(effective_days, target_days)
                     
                     # Ensure we honor high velocity unlock for essential flow
                     if effective_avg_sales > 5.0:
                         effective_days = max(effective_days, target_days) 
                
                # Calculate Ideal
                ideal_qty = int(effective_avg_sales * effective_days)
                
                # Min Packs
                min_pack_floor = 1
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                     unit_price = float(rec.get('selling_price', 0))
                     min_pack_floor = 12 if unit_price < 50 else 6
                
                ideal_qty = max(ideal_qty, min_pack_floor)
                
                # Max Packs (Constraint)
                current_qty = rec['recommended_quantity']
                pack_size = int(rec.get('pack_size', 1))
                max_total_packs = int(profile.get('max_packs', 10))
                max_allowed_units = max_total_packs * pack_size
                
                # High Velocity Unlock
                if is_small and dept in ['COOKING OIL', 'FLOUR', 'SUGAR']:
                    max_allowed_units = 999
                elif total_budget >= 20000000: 
                    max_allowed_units = 99999999
                elif effective_avg_sales > 1.0:
                     velocity_floor = int(effective_avg_sales * 7)
                     max_allowed_units = max(max_allowed_units, velocity_floor)
                
                final_target = min(ideal_qty, max_allowed_units)
                
                if current_qty < final_target:
                    price = float(rec.get('selling_price', 0.0))
                    cost_price_est = self._get_actual_cost_price(rec, price)
                    
                    queue.append({
                        'rec': rec,
                        'dept': dept,
                        'pack_size': pack_size,
                        'cost_per_pack': pack_size * cost_price_est,
                        'target_qty': final_target,
                        'cost_est': cost_price_est
                    })

            # 2. Execute Round Robin
            phase_cost = 0.0
            active = True
            
            while active and queue:
                active = False
                for i in range(len(queue) - 1, -1, -1):
                    item = queue[i]
                    rec = item['rec']
                    dept = item['dept']
                    pack_cost = item['cost_per_pack']
                    pack_size = item['pack_size']
                    
                    # Check Phase Cap
                    if (phase_cost + pack_cost) > phase_cap:
                        rec['reasoning'] += f" [{phase_name} CAP]"
                        queue.pop(i)
                        continue
                        
                    # Check Share Cap (except for Priority)
                    is_priority = (phase_name == "PRIORITY")
                    if not is_priority:
                         wallet_limit_ratio = 0.25 if is_small else 0.50
                         max_item_spend = wallets.get(dept, {}).get('allocated_budget', 0) * wallet_limit_ratio if dept in wallets else 99999999.0
                         current_spend = rec['recommended_quantity'] * item['cost_est']
                         if (current_spend + pack_cost) > max_item_spend:
                             if rec.get('pass1_allocated'): rec['reasoning'] += " [SHARE CAP]"
                             queue.pop(i)
                             continue

                    # Check Wallet
                    can_spend = True
                    if not is_priority and dept in wallets:
                         if not self.budget_manager.check_funds(wallets, dept, pack_cost):
                             can_spend = False
                    
                    if can_spend:
                        rec['recommended_quantity'] += pack_size
                        if not is_priority and dept in wallets:
                            self.budget_manager.spend_from_wallet(wallets, dept, pack_cost)
                        
                        rec['pass2_allocated'] = True
                        phase_cost += pack_cost
                        active = True
                        
                        if rec['recommended_quantity'] >= item['target_qty']:
                            queue.pop(i)
            
            return phase_cost

        # --- EXECUTION SEQUENCE ---
        
        # 1. Fast Five (Priority)
        added_fast_five_cost = allocate_list_constrained(fast_five_candidates, total_remaining_budget, "PRIORITY")
        
        # 2. Other Staples
        remaining_after_ff = total_remaining_budget - added_fast_five_cost
        staple_allocation_target = remaining_after_ff * pass2_staple_share
        
        logger.info(f"Pass 2 Remaining: ${remaining_after_ff:,.2f} (Other Staples Target: ${staple_allocation_target:,.2f})")
        
        added_other_staple_cost = allocate_list_constrained(other_staple_candidates, staple_allocation_target, "STAPLE")
        
        # 3. Discretionary
        remaining_disc = remaining_after_ff * (1.0 - pass2_staple_share)
        added_disc_cost = allocate_list_constrained(discretionary_candidates, remaining_disc, "DISC")
        
        pass2_cost = added_fast_five_cost + added_other_staple_cost + added_disc_cost

        logger.info(f"Pass 2 Complete. Added Depth: ${pass2_cost:,.2f} (Desc: {added_fast_five_cost:,.0f}/{added_other_staple_cost:,.0f}/{added_disc_cost:,.0f})")"""

# Perform replacement
new_content = content[:start_idx] + new_code + content[end_idx:]

with open(target_file, "w", encoding="utf-8") as f:
    f.write(new_content)

print("Successfully patched order_engine.py")
