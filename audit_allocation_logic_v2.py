"""
Audit Allocation Logic v2
==========================
Systematic review of order_engine.py logic for remaining gaps.
Focused on v3.0 features (Flex Pool) and known issues (Pass 1 dominance).
"""

gaps = []

# GAP K: Pass 1 Dominance
# ---------------------
# Observation: In simulations, Pass 1 (Width) consumes 100% of budget for small scenarios (200k-600k).
# Cause: High MDQ (3 units) * Large Assortment (23k SKUs).
# Implication: "Strategic Depth" (Pass 2) never runs. We essentially become a "Wide & Shallow" store.
# Risk: Stockouts on high velocity items because we bought 1 unit of everything else.
gaps.append({
    "code": "GAP K",
    "name": "Pass 1 Budget Dominance",
    "severity": "HIGH",
    "desc": "Pass 1 (Width) consumes >95% of budget in Small/Micro scenarios, preventing Strategic Depth (Pass 2).",
    "recommendation": "Implement 'Assortment Trimming' for Micro stores. Hard cap Pass 1 at 70% of budget. Drop C-Class/Non-Essentials if cap hit."
})

# GAP L: Wallet Leaking
# ---------------------
# Observation: Pass 2 respects wallet limits. Item might be rejected because 'Fresh' wallet is empty,
# even if 'General' wallet has $50k unused.
# Flex Pool (Pass 2B) fixes this *after* the fact, but only if >5% global unused.
# If Unused is 4%, Flex Pool doesn't trigger, and that 'Fresh' item remains starved.
# Risk: Suboptimal allocation due to strict wallet silos + high activation threshold.
gaps.append({
    "code": "GAP L",
    "name": "Wallet Silo Inefficiency",
    "severity": "MEDIUM",
    "desc": "Strict wallet silos in Pass 2 may starve high-priority depts while others sit on cash. Flex Pool threshold (5%) creates a dead zone.",
    "recommendation": "Implement 'Dynamic Wallet Borrowing' in Pass 2 for Staples. Allow dipping into General Fund if Department Wallet empty."
})

# GAP M: Consignment Depth Logic
# ------------------------------
# Observation: Consignment items bypass cost checks (free).
# But do they follow the same Depth limits?
# Logic: `if is_consignment: can_afford = True`.
# Logic: `ideal_qty = int(effective_avg_sales * effective_days)`.
# Implication: We order consignment items to the same depth (e.g., 10 days) as cash items.
# Opportunity: Since it's free capital (consignment), why not stock 30 days? Or Max Packs?
# Risk: Leaving free money on table by artificially constraining Consignment to Cash Depth profiles.
gaps.append({
    "code": "GAP M",
    "name": "Consignment Depth Constraint",
    "severity": "LOW",
    "desc": "Consignment items are constrained to the same 'Depth Days' as cash items.",
    "recommendation": "Boost Depth Cap for Consignment items (e.g., 2x Tier Depth or Max Packs). Consignment = Free Depth."
})

# GAP N: Zero-Sales New Product Risk
# ----------------------------------
# Observation: New products get 0.3-0.5 baseline.
# If we have 100 new products, that's 50 sales/day demand simulated.
# If they don't sell, we have dead stock.
# Pass 1 buys them (Width).
# Risk: Over-indexing on 'New' items in the scorecard that have no proven demand.
gaps.append({
    "code": "GAP N",
    "name": "New Product Accumulation",
    "severity": "MEDIUM",
    "desc": "Pass 1 blindly buys MDQ for 'New' items. Large volume of new items can bloat inventory.",
    "recommendation": "Limit 'New Product' budget share (e.g., max 5% of total budget)."
})

# Print Report
print("="*60)
print("ALLOCATION LOGIC GAP ANALYSIS (v3.0 Post-Flex Pool)")
print("="*60)
print(f"Total Gaps Identified: {len(gaps)}")
print()

for g in gaps:
    print(f"[{g['code']}] {g['name']} ({g['severity']})")
    print(f"   Issue: {g['desc']}")
    print(f"   Fix:   {g['recommendation']}")
    print("-" * 60)
