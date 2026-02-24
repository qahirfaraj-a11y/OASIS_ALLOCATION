"""
Final Comprehensive Gap Analysis - After GAPs A-D Fixes
======================================================

Systematic review of order_engine.py to identify remaining issues.
"""
import pandas as pd

print("=" * 70)
print("FINAL ALLOCATION LOGIC GAP ANALYSIS")
print("=" * 70)
print()

gaps_found = []

# ============================================================================
# GAP E: BUDGET GUARD 95% MAY LEAVE UNUSED CASH
# ============================================================================
print("GAP E: Budget Utilization Cap")
print("-" * 70)
print("Location: order_engine.py line 951")
print()
print("Issue:")
print("  Pass 1 stops at 95% budget to allow Pass 2 depth")
print("  But if Pass 2 doesn't use the remaining 5%, budget is wasted")
print()
print("Current Logic:")
print("  if pass1_cost > (total_budget * 0.95):")
print("      # Stop adding items")
print()
print("Problem:")
print("  - Small stores may leave 5-10% unused")
print("  - Pass 2B tries to redistribute but may not find items")
print()
print("Recommendation:")
print("  - Lower Pass 1 cap to 85% for small stores (more room for depth)")
print("  - OR: Add Pass 3 'Mop-Up' to spend any remaining <5%")
print()

gaps_found.append({
    'gap': 'GAP E: Budget Utilization',
    'severity': 'LOW',
    'line': 951,
    'impact': 'May leave 5-10% budget unused'
})

# ============================================================================
# GAP F: MAX_PACKS OVERRIDE FOR ANCHORS NOT CONSISTENT
# ============================================================================
print("=" * 70)
print("GAP F: Max Packs Override Inconsistency")
print("-" * 70)
print("Location: order_engine.py lines 1197-1199")
print()
print("Issue:")
print("  Pass 2 overrides max_packs for COOKING OIL/FLOUR/SUGAR")
print("  But Pass 1 doesn't have same override")
print()
print("Current Logic:")
print("  Pass 1 (line 972): Enforces max_packs limit for ALL items")
print("  Pass 2 (line 1197): Sets max_allowed_units = 999 for anchors")
print()
print("Problem:")
print("  - Anchors might be capped in Pass 1 at 3 packs")
print("  - Then Pass 2 tries to add more but item already has quantity")
print("  - Inconsistent behavior")
print()
print("Recommendation:")
print("  - Apply same anchor override in Pass 1 (line 972)")
print()

gaps_found.append({
    'gap': 'GAP F: Max Packs Override',
    'severity': 'MEDIUM',
    'line': 972,
    'impact': 'Staple anchors may be under-stocked'
})

# ============================================================================
# GAP G: NEW PRODUCT BASELINE MAY BE TOO CONSERVATIVE
# ============================================================================
print("=" * 70)
print("GAP G: New Product Baseline Logic")
print("-" * 70)
print("Location: order_engine.py lines 1090-1107")
print()
print("Issue:")
print("  New products with 0 sales get baseline: 0.3/day (fresh) or 0.5/day (dry)")
print("  This may be too low for staple departments")
print()
print("Current Logic:")
print("  if avg_daily_sales == 0:")
print("      if lookalike_demand > 0:")
print("          use lookalike")
print("      else:")
print("          use 0.3 or 0.5 baseline")
print()
print("Problem:")
print("  - A new MILK brand gets 0.3/day baseline")
print("  - But established MILK items sell 20+/day")
print("  - Baseline too conservative for essential categories")
print()
print("Recommendation:")
print("  - Use department median sales for new products in staple depts")
print("  - E.g., new MILK → use median MILK sales as baseline")
print()

gaps_found.append({
    'gap': 'GAP G: New Product Baseline',
    'severity': 'LOW',
    'line': 1104,
    'impact': 'New staple products may be under-allocated'
})

# ============================================================================
# GAP H: CONSIGNMENT NOT CHECKED IN PRIORITY SCORE
# ============================================================================
print("=" * 70)
print("GAP H: Consignment Items in Priority Score")
print("-" * 70)
print()
print("Issue:")
print("  Priority_Score doesn't consider consignment status")
print("  Consignment items should be Priority 1 (free capital)")
print()
print("Current Logic:")
print("  Priority = f(ABC_Class, Velocity_Tier)")
print("  Consignment checked separately in allocation")
print()
print("Problem:")
print("  - Consignment items might be Priority 4 due to low sales")
print("  - But they're FREE, so should be allocated first")
print()
print("Recommendation:")
print("  - Add consignment flag to priority calculation")
print("  - Consignment items → auto Priority 1 or 2")
print()

gaps_found.append({
    'gap': 'GAP H: Consignment Priority',
    'severity': 'MEDIUM',
    'impact': 'Free consignment items may be deprioritized'
})

# ============================================================================
# GAP I: PASS 2B REDISTRIBUTION ONLY TARGETS CAPPED ITEMS
# ============================================================================
print("=" * 70)
print("GAP I: Pass 2B Redistribution Scope")
print("-" * 70)
print("Location: order_engine.py lines 1350-1364")
print()
print("Issue:")
print("  Pass 2B only redistributes to items that were CAPPED")
print("  Ignores items that could use more depth but weren't capped")
print()
print("Current Logic:")
print("  was_capped = 'CAP' in reasoning or 'WALLET' in reasoning")
print("  if was_capped: realloc_candidates.append(rec)")
print()
print("Problem:")
print("  - Item might want 10 units but only got 7 (not capped)")
print("  - Won't be considered for redistribution")
print("  - Unused budget not fully optimized")
print()
print("Recommendation:")
print("  - Include all Priority 1/2 items in redistribution")
print("  - Not just capped ones")
print()

gaps_found.append({
    'gap': 'GAP I: Pass 2B Scope',
    'severity': 'LOW',
    'line': 1357,
    'impact': 'Suboptimal budget redistribution'
})

# ============================================================================
# GAP J: SHELF LIFE ENFORCEMENT ONLY FOR <30 DAYS
# ============================================================================
print("=" * 70)
print("GAP J: Shelf Life Enforcement Threshold")
print("-" * 70)
print("Location: order_engine.py lines 1178-1184")
print()
print("Issue:")
print("  Shelf life cap only applies if shelf_life < 30 days")
print("  But items with 40-60 day shelf life also need careful management")
print()
print("Current Logic:")
print("  if shelf_life < 30:")
print("      enforce cap")
print()
print("Problem:")
print("  - Yogurt (45 days) not capped")
print("  - Could order 60 days worth → spoilage")
print()
print("Recommendation:")
print("  - Use sliding scale: cap at 50% of shelf life")
print("  - E.g., 60-day shelf life → max 30 days stock")
print()

gaps_found.append({
    'gap': 'GAP J: Shelf Life Threshold',
    'severity': 'LOW',
    'line': 1178,
    'impact': 'Medium-shelf-life items may spoil'
})

# ============================================================================
# SUMMARY
# ============================================================================
print("=" * 70)
print("SUMMARY: REMAINING GAPS AFTER A-D FIXES")
print("=" * 70)
print()

print(f"Total gaps identified: {len(gaps_found)}")
print()

severity_counts = {}
for g in gaps_found:
    sev = g['severity']
    severity_counts[sev] = severity_counts.get(sev, 0) + 1

print("By severity:")
for sev in ['HIGH', 'MEDIUM', 'LOW']:
    count = severity_counts.get(sev, 0)
    if count > 0:
        print(f"  {sev}: {count}")
print()

print("Detailed list:")
for i, g in enumerate(gaps_found, 1):
    print(f"\n{i}. {g['gap']}")
    print(f"   Severity: {g['severity']}")
    print(f"   Impact: {g['impact']}")
    if 'line' in g:
        print(f"   Location: Line {g['line']}")

print()
print("=" * 70)
print("CRITICAL vs NON-CRITICAL")
print("=" * 70)
print("""
CRITICAL (Must Fix):
  - None (all critical gaps A-D already fixed)

MEDIUM Priority (Should Fix):
  - GAP F: Max packs override inconsistency
  - GAP H: Consignment items not prioritized

LOW Priority (Nice to Have):
  - GAP E: Budget utilization cap
  - GAP G: New product baseline
  - GAP I: Pass 2B scope
  - GAP J: Shelf life threshold

RECOMMENDATION:
  Focus on GAP F and GAP H in next session.
  These have tangible impact on allocation quality.
""")
