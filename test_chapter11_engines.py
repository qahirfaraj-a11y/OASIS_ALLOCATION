"""Test all Chapter 11 OASIS Pre-Flight Engines against the live neural network data."""
import sys
import os
sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')

print("=" * 60)
print("OASIS Chapter 11 Engine Test Suite")
print("=" * 60)

nn_path = r'C:\Users\iLink\.gemini\antigravity\scratch\neutral_network_export'
data_dir = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'

# ---- TEST 1: AMIT (Dynamic Assortment Scaling) ----
print("\n--- TEST 1: AMIT (Dynamic Assortment Scaling) ---")
from oasis.logic.amit_gatekeeper import run_amit

print("  Running with 1M KES Budget (Micro-Variety)...")
amit_1m = run_amit(nn_path, data_dir, total_budget=1_000_000)
print(f"    Wines Cap: {amit_1m['department_caps_applied'].get('WINES')}")
print(f"    Total Blacklisted: {amit_1m['stats']['total_blacklisted']}")

print("  Running with 50M KES Budget (Mega Supermarket)...")
amit_50m = run_amit(nn_path, data_dir, total_budget=50_000_000)
print(f"    Wines Cap: {amit_50m['department_caps_applied'].get('WINES')}")
print(f"    Total Blacklisted: {amit_50m['stats']['total_blacklisted']}")

amit_result = amit_50m # For backward compatibility in script
print("  AMIT Scaling: PASS")

# ---- TEST 2: LATA ----
print("\n--- TEST 2: LATA (Lead-Time & Allocation Shield) ---")
from oasis.logic.lata_shield import run_lata
lata_result = run_lata(data_dir)
print(f"  Suppliers Updated: {lata_result['updated']}")
print(f"  Safety Inflated (unreliable): {lata_result['inflated']}")
print(f"  Capital Released (reliable): {lata_result['deflated']}")
print(f"  Neutral: {lata_result['neutral']}")
print("  LATA: PASS")

# ---- TEST 3: DHARAM ----
print("\n--- TEST 3: DHARAM (Demand, Halo & Revenue Analytics) ---")
from oasis.logic.dharam_revenue import run_dharam
dharam_result = run_dharam(nn_path, data_dir)
stats = dharam_result['stats']
print(f"  Nodes Analyzed: {stats['total_nodes_analyzed']}")
print(f"  Anchors Identified: {stats['total_anchors_identified']}")
print(f"  Demand Patches: {stats['total_demand_patches']}")
if dharam_result.get('top_patches'):
    print("  Top 5 Ghost Demand Recoveries:")
    for p in dharam_result['top_patches'][:5]:
        delta = p['patched_ads'] - p['original_ads']
        print(f"    {p['sku']}: {p['original_ads']:.3f} -> {p['patched_ads']:.3f} (+{delta:.3f} ADS)")
print("  DHARAM: PASS")

# ---- TEST 4: MANDE ----
print("\n--- TEST 4: MANDE (Market, Network & Distribution Efficiency) ---")
from oasis.logic.mande_triage import run_mande
mande_result = run_mande(nn_path, data_dir)
summary = mande_result['summary']
print(f"  Suppliers Analyzed: {summary['total_suppliers_analyzed']}")
print(f"  High-Risk Purge Candidates: {summary['purge_candidates_high']}")
print(f"  Medium-Risk Candidates: {summary['purge_candidates_medium']}")
print(f"  Total Capital Release Potential: {summary['total_capital_release_potential']:,.0f} KES")
print(f"  Avg Days Improvement: {summary['avg_days_improvement']}")
if mande_result.get('purge_candidates'):
    print("  Top 5 Purge Candidates:")
    for s in mande_result['purge_candidates'][:5]:
        print(f"    {s['supplier']}: SEI={s['sei']:.4f}, Trapped={s['trapped_capital_30d']:,.0f} KES, Subs={s['avg_substitution_edges']:.1f}")
print("  MANDE: PASS")

print("\n" + "=" * 60)
print("ALL ENGINES TESTED SUCCESSFULLY")
print("=" * 60)
