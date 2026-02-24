"""
Deep Dive: Why does new ADS still not reach 114M?
====================================================
Possible causes:
1. Unit prices in scorecard are at SELLING price, but stock is valued at COST
2. Missing POS months (Feb, Apr, Aug, Nov) could affect ADS calculation
3. The 114M might include stock at COST PRICE (GRN prices), not selling price

This script checks COST vs SELLING price valuation.
"""
import pandas as pd
import numpy as np
import os
import json

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

# Load scorecard
sc = pd.read_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv")

# Load corrected ADS
with open(os.path.join(DATA_DIR, "corrected_ads_from_pos.json")) as f:
    corrected_ads = json.load(f)

# Load GRN data to check actual COST prices
grn_files = [f for f in os.listdir(DATA_DIR) if f.startswith("grn")]
print(f"GRN files found: {len(grn_files)}")
for gf in grn_files[:5]:
    print(f"  {gf}")

print("\n" + "=" * 80)
print("  PRICE VALUATION ANALYSIS")
print("=" * 80)

# The scorecard uses Unit_Price (selling price)
# Stock snapshot of 114M is likely at COST (what they paid suppliers)
# Let's check the margin structure

price = pd.to_numeric(sc['Unit_Price'], errors='coerce').fillna(0)
margin = pd.to_numeric(sc['Margin_Pct'], errors='coerce').fillna(0)
cost = price * (1 - margin / 100)

print(f"\n--- Price vs Cost ---")
print(f"  Mean selling price:  KES {price.mean():.2f}")
print(f"  Mean cost price:     KES {cost.mean():.2f}")
print(f"  Mean margin:         {margin.mean():.1f}%")
print(f"  Median margin:       {margin.median():.1f}%")

# Items with negative margin (loss leaders)
neg_margin = sc[margin < 0]
print(f"\n  Items with negative margin: {len(neg_margin)} ({len(neg_margin)/len(sc)*100:.1f}%)")

# KEY QUESTION: Is 114M at SELLING price or COST price?
# If 114M is at SELLING price, we should use selling price for comparison
# If 114M is at COST price, we should use cost price

# Check what happens if we value at SELLING price
new_ads_values = []
for _, row in sc.iterrows():
    product = str(row["Product"]).strip().upper()
    p = float(row.get("Unit_Price", 0)) if pd.notna(row.get("Unit_Price")) else 0
    m = float(row.get("Margin_Pct", 0)) if pd.notna(row.get("Margin_Pct")) else 0
    c = p * (1 - m / 100)
    
    old_ads = float(row.get("Avg_Daily_Sales", 0)) if pd.notna(row.get("Avg_Daily_Sales")) else 0
    
    if product in corrected_ads:
        ads = corrected_ads[product]["new_ads"]
    else:
        ads = old_ads
    
    new_ads_values.append({
        "product": product,
        "ads": ads,
        "selling_price": p,
        "cost_price": c,
        "margin_pct": m,
        "daily_sell_value": ads * p,
        "daily_cost_value": ads * c,
    })

df = pd.DataFrame(new_ads_values)

total_daily_sell = df["daily_sell_value"].sum()
total_daily_cost = df["daily_cost_value"].sum()

print(f"\n--- Daily Velocity (with corrected POS ADS) ---")
print(f"  At SELLING price:  KES {total_daily_sell:>12,.0f}/day")
print(f"  At COST price:     KES {total_daily_cost:>12,.0f}/day")

print(f"\n--- Stock Projections at SELLING price ---")
for depth in [14, 21, 28, 30, 45, 57]:
    stock = total_daily_sell * depth
    marker = " <-- 114M" if abs(stock - 114_000_000) / 114_000_000 < 0.15 else ""
    print(f"  {depth:>3}d:  KES {stock:>14,.0f}{marker}")

print(f"\n--- Stock Projections at COST price ---")
for depth in [14, 21, 28, 30, 45, 57]:
    stock = total_daily_cost * depth
    marker = " <-- 114M" if abs(stock - 114_000_000) / 114_000_000 < 0.15 else ""
    print(f"  {depth:>3}d:  KES {stock:>14,.0f}{marker}")

# Implied depth at each valuation
if total_daily_sell > 0:
    print(f"\n  Implied depth (114M at SELLING price): {114_000_000 / total_daily_sell:.1f} days")
if total_daily_cost > 0:
    print(f"  Implied depth (114M at COST price):    {114_000_000 / total_daily_cost:.1f} days")

# =========================================================================
# Look at GRN data for actual purchase prices
# =========================================================================
print(f"\n{'='*80}")
print(f"  GRN PURCHASE PRICE CHECK")
print(f"{'='*80}")

# Try loading a GRN file to see actual purchase costs
try:
    grn_sample = pd.read_excel(os.path.join(DATA_DIR, grn_files[0]))
    print(f"\n  Sample GRN file: {grn_files[0]}")
    print(f"  Columns: {list(grn_sample.columns)}")
    print(f"  Shape: {grn_sample.shape}")
    print(f"\n  First 3 rows:")
    print(grn_sample.head(3).to_string())
except Exception as e:
    print(f"  Error reading GRN: {e}")

# =========================================================================
# Check: maybe many products have zero/wrong prices
# =========================================================================
print(f"\n{'='*80}")
print(f"  PRICE QUALITY CHECK")
print(f"{'='*80}")

zero_price = df[df["selling_price"] == 0]
print(f"  Products with zero selling price: {len(zero_price)} ({len(zero_price)/len(df)*100:.1f}%)")

low_price = df[(df["selling_price"] > 0) & (df["selling_price"] < 10)]
print(f"  Products with price < KES 10:    {len(low_price)} ({len(low_price)/len(df)*100:.1f}%)")

# Check: what is the total qty sold per day (not value, just units)?
total_daily_qty = df["ads"].sum()
print(f"\n  Total daily quantity sold: {total_daily_qty:,.0f} units/day")
print(f"  Avg price per unit:       KES {total_daily_sell / total_daily_qty:.2f}")
print(f"  Avg cost per unit:        KES {total_daily_cost / total_daily_qty:.2f}")

# Check: what percentage of daily value is from top departments?
df_dept = sc.copy()
df_dept["corrected_ads"] = df_dept["Product"].apply(
    lambda x: corrected_ads.get(str(x).strip().upper(), {}).get("new_ads", 0)
)
df_dept["daily_sell"] = df_dept["corrected_ads"] * price

dept_sell = df_dept.groupby("Department")["daily_sell"].sum().sort_values(ascending=False)
print(f"\n  Top 5 departments by daily selling value:")
for dept, val in dept_sell.head(5).items():
    print(f"  {dept:<30} KES {val:>12,.0f}/day")

# =========================================================================
# HYPOTHESIS: Stock value includes FULL RANGE at safety/display quantities
# =========================================================================
print(f"\n{'='*80}")
print(f"  HYPOTHESIS: DISPLAY/SAFETY STOCK CONTRIBUTES TO 114M")
print(f"{'='*80}")

# Many slow-moving items still have physical stock on shelf (display qty)
# Even if ADS = 0.01, you hold at least 1-2 units on shelf
# Let's calculate: what if every product had a minimum of 2 units on shelf?

# Minimum display qty: 2 units
min_display = 2
display_cost = (df["cost_price"] * min_display).sum()
display_sell = (df["selling_price"] * min_display).sum()

# ADS-driven stock at various depths
for depth in [14, 21]:
    ads_driven_cost = (df["ads"] * depth * df["cost_price"]).sum()
    ads_driven_sell = (df["ads"] * depth * df["selling_price"]).sum()
    
    total_with_display_cost = ads_driven_cost + display_cost
    total_with_display_sell = ads_driven_sell + display_sell
    
    print(f"\n  {depth}-day depth + minimum display stock (2 units):")
    print(f"    ADS-driven (sell):  KES {ads_driven_sell:>12,.0f}")
    print(f"    Display minimum:    KES {display_sell:>12,.0f}")
    print(f"    TOTAL (sell):       KES {total_with_display_sell:>12,.0f}")
    print(f"    ADS-driven (cost):  KES {ads_driven_cost:>12,.0f}")
    print(f"    Display minimum:    KES {display_cost:>12,.0f}")
    print(f"    TOTAL (cost):       KES {total_with_display_cost:>12,.0f}")

# What if minimum display is based on pack size?
# Fresh items: 3-5 units, ambient: 6-12 units, wines/spirits: 2-3 units
print(f"\n  Realistic minimum display quantities:")
for min_qty_label, min_qty in [("1 unit", 1), ("3 units", 3), ("6 units", 6), ("12 units (1 case)", 12)]:
    total = (df["selling_price"] * min_qty).sum()
    print(f"    {min_qty_label:<20}: KES {total:>12,.0f}")

print("=" * 80)
