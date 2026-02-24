"""
Root Cause Analysis: ADS Undervaluation
=========================================
Traces the ADS pipeline from POS -> forecast -> scorecard
to find where the undervaluation occurs.
"""
import json
import pandas as pd
import numpy as np
import os

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

# 1. Load the forecast data (source of scorecard ADS)
with open(os.path.join(DATA_DIR, "sales_forecasting_2025 (1).json")) as f:
    forecast_data = json.load(f)

# 2. Load profitability data (has total_qty_sold, revenue)
with open(os.path.join(DATA_DIR, "sales_profitability_intelligence_2025_updated.json")) as f:
    profit_data = json.load(f)

# 3. Load scorecard for comparison
scorecard = pd.read_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv")

print("=" * 80)
print("  ADS ROOT CAUSE ANALYSIS")
print("=" * 80)

# --- Examine forecast data structure ---
print(f"\n--- Forecast Data ---")
print(f"  Total SKUs: {len(forecast_data)}")
sample_items = list(forecast_data.items())[:3]
for name, data in sample_items:
    print(f"  '{name}':")
    for k, v in data.items():
        print(f"    {k}: {v}")

# --- Examine profitability data structure ---
print(f"\n--- Profitability Data ---")
print(f"  Total SKUs: {len(profit_data)}")
sample_items = list(profit_data.items())[:3]
for name, data in sample_items:
    print(f"  '{name}':")
    for k, v in data.items():
        print(f"    {k}: {v}")

# --- Cross-reference: Does profitability tell us the true sales volume? ---
print(f"\n--- Cross-Reference: Forecast ADS vs Profitability total_qty_sold ---")

# POS data is Jan-Nov = ~330 days (11 months)
DAYS_IN_PERIOD = 330  # Jan-Nov

matches = []
for name, f_data in forecast_data.items():
    name_upper = name.strip().upper()
    p_data = profit_data.get(name_upper, profit_data.get(name, {}))
    
    if p_data:
        forecast_ads = f_data.get("avg_daily_sales", 0)
        total_qty_sold = p_data.get("total_qty_sold", 0)
        revenue = p_data.get("revenue", 0)
        
        # Compute POS-derived ADS (total_qty / days in period)
        pos_ads = total_qty_sold / DAYS_IN_PERIOD if DAYS_IN_PERIOD > 0 else 0
        
        if forecast_ads > 0 and total_qty_sold > 0:
            ratio = pos_ads / forecast_ads
            matches.append({
                "product": name_upper[:40],
                "forecast_ads": forecast_ads,
                "total_qty_sold": total_qty_sold,
                "pos_ads": pos_ads,
                "ratio": ratio,
                "revenue": revenue,
            })

df_compare = pd.DataFrame(matches)
print(f"  Matched products: {len(df_compare)}")

if len(df_compare) > 0:
    print(f"\n  Ratio (POS ADS / Forecast ADS):")
    print(f"    Mean:   {df_compare['ratio'].mean():.2f}x")
    print(f"    Median: {df_compare['ratio'].median():.2f}x")
    print(f"    Min:    {df_compare['ratio'].min():.2f}x")
    print(f"    Max:    {df_compare['ratio'].max():.2f}x")
    print(f"    Std:    {df_compare['ratio'].std():.2f}")

    # Distribution of ratios
    print(f"\n  Ratio Distribution:")
    buckets = [0, 0.5, 1, 2, 3, 5, 10, float('inf')]
    labels = ['<0.5x', '0.5-1x', '1-2x', '2-3x', '3-5x', '5-10x', '>10x']
    ratio_cut = pd.cut(df_compare['ratio'], bins=buckets, labels=labels)
    for label in labels:
        count = (ratio_cut == label).sum()
        pct = count / len(df_compare) * 100
        print(f"    {label:<8} {count:>6} SKUs  ({pct:>5.1f}%)")

    # Total daily cost velocity comparison
    scorecard_ads = pd.to_numeric(scorecard['Avg_Daily_Sales'], errors='coerce').fillna(0)
    scorecard_price = pd.to_numeric(scorecard['Unit_Price'], errors='coerce').fillna(0)
    scorecard_margin = pd.to_numeric(scorecard['Margin_Pct'], errors='coerce').fillna(25)
    scorecard_cost = scorecard_price * (1 - scorecard_margin / 100)
    
    total_daily_cost_forecast = (scorecard_ads * scorecard_cost).sum()
    
    # POS-derived total  
    total_pos_ads_cost = 0
    for name, f_data in forecast_data.items():
        name_upper = name.strip().upper()
        p_data = profit_data.get(name_upper, profit_data.get(name, {}))
        if p_data:
            total_qty = p_data.get("total_qty_sold", 0)
            pos_ads = total_qty / DAYS_IN_PERIOD
            # Get price from scorecard
            match = scorecard[scorecard['Product'] == name_upper]
            if len(match) > 0:
                cost = float(match.iloc[0]['Unit_Price']) * (1 - float(match.iloc[0]['Margin_Pct']) / 100)
                total_pos_ads_cost += pos_ads * cost

    print(f"\n  Total Daily Cost Velocity:")
    print(f"    From Forecast ADS:     KES {total_daily_cost_forecast:>12,.0f}/day")
    print(f"    From POS total_qty:    KES {total_pos_ads_cost:>12,.0f}/day")
    if total_daily_cost_forecast > 0:
        print(f"    POS / Forecast ratio:  {total_pos_ads_cost / total_daily_cost_forecast:.2f}x")
    
    print(f"\n  Stock Value Projections:")
    for depth in [14, 21, 28]:
        forecast_stock = total_daily_cost_forecast * depth
        pos_stock = total_pos_ads_cost * depth
        print(f"    {depth}-day: Forecast=KES {forecast_stock:>12,.0f}  POS=KES {pos_stock:>12,.0f}")

    # Show top 10 products with biggest ADS gap
    print(f"\n  Top 10 Products with Biggest Undervaluation (POS >> Forecast):")
    top_gaps = df_compare.nlargest(10, 'ratio')
    for _, row in top_gaps.iterrows():
        print(f"    {row['product']:<40} | Forecast: {row['forecast_ads']:>7.2f} | POS: {row['pos_ads']:>7.2f} | {row['ratio']:>5.1f}x")
    
    # AND show some products where they roughly match
    print(f"\n  Products Where Forecast ~ POS (ratio 0.8-1.2):")
    close_match = df_compare[(df_compare['ratio'] >= 0.8) & (df_compare['ratio'] <= 1.2)]
    print(f"    {len(close_match)} products match closely ({len(close_match)/len(df_compare)*100:.1f}%)")

# --- Check: what does the forecast file's avg_daily_sales represent? ---
print(f"\n--- Investigating Forecast avg_daily_sales Calculation ---")
# Check if there are other fields that hint at the calculation method
all_keys = set()
for name, f_data in forecast_data.items():
    all_keys.update(f_data.keys())
print(f"  All keys in forecast data: {sorted(all_keys)}")

# Check a high-volume item
high_vol_items = sorted(forecast_data.items(), 
                         key=lambda x: x[1].get("avg_daily_sales", 0), 
                         reverse=True)[:5]
print(f"\n  Top 5 by Forecast ADS:")
for name, data in high_vol_items:
    name_upper = name.strip().upper()
    p_data = profit_data.get(name_upper, profit_data.get(name, {}))
    total_qty = p_data.get("total_qty_sold", 0) if p_data else 0
    pos_ads = total_qty / DAYS_IN_PERIOD if total_qty > 0 else 0
    print(f"    {name_upper[:50]}")
    print(f"      Forecast ADS: {data.get('avg_daily_sales', 0):.2f}")
    print(f"      POS total_qty: {total_qty:,.0f} (ADS: {pos_ads:.2f})")
    for k, v in data.items():
        if k != "avg_daily_sales":
            print(f"      {k}: {v}")
    print()

print("=" * 80)
