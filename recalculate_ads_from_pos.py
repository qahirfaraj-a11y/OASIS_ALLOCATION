"""
ADS Recalculation from Raw POS Cash Files
==========================================
Reads monthly POS data, excludes July (outage), computes true ADS per SKU,
and compares against scorecard values.
"""
import pandas as pd
import numpy as np
import os
import json

DATA_DIR = r"c:\Users\iLink\.gemini\antigravity\scratch\oasis\data"

# Monthly POS files and calendar days per month (2025)
POS_FILES = {
    "January":   ("jan_cash.xlsx", 31),
    "March":     ("mar_cash.xlsx", 31),
    "May":       ("may_cash.xlsx", 31),
    "June":      ("jun_cash.xlsx", 30),
    "July":      ("jul_cash.xlsx", 31),  # Will be excluded (outage)
    "September": ("sep_cash.xlsx", 30),
    "October":   ("oct_cash.xlsx", 31),
}

EXCLUDE_MONTHS = {"July"}  # Known POS outage

def load_pos_file(filepath):
    """Load a POS cash file, handling varying column layouts."""
    df_raw = pd.read_excel(filepath)
    
    # Row 0 contains actual headers
    headers = df_raw.iloc[0].tolist()
    df = df_raw.iloc[1:].copy()
    df.columns = headers
    
    # Normalize column names
    col_map = {}
    for c in df.columns:
        c_str = str(c).strip().upper()
        if "ITEM" in c_str and "NAME" in c_str:
            col_map[c] = "Item_Name"
        elif c_str == "QTY":
            col_map[c] = "Qty"
        elif "DEPARTMENT" in c_str:
            col_map[c] = "Department"
    
    df = df.rename(columns=col_map)
    
    if "Item_Name" not in df.columns or "Qty" not in df.columns:
        raise ValueError(f"Missing required columns in {filepath}. Found: {list(df.columns)}")
    
    # Clean
    df["Item_Name"] = df["Item_Name"].astype(str).str.strip().str.upper()
    df["Qty"] = pd.to_numeric(df["Qty"], errors="coerce").fillna(0)
    
    # Filter out total/summary rows
    df = df[~df["Item_Name"].isin(["TOTAL", "GRAND TOTAL", ""])]
    df = df[df["Item_Name"] != "NAN"]
    
    return df

def main():
    print("=" * 80)
    print("  ADS RECALCULATION FROM RAW POS DATA")
    print("=" * 80)
    
    # =========================================================================
    # STEP 1: Load and aggregate all POS data
    # =========================================================================
    all_monthly = {}
    total_selling_days = 0
    months_used = []
    
    for month_name, (filename, days) in POS_FILES.items():
        filepath = os.path.join(DATA_DIR, filename)
        
        if month_name in EXCLUDE_MONTHS:
            print(f"  [SKIP] {month_name:>10} -- POS outage")
            continue
        
        if not os.path.exists(filepath):
            print(f"  [MISS] {month_name:>10} -- file not found")
            continue
        
        df = load_pos_file(filepath)
        
        # Aggregate by product: total qty sold in this month
        monthly_agg = df.groupby("Item_Name")["Qty"].sum()
        
        # Store
        all_monthly[month_name] = monthly_agg
        total_selling_days += days
        months_used.append(month_name)
        
        total_qty = monthly_agg.sum()
        unique_skus = len(monthly_agg)
        print(f"  [OK]   {month_name:>10} | {days:>2} days | {unique_skus:>6} SKUs | {total_qty:>10,.0f} units")
    
    print(f"\n  Total selling days (excl. July): {total_selling_days}")
    print(f"  Months used: {', '.join(months_used)}")
    
    # =========================================================================
    # STEP 2: Compute per-SKU totals and ADS
    # =========================================================================
    # Combine all months into one total
    all_products = set()
    for monthly in all_monthly.values():
        all_products.update(monthly.index)
    
    product_totals = {}
    product_months_active = {}
    
    for product in all_products:
        total = 0
        months_seen = 0
        for month_name, monthly_agg in all_monthly.items():
            if product in monthly_agg.index:
                qty = monthly_agg[product]
                if qty > 0:
                    total += qty
                    months_seen += 1
        product_totals[product] = total
        product_months_active[product] = months_seen
    
    # Compute ADS = total_qty / total_selling_days
    pos_ads = {p: total / total_selling_days for p, total in product_totals.items()}
    
    print(f"\n  Total unique products across all months: {len(pos_ads)}")
    
    # =========================================================================
    # STEP 3: Compare against scorecard
    # =========================================================================
    scorecard = pd.read_csv(r"c:\Users\iLink\.gemini\antigravity\scratch\Full_Product_Allocation_Scorecard_v7.csv")
    sc_ads = dict(zip(scorecard["Product"].str.strip().str.upper(), 
                       pd.to_numeric(scorecard["Avg_Daily_Sales"], errors="coerce").fillna(0)))
    sc_price = dict(zip(scorecard["Product"].str.strip().str.upper(),
                         pd.to_numeric(scorecard["Unit_Price"], errors="coerce").fillna(0)))
    sc_margin = dict(zip(scorecard["Product"].str.strip().str.upper(),
                          pd.to_numeric(scorecard["Margin_Pct"], errors="coerce").fillna(25)))
    
    # Match products
    comparisons = []
    for product in pos_ads:
        if product in sc_ads:
            new_ads = pos_ads[product]
            old_ads = sc_ads[product]
            price = sc_price.get(product, 0)
            margin = sc_margin.get(product, 25)
            cost = price * (1 - margin / 100)
            
            ratio = new_ads / old_ads if old_ads > 0 else float('inf')
            comparisons.append({
                "product": product,
                "old_ads": old_ads,
                "new_ads": new_ads,
                "ratio": ratio,
                "price": price,
                "cost": cost,
                "old_daily_cost": old_ads * cost,
                "new_daily_cost": new_ads * cost,
            })
    
    df_cmp = pd.DataFrame(comparisons)
    # Filter out infinite ratios for stats
    df_finite = df_cmp[df_cmp["ratio"] < 1000]
    
    print(f"\n{'='*80}")
    print(f"  COMPARISON: OLD FORECAST ADS vs NEW POS-RECALCULATED ADS")
    print(f"{'='*80}")
    print(f"  Matched products: {len(df_cmp)}")
    print(f"\n  Ratio (New POS ADS / Old Forecast ADS):")
    print(f"    Mean:   {df_finite['ratio'].mean():.2f}x")
    print(f"    Median: {df_finite['ratio'].median():.2f}x")
    print(f"    P25:    {df_finite['ratio'].quantile(0.25):.2f}x")
    print(f"    P75:    {df_finite['ratio'].quantile(0.75):.2f}x")
    
    # Distribution
    print(f"\n  Ratio Distribution:")
    buckets = [0, 0.5, 1.0, 1.5, 2.0, 3.0, 5.0, 10.0, float('inf')]
    labels = ['<0.5x', '0.5-1x', '1-1.5x', '1.5-2x', '2-3x', '3-5x', '5-10x', '>10x']
    ratio_cut = pd.cut(df_finite['ratio'], bins=buckets, labels=labels)
    for label in labels:
        count = (ratio_cut == label).sum()
        pct = count / len(df_finite) * 100
        print(f"    {label:<8} {count:>6} SKUs  ({pct:>5.1f}%)")
    
    # =========================================================================
    # STEP 4: Stock Value Impact
    # =========================================================================
    old_daily_total = df_cmp["old_daily_cost"].sum()
    new_daily_total = df_cmp["new_daily_cost"].sum()
    
    # Also compute for ALL scorecard items (including those not in POS)
    # Items not in POS keep their old ADS
    all_old_daily = 0
    all_new_daily = 0
    products_upgraded = 0
    products_kept = 0
    
    for product in sc_ads:
        price = sc_price.get(product, 0)
        margin = sc_margin.get(product, 25)
        cost = price * (1 - margin / 100)
        old_ads = sc_ads[product]
        
        if product in pos_ads and pos_ads[product] > 0:
            new_ads = pos_ads[product]
            products_upgraded += 1
        else:
            new_ads = old_ads  # Keep forecast ADS for items not in POS files
            products_kept += 1
        
        all_old_daily += old_ads * cost
        all_new_daily += new_ads * cost
    
    print(f"\n{'='*80}")
    print(f"  STOCK VALUE IMPACT")
    print(f"{'='*80}")
    print(f"  Products with POS data (upgraded): {products_upgraded}")
    print(f"  Products kept at forecast ADS:     {products_kept}")
    print(f"\n  Daily Cost Velocity (matched products only):")
    print(f"    Old (Forecast):  KES {old_daily_total:>12,.0f}/day")
    print(f"    New (POS):       KES {new_daily_total:>12,.0f}/day")
    print(f"    Change:          {new_daily_total/old_daily_total:.2f}x")
    
    print(f"\n  Daily Cost Velocity (ALL scorecard products):")
    print(f"    Old (Forecast):  KES {all_old_daily:>12,.0f}/day")
    print(f"    New (POS):       KES {all_new_daily:>12,.0f}/day")
    print(f"    Change:          {all_new_daily/all_old_daily:.2f}x")
    
    print(f"\n  Stock Value Projections (ALL products):")
    print(f"  {'Depth':>5}  {'Old Forecast':>15}  {'New POS':>15}  {'Real':>15}")
    print(f"  {'-'*55}")
    for depth in [14, 21, 28, 30]:
        old_stock = all_old_daily * depth
        new_stock = all_new_daily * depth
        target = f"{'<-- 114M' if abs(new_stock - 114_000_000) / 114_000_000 < 0.2 else ''}"
        print(f"  {depth:>5}d  KES {old_stock:>12,.0f}  KES {new_stock:>12,.0f}  {target}")
    
    # What depth gives 114M with new ADS?
    if all_new_daily > 0:
        implied_depth = 114_000_000 / all_new_daily
        print(f"\n  Implied depth for KES 114M with new ADS: {implied_depth:.1f} days")
    
    # =========================================================================
    # STEP 5: Department-level breakdown
    # =========================================================================
    print(f"\n{'='*80}")
    print(f"  DEPARTMENT BREAKDOWN (New POS ADS)")
    print(f"{'='*80}")
    
    dept_data = []
    for _, row in scorecard.iterrows():
        product = str(row["Product"]).strip().upper()
        dept = row.get("Department", "Unknown")
        price = float(row.get("Unit_Price", 0)) if pd.notna(row.get("Unit_Price")) else 0
        margin = float(row.get("Margin_Pct", 25)) if pd.notna(row.get("Margin_Pct")) else 25
        cost = price * (1 - margin / 100)
        old_ads = float(row.get("Avg_Daily_Sales", 0)) if pd.notna(row.get("Avg_Daily_Sales")) else 0
        
        if product in pos_ads and pos_ads[product] > 0:
            new_ads = pos_ads[product]
        else:
            new_ads = old_ads
        
        dept_data.append({
            "Department": dept,
            "old_daily_cost": old_ads * cost,
            "new_daily_cost": new_ads * cost,
        })
    
    dept_df = pd.DataFrame(dept_data)
    dept_summary = dept_df.groupby("Department").agg(
        old_total=("old_daily_cost", "sum"),
        new_total=("new_daily_cost", "sum"),
    ).sort_values("new_total", ascending=False)
    
    dept_summary["change"] = dept_summary["new_total"] / dept_summary["old_total"].replace(0, 1)
    
    print(f"  {'Department':<30} {'Old KES/day':>12} {'New KES/day':>12} {'Change':>8}")
    print(f"  {'-'*65}")
    for dept, row in dept_summary.head(20).iterrows():
        print(f"  {dept:<30} {row['old_total']:>12,.0f} {row['new_total']:>12,.0f} {row['change']:>7.2f}x")
    
    # =========================================================================
    # STEP 6: Save corrected ADS to JSON for reuse
    # =========================================================================
    output_path = os.path.join(DATA_DIR, "corrected_ads_from_pos.json")
    
    corrected_ads_data = {}
    for product in sc_ads:
        old_ads = sc_ads[product]
        if product in pos_ads and pos_ads[product] > 0:
            new_ads = pos_ads[product]
            source = "POS"
        else:
            new_ads = old_ads
            source = "forecast"
        
        corrected_ads_data[product] = {
            "old_ads": round(old_ads, 4),
            "new_ads": round(new_ads, 4),
            "source": source,
            "months_active": product_months_active.get(product, 0),
            "total_qty": product_totals.get(product, 0),
        }
    
    with open(output_path, "w") as f:
        json.dump(corrected_ads_data, f, indent=2)
    
    print(f"\n  [OK] Corrected ADS saved to: {output_path}")
    print(f"       {len(corrected_ads_data)} products")
    print("=" * 80)

if __name__ == "__main__":
    main()
