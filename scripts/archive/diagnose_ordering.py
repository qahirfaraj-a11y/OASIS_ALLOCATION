import sqlite3, os
os.environ['PYTHONIOENCODING'] = 'utf-8'

db_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data\mock_pos_erp.db'
conn = sqlite3.connect(db_path)
conn.row_factory = sqlite3.Row

# --- ADS summary ---
r2 = conn.execute(
    "SELECT COUNT(DISTINCT ITM_CD) as n, AVG(daily_qty) as avg_ads, MAX(daily_qty) as max_ads "
    "FROM (SELECT ITM_CD, SUM(QTY)/90.0 as daily_qty "
    "      FROM POS_SALES_DTL WHERE ORG_CD='ORG001' AND VOID_FLAG='F' GROUP BY ITM_CD)"
).fetchone()
print("ADS SUMMARY:")
print(f"  SKUs with sales: {r2['n']}  AvgADS: {r2['avg_ads']:.4f}  MaxADS: {r2['max_ads']:.2f}")

# --- Top 5 items by ADS to check for inflation ---
r3 = conn.execute(
    "SELECT i.ITM_LONG_NAME as nm, i.DEPARTMENT as dept, SUM(d.QTY)/90.0 as ads "
    "FROM POS_SALES_DTL d JOIN ITEM_MST i ON i.ITM_CD=d.ITM_CD "
    "WHERE d.ORG_CD='ORG001' AND d.VOID_FLAG='F' "
    "GROUP BY d.ITM_CD ORDER BY ads DESC LIMIT 5"
).fetchall()
print("TOP 5 BY ADS:")
for r in r3:
    print(f"  [{str(r['dept'])[:20]:20}] {str(r['nm'])[:35]:35} ADS={r['ads']:.2f}")

# --- Load all SKUs with stock and ADS ---
all_skus = conn.execute(
    "SELECT i.ITM_LONG_NAME as name, i.DEPARTMENT as dept, "
    "       s.SM_QTY as stock, COALESCE(ads.daily_qty,0) as ads "
    "FROM ITEM_MST i "
    "JOIN STOCK_MASTER s ON s.SM_ITM_CD=i.ITM_CD AND s.SM_ORG_CD='ORG001' "
    "LEFT JOIN (SELECT ITM_CD, SUM(QTY)/90.0 as daily_qty "
    "           FROM POS_SALES_DTL WHERE ORG_CD='ORG001' AND VOID_FLAG='F' GROUP BY ITM_CD) ads "
    "ON ads.ITM_CD=i.ITM_CD WHERE i.ACTIVE_FLAG='Y'"
).fetchall()

total = len(all_skus)
no_ads = sum(1 for r in all_skus if r['ads'] == 0)
has_ads = total - no_ads

FRESH_KW = ['DAIRY','FRESH','BAKERY','BUTCH','DELI','MEAT','CREAM']

would_order_fresh = would_order_dry = no_order_fresh = no_order_dry = 0
dry_covers = []
fresh_covers = []
dry_above_rop_with_ads = 0

for r in all_skus:
    dept = str(r['dept'] or '').upper()
    ads = r['ads']
    stock = r['stock']
    is_fresh = any(k in dept for k in FRESH_KW)
    rop = ads * (2 if is_fresh else 7)
    dc = stock / ads if ads > 0 else 9999
    if is_fresh:
        fresh_covers.append(dc)
    else:
        dry_covers.append(dc)
    if ads > 0 and stock <= rop:
        if is_fresh: would_order_fresh += 1
        else: would_order_dry += 1
    else:
        if is_fresh: no_order_fresh += 1
        else: no_order_dry += 1
        if not is_fresh and ads > 0:
            dry_above_rop_with_ads += 1

print("\nREORDER TRIGGER ANALYSIS:")
print(f"  Total active SKUs:             {total}")
print(f"  SKUs with NO ADS (zero sales): {no_ads}  (cannot order - no demand signal)")
print(f"  SKUs with sales data:          {has_ads}")
print(f"  Would Order - FRESH:           {would_order_fresh}")
print(f"  Would Order - DRY:             {would_order_dry}")
print(f"  No Order   - FRESH:            {no_order_fresh}")
print(f"  No Order   - DRY:              {no_order_dry}")
print(f"  Dry with ADS but above ROP:    {dry_above_rop_with_ads}  << KEY NUMBER")

valid_dry = [c for c in dry_covers if c < 9999]
valid_fresh = [c for c in fresh_covers if c < 9999]
avg_dry = sum(valid_dry)/len(valid_dry) if valid_dry else 0
avg_fresh = sum(valid_fresh)/len(valid_fresh) if valid_fresh else 0
max_dry = max(valid_dry) if valid_dry else 0

print(f"\nSTOCK COVER:")
print(f"  Avg DRY cover:   {avg_dry:.1f} days  (ROP threshold = 7 days)")
print(f"  Avg FRESH cover: {avg_fresh:.1f} days  (ROP threshold = 2 days)")
print(f"  Max DRY cover:   {max_dry:.1f} days")

print("\nDRY GOODS DAYS-OF-COVER DISTRIBUTION:")
b = {'<7d (below ROP - should order)':0, '7-14d':0, '14-30d':0, '30-60d':0, '60-120d':0, '>120d (massively inflated)':0}
for c in valid_dry:
    if c < 7: b['<7d (below ROP - should order)'] += 1
    elif c < 14: b['7-14d'] += 1
    elif c < 30: b['14-30d'] += 1
    elif c < 60: b['30-60d'] += 1
    elif c < 120: b['60-120d'] += 1
    else: b['>120d (massively inflated)'] += 1
for k, v in b.items():
    print(f"  {k:40}: {v:6}")

print("\nFRESH GOODS DAYS-OF-COVER DISTRIBUTION:")
bf = {'<1d':0,'1-2d':0,'2-5d':0,'5-14d':0,'>14d':0}
for c in valid_fresh:
    if c < 1: bf['<1d'] += 1
    elif c < 2: bf['1-2d'] += 1
    elif c < 5: bf['2-5d'] += 1
    elif c < 14: bf['5-14d'] += 1
    else: bf['>14d'] += 1
for k, v in bf.items():
    print(f"  {k:40}: {v:6}")

# --- Sample top dry with their cover ---
print("\nTOP 10 DRY GOODS BY ADS (with days cover):")
top_dry = sorted(
    [r for r in all_skus if r['ads']>0 and not any(k in str(r['dept'] or '').upper() for k in FRESH_KW)],
    key=lambda x: -x['ads']
)[:10]
print(f"  {'Name':40} {'Dept':25} {'Stock':>7} {'ADS':>8} {'Cover':>8}")
print("  " + "-"*92)
for r in top_dry:
    dc = r['stock']/r['ads'] if r['ads'] > 0 else 9999
    print(f"  {str(r['name'])[:40]:40} {str(r['dept'] or '')[:25]:25} {r['stock']:>7.1f} {r['ads']:>8.3f} {dc:>7.0f}d")

# --- Root cause summary ---
print("\nROOT CAUSE DIAGNOSIS:")
if avg_dry > 30:
    print(f"  [CONFIRMED] DRY STOCK IS MASSIVELY OVER-SEEDED: {avg_dry:.0f}d average cover.")
    print(f"             The ordering engine is CORRECT not to order -- stock is nowhere near ROP.")
    print(f"             Fix: Re-seed dry goods to ADS x 5-10 days (not ADS x {avg_dry:.0f}d).")
if avg_fresh < 5:
    print(f"  [CONFIRMED] FRESH STOCK IS CORRECTLY LEAN: {avg_fresh:.1f}d average cover.")
    print(f"             Fresh items cross ROP quickly, so ONLY fresh orders appear.")
if no_ads > has_ads * 0.3:
    pct = no_ads*100//total
    print(f"  [CONFIRMED] {no_ads} SKUs ({pct}%) have NO sales history -- ghost catalog items.")
    print(f"             These can never trigger an order regardless of stock level.")
if r2['max_ads'] > 1000:
    print(f"  [WARNING]   Max ADS = {r2['max_ads']:.0f} units/day -- check for data duplication in POS.")

conn.close()
print("\nDIAGNOSTIC COMPLETE")
