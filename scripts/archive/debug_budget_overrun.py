import sys
import os
import pandas as pd

sys.path.append(os.getcwd())

from app.logic.order_engine import OrderEngine

SCORECARD_FILE = r"Full_Product_Allocation_Scorecard_v3.csv"

# Load CSV
df = pd.read_csv(SCORECARD_FILE)

# Convert to recommendations (same as allocation_app.py)
recommendations = []
for _, row in df.iterrows():
    rec = {
        'product_name': row.get('Product'),
        'selling_price': float(row.get('Unit_Price', 0) if pd.notnull(row.get('Unit_Price')) else 0),
        'avg_daily_sales': float(row.get('Avg_Daily_Sales', 0) if pd.notnull(row.get('Avg_Daily_Sales')) else 0),
        'product_category': row.get('Department', 'GENERAL'),
        'pack_size': 1,
        'moq_floor': 0,
        'historical_order_count': 0,
        'is_staple_override': str(row.get('Is_Staple', 'False')).upper() == 'TRUE',
        'margin_pct': float(row.get('Margin_Pct')) if pd.notnull(row.get('Margin_Pct')) else None,
        'recommended_quantity': 0,
        'reasoning': ''
    }
    recommendations.append(rec)

# Run allocation
engine = OrderEngine(os.getcwd())
budget = 300_000
result = engine.apply_greenfield_allocation(recommendations, budget)

final_recs = result['recommendations']
summary = result['summary']

# Calculate totals the same way as UI
total_cash_spend_reported = 0.0
total_consignment_reported = 0.0

# Build product map
product_data_map = {}
for _, row in df.iterrows():
    product_name = row.get('Product')
    if product_name:
        product_data_map[product_name] = {
            'margin_pct': row.get('Margin_Pct') if pd.notnull(row.get('Margin_Pct')) else None
        }

for r in final_recs:
    qty = r['recommended_quantity']
    if qty > 0:
        price = r['selling_price']
        is_consignment = r.get('is_consignment', False)
        
        # Calculate cost same as UI
        cost_price = None
        
        # Try GRN
        if cost_price is None and hasattr(engine, 'grn_db'):
            p_name = r['product_name']
            p_barcode = str(r.get('barcode', '')).strip()
            grn_key = p_barcode if p_barcode else engine.normalize_product_name(p_name)
            grn_stat = engine.grn_db.get(grn_key)
            if grn_stat and grn_stat.get('avg_cost'):
                cost_price = grn_stat['avg_cost']
        
        # Try Margin
        if cost_price is None:
            product_info = product_data_map.get(r['product_name'])
            if product_info:
                margin_pct = product_info['margin_pct']
                if margin_pct is not None and margin_pct >= 0 and margin_pct < 100:
                    cost_price = price * (1 - margin_pct / 100.0)
        
        # Fallback
        if cost_price is None or cost_price <= 0:
            cost_price = price * 0.75
        
        cost = qty * cost_price
        
        if is_consignment:
            total_consignment_reported += cost
        else:
            total_cash_spend_reported += cost

print("="*80)
print(f"BUDGET VERIFICATION TEST (Budget: ${budget:,.0f})")
print("="*80)
print(f"\nEngine Summary:")
print(f"  Pass 1 Cash:  ${summary['pass1_cash']:,.2f}")
print(f"  Pass 2 Cash:  ${summary['pass2_cash']:,.2f}")
print(f"  Total Cash:   ${summary['total_cash_used']:,.2f}")
print(f"  Consignment:  ${summary['total_consignment']:,.2f}")
print(f"  Unused:       ${summary['unused_budget']:,.2f}")

print(f"\nUI Reporting Calculation:")
print(f"  Cash Spend:   ${total_cash_spend_reported:,.2f}")
print(f"  Consignment:  ${total_consignment_reported:,.2f}")

print(f"\nMismatch Analysis:")
print(f"  Engine Total: ${summary['total_cash_used']:,.2f}")
print(f"  UI Total:     ${total_cash_spend_reported:,.2f}")
print(f"  Difference:   ${total_cash_spend_reported - summary['total_cash_used']:,.2f}")

if total_cash_spend_reported > budget:
    print(f"\n⚠️  BUDGET OVERRUN: ${total_cash_spend_reported - budget:,.2f} over budget!")
    print(f"  Engine thinks: ${summary['total_cash_used']:,.2f} spent")
    print(f"  UI reports:    ${total_cash_spend_reported:,.2f} spent")
else:
    print(f"\n✓ Budget OK: ${budget - total_cash_spend_reported:,.2f} remaining")
