import os
import sys

sys.path.append(os.getcwd())

from allocation_app import load_and_run_allocation

budget = 100000000
basket_df, cash_spend, consignment_val, alloc_summary, seasonal_map = load_and_run_allocation(budget, "JAN")

# look at the reasons to see what guards triggered
df_guards = basket_df[basket_df['Reasoning'].str.contains('GUARD', na=False)]
print(f"Items blocked/reduced by guards: {len(df_guards)}")
guard_reasons = df_guards['Reasoning'].value_counts().head(20)
print(guard_reasons)

def extract_guard(reason_str):
    import re
    match = re.search(r'\[GUARD:[^\]]+\]', reason_str)
    return match.group(0) if match else "None"

basket_df['guard_tag'] = basket_df['Reasoning'].apply(extract_guard)
print(basket_df['guard_tag'].value_counts().head(10))

print(f"Total Cash Spend: {cash_spend:,.2f}")
print(f"Total Engine Total Used: {alloc_summary['total_cash_used']:,.2f}")
