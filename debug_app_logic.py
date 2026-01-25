import pandas as pd
import json
import os
import sys

# Mock streamlit for the app import
class MockSt:
    def cache_data(self, func): return func
    def set_page_config(self, **kwargs): pass
    def title(self, text): pass
    def markdown(self, text): pass
    def columns(self, n): return [MockCol()] * n
    def info(self, text): pass
    def subheader(self, text): pass
    def plotly_chart(self, fig, use_container_width=True): pass
    def dataframe(self, df, height=400): pass
    def download_button(self, **kwargs): pass
    def error(self, text): print(f"ERROR: {text}")
    class sidebar:
        @staticmethod
        def header(text): pass
        @staticmethod
        def slider(label, **kwargs): return kwargs.get('value', 300000)

class MockCol:
    def metric(self, label, value, delta=None): pass

sys.modules['streamlit'] = MockSt()
import allocation_app

def test():
    print("Testing load_data...")
    df, staples, ratios = allocation_app.load_data()
    if df is None:
        print("FAIL: load_data returned None")
        return
    print(f"Loaded {len(df)} rows, {len(staples)} staples, {len(ratios)} ratio groups.")

    print("\nTesting allocate_budget (300k)...")
    basket, profile, spend = allocation_app.allocate_budget(df, 300000, staples, ratios)
    print(f"Allocated: ${spend:,.2f} ({len(basket)} SKUs)")
    print(f"Profile: {profile['name']}")
    
    print("\nTesting allocate_budget (10M)...")
    basket, profile, spend = allocation_app.allocate_budget(df, 10000000, staples, ratios)
    print(f"Allocated: ${spend:,.2f} ({len(basket)} SKUs)")

if __name__ == "__main__":
    test()
