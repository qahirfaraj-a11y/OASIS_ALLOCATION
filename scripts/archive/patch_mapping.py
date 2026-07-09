import sys

def patch_fulfillment_decider():
    file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\fulfillment_decider.py'
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # 1. Update NetworkAvailabilityMap.add
    old_add = """    def add(self, state: StoreSkuState):
        if state.itm_cd not in self._index:
            self._index[state.itm_cd] = []
        self._index[state.itm_cd].append(state)"""
    new_add = """    def add(self, state: StoreSkuState, bcode: str = ''):
        keys = {state.itm_cd, state.product_name}
        if bcode:
            keys.add(bcode)
        for k in keys:
            if k:
                if k not in self._index:
                    self._index[k] = []
                # avoid duplicates
                if not any(x.org_cd == state.org_cd for x in self._index[k]):
                    self._index[k].append(state)"""
    content = content.replace(old_add, new_add)

    # 2. Update find_donors signature and search logic
    old_find = """    def find_donors(self, itm_cd: str, recipient_org: str,
                    min_excess_ratio: float = 2.0,
                    distance_calc: Optional[Any] = None,
                    use_dynamic_ratio: bool = True,
                    warehouse_hubs: Optional[List[str]] = None) -> List[StoreSkuState]:
        \"\"\"
        Find stores that have excess stock for this item, prioritized by proximity and volume."""
    new_find = """    def find_donors(self, itm_cd: str, recipient_org: str,
                    min_excess_ratio: float = 2.0,
                    distance_calc: Optional[Any] = None,
                    use_dynamic_ratio: bool = True,
                    warehouse_hubs: Optional[List[str]] = None,
                    product_name: str = '') -> List[StoreSkuState]:
        \"\"\"
        Find stores that have excess stock for this item, prioritized by proximity and volume."""
    content = content.replace(old_find, new_find)

    old_search = """        donors = []
        for state in self._index.get(itm_cd, []):"""
    new_search = """        donors = []
        candidates = self._index.get(itm_cd, [])
        if not candidates and product_name:
            candidates = self._index.get(product_name, [])
        for state in candidates:"""
    content = content.replace(old_search, new_search)

    # 3. Update decide to pass product_name
    old_decide = """        donors = network_map.find_donors(
            itm_cd=itm_cd,
            recipient_org=recipient_org,
            distance_calc=self.distance_map,
            warehouse_hubs=self.warehouse_hubs
        )"""
    new_decide = """        donors = network_map.find_donors(
            itm_cd=itm_cd,
            recipient_org=recipient_org,
            distance_calc=self.distance_map,
            warehouse_hubs=self.warehouse_hubs,
            product_name=product_name
        )"""
    content = content.replace(old_decide, new_decide)

    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("fulfillment_decider.py patched successfully.")

def patch_consolidated_transfer_service():
    file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\logic\consolidated_transfer_service.py'
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    old_build = """    def _build_network_map(self) -> NetworkAvailabilityMap:
        \"\"\"Build cross-store availability index from current stock data.\"\"\"
        nmap = NetworkAvailabilityMap()

        for org_cd, products in self.stock_data.items():
            org_name = self.org_names.get(org_cd, org_cd)
            for p in products:
                ads = float(p.get('avg_daily_sales', 0) or 0)
                current = float(p.get('current_stocks', 0) or 0)
                safety = ads * 2.0  # 2 days cover as minimum safety stock
                excess = current - safety

                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                is_fresh = any(k in dept for k in [
                    'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY'
                ]) or p.get('is_fresh', False)

                nmap.add(StoreSkuState(
                    org_cd=org_cd,
                    org_name=org_name,
                    itm_cd=str(p.get('itm_cd', p.get('item_code', p.get('product_name', '')))),
                    product_name=str(p.get('product_name', 'Unknown')),
                    current_stock=current,
                    avg_daily_sales=ads,
                    safety_stock=safety,
                    excess=excess,
                    is_fresh=is_fresh,
                    sell_price=float(p.get('selling_price', p.get('sell_price', 0)) or 0),
                    department=dept,
                    days_since_delivery=int(p.get('last_days_since_last_delivery', 0) or 0),
                    velocity_ratio=float(ads / max(1.0, current)) if current > 0 else 0.0
                ))
        return nmap"""

    new_build = """    def _build_network_map(self) -> NetworkAvailabilityMap:
        \"\"\"Build cross-store availability index from current stock data.\"\"\"
        nmap = NetworkAvailabilityMap()
        
        # Load barcode map
        import json
        try:
            with open(r'C:\\Users\\iLink\\.gemini\\antigravity\\scratch\\oasis\\data\\product_barcode_map.json', 'r', encoding='utf-8') as f:
                bcode_map = json.load(f)
        except:
            bcode_map = {}

        for org_cd, products in self.stock_data.items():
            org_name = self.org_names.get(org_cd, org_cd)
            for p in products:
                ads = float(p.get('avg_daily_sales', 0) or 0)
                current = float(p.get('current_stocks', 0) or 0)
                safety = ads * 2.0  # 2 days cover as minimum safety stock
                excess = current - safety

                dept = str(p.get('department', p.get('product_category', 'GENERAL'))).upper()
                is_fresh = any(k in dept for k in [
                    'MILK', 'DAIRY', 'FRESH', 'MEAT', 'BREAD', 'BAKERY'
                ]) or p.get('is_fresh', False)

                pname = str(p.get('product_name', 'Unknown'))
                bcode = bcode_map.get(pname, '')

                nmap.add(StoreSkuState(
                    org_cd=org_cd,
                    org_name=org_name,
                    itm_cd=str(p.get('itm_cd', p.get('item_code', p.get('product_name', '')))),
                    product_name=pname,
                    current_stock=current,
                    avg_daily_sales=ads,
                    safety_stock=safety,
                    excess=excess,
                    is_fresh=is_fresh,
                    sell_price=float(p.get('selling_price', p.get('sell_price', 0)) or 0),
                    department=dept,
                    days_since_delivery=int(p.get('last_days_since_last_delivery', 0) or 0),
                    velocity_ratio=float(ads / max(1.0, current)) if current > 0 else 0.0
                ), bcode)
        return nmap"""
    content = content.replace(old_build, new_build)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("consolidated_transfer_service.py patched successfully.")

def patch_ops_dashboard():
    file_path = r'C:\Users\iLink\.gemini\antigravity\scratch\ops_dashboard.py'
    with open(file_path, 'r', encoding='utf-8') as f:
        content = f.read()

    # We need to inject ADS from product intelligence
    # Since fetch_product_master takes a while, we will just cache it.
    import_injection = """import streamlit as st
import torch
"""
    if "def _cached_ads_map(org_cd: str):" not in content:
        cached_ads_logic = """
@st.cache_data(ttl=3600)
def _cached_ads_map(org_cd: str):
    adapter = get_adapter()
    try:
        products = adapter.fetch_product_master(org_cd)
        return {p.get('item_code', ''): float(p.get('avg_daily_sales', 0.0)) for p in products}
    except:
        return {}
"""
        content = content.replace('def get_all_store_risks(sim_hour: int):', cached_ads_logic + '\ndef get_all_store_risks(sim_hour: int):')

    old_stocks_loop = """        for i, src in enumerate(gnn_sim.stores_data):
            org_cd = src.get('store_id', '')
            stocks = all_stocks.get(org_cd, [])
            
            so_ratio = 0.0
            crit_ratio = 0.0"""
            
    new_stocks_loop = """        for i, src in enumerate(gnn_sim.stores_data):
            org_cd = src.get('store_id', '')
            stocks = all_stocks.get(org_cd, [])
            
            # --- INJECT ADS ---
            ads_map = _cached_ads_map(org_cd)
            for item in stocks:
                ic = item.get('item_code', '')
                if ic in ads_map:
                    item['avg_daily_sales'] = ads_map[ic]
            # ------------------
            
            so_ratio = 0.0
            crit_ratio = 0.0"""
    content = content.replace(old_stocks_loop, new_stocks_loop)
    with open(file_path, 'w', encoding='utf-8') as f:
        f.write(content)
    print("ops_dashboard.py patched successfully.")

if __name__ == '__main__':
    patch_fulfillment_decider()
    patch_consolidated_transfer_service()
    patch_ops_dashboard()
