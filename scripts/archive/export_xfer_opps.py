import sys, os, json, math
import pandas as pd

sys.path.insert(0, r'C:\Users\iLink\.gemini\antigravity\scratch')
from oasis.logic.db_connector import UniversalConnector, SchemaMapper
from oasis.logic.pos_erp_adapter import PosErpAdapter
from oasis.logic.fulfillment_decider import FulfillmentDecider, NetworkAvailabilityMap, StoreSkuState

DATA_DIR = r'C:\Users\iLink\.gemini\antigravity\scratch\oasis\data'
uri = 'sqlite:///C:/Users/iLink/.gemini/antigravity/scratch/oasis/data/mock_pos_erp_showcase.db'
conn = UniversalConnector(uri, SchemaMapper.for_pos_erp())
adapter = PosErpAdapter(conn)

print("Fetching organizations...")
orgs = adapter.fetch_all_organizations()
org_cds = [o["ORG_CD"] for o in orgs]
org_name_map = {o["ORG_CD"]: o.get("ORG_NAME", o["ORG_CD"]) for o in orgs}

print("Loading mapping files...")
bcode_map_file = os.path.join(DATA_DIR, 'product_barcode_map.json')
with open(bcode_map_file, 'r', encoding='utf-8') as f:
    _bcode_map = json.load(f)

print("Fetching network stock...")
_net_stock = {}
for org_cd in org_cds:
    _net_stock[org_cd] = adapter.fetch_enriched_products(org_cd)

print("Building network map...")
_nmap = NetworkAvailabilityMap()
_store_excess_count = {}
_store_deficit_items = {}

for _org_cd, _prods in _net_stock.items():
    _excess_n = 0
    _deficits = []
    for _p in _prods:
        _ads   = float(_p.get("avg_daily_sales", 0) or 0)
        _curr  = float(_p.get("current_stocks", 0) or 0)
        _safe  = _ads * 2.0
        _excs  = _curr - _safe
        _pname = str(_p.get("product_name", ""))
        _bcode = _bcode_map.get(_pname, "")
        _dept  = str(_p.get("department", _p.get("category", "GENERAL"))).upper()
        _fresh = bool(_p.get("is_fresh", False)) or any(
            k in _dept for k in ["MILK","DAIRY","FRESH","MEAT","BREAD","BAKERY"]
        )
        _nmap.add(StoreSkuState(
            org_cd=_org_cd,
            org_name=org_name_map.get(_org_cd, _org_cd),
            itm_cd=str(_p.get("item_code", "")),
            product_name=_pname,
            current_stock=_curr,
            avg_daily_sales=_ads,
            safety_stock=_safe,
            excess=_excs,
            is_fresh=_fresh,
            sell_price=float(_p.get("selling_price", 0) or 0),
            department=_dept,
            days_since_delivery=int(_p.get("last_days_since_last_delivery", 0) or 0),
            velocity_ratio=float(_ads / max(1.0, _curr)) if _curr > 0 else 0.0,
        ), _bcode)
        if _excs > 0:
            _excess_n += 1

        _days_cover = (_curr / _ads) if _ads > 0 else 999.0
        _pull_trigger = (
            (_ads > 0 and _days_cover < 7.0) or
            (_ads == 0 and _curr < 1.0)
        )
        if _pull_trigger:
            _deficits.append({
                "itm_cd": str(_p.get("item_code", "")),
                "product_name": _pname,
                "current_stock": _curr,
                "avg_daily_sales": _ads,
                "days_cover": round(_days_cover, 1),
                "sell_price": float(_p.get("selling_price", 0) or 0),
                "department": _dept,
                "supplier": str(_p.get("supplier_name", "") or ""),
                "uom": str(_p.get("uom", "EA")).upper(),
                "trigger": "PULL",
            })
    _store_excess_count[_org_cd] = _excess_n
    _store_deficit_items[_org_cd] = _deficits

print("Building PUSH registry...")
_cold_days = 60
_hot_days  = 14
_item_coverage = {}
for _oc2, _prods2 in _net_stock.items():
    for _p2 in _prods2:
        _ic2  = str(_p2.get("item_code", ""))
        _a2   = float(_p2.get("avg_daily_sales", 0) or 0)
        _c2   = float(_p2.get("current_stocks", 0) or 0)
        _dc2  = (_c2 / _a2) if _a2 > 0 else (0.0 if _c2 < 1.0 else 999.0)
        if _ic2 not in _item_coverage:
            _item_coverage[_ic2] = {}
        _item_coverage[_ic2][_oc2] = {
            "days_cover": _dc2,
            "current_stock": _c2,
            "avg_daily_sales": _a2,
            "product_name": str(_p2.get("product_name", "")),
            "sell_price": float(_p2.get("selling_price", 0) or 0),
            "department": str(_p2.get("department", _p2.get("category", "GENERAL"))).upper(),
            "supplier": str(_p2.get("supplier_name", "") or ""),
            "uom": str(_p2.get("uom", "EA")).upper(),
            "safety_stock": float(_p2.get("avg_daily_sales", 0) or 0) * 2.0,
            "excess": _c2 - float(_p2.get("avg_daily_sales", 0) or 0) * 2.0,
        }

_push_items = []
for _ic2, _cov_map in _item_coverage.items():
    _cold_stores = [(oc, d) for oc, d in _cov_map.items() if d["days_cover"] > _cold_days and d["excess"] > 0]
    _hot_stores  = [(oc, d) for oc, d in _cov_map.items() if d["days_cover"] < _hot_days]
    for _donor_oc, _donor_d in _cold_stores:
        for _recip_oc, _recip_d in _hot_stores:
            if _donor_oc == _recip_oc:
                continue
            _xfer_qty = min(_donor_d["excess"] * 0.4,
                            max(1.0, (_hot_days - _recip_d["days_cover"]) * max(_recip_d["avg_daily_sales"], 0.5)))
            
            _push_uom = str(_donor_d.get("uom", "EA")).upper()
            if _push_uom != "KG":
                _xfer_qty = math.ceil(_xfer_qty)
            else:
                _xfer_qty = round(_xfer_qty, 1)
            
            if _xfer_qty < 1:
                continue
            _push_items.append({
                "itm_cd":      _ic2,
                "product_name": _donor_d["product_name"],
                "from_org":    _donor_oc,
                "to_org":      _recip_oc,
                "transfer_qty": round(_xfer_qty, 1),
                "donor_days":  round(_donor_d["days_cover"], 1),
                "recip_days":  round(_recip_d["days_cover"], 1),
                "donor_excess": round(_donor_d["excess"], 1),
                "sell_price":  _donor_d["sell_price"],
                "department":  _donor_d["department"],
                "supplier":    _donor_d["supplier"],
                "uom":         _push_uom,
                "trigger":     "PUSH",
            })

_push_items.sort(key=lambda x: -(x["transfer_qty"] * x["sell_price"]))

print("Generating opportunities...")
_decider = FulfillmentDecider(
    transfer_cost_kes=500.0,
    distance_map={}, # Using empty distance map for pure logic dump
    warehouse_hubs=[],
)

_xfer_opps = []
for _rec_org, _deficits in _store_deficit_items.items():
    _rec_name   = org_name_map.get(_rec_org, _rec_org)
    for _item in _deficits[:50]:
        _itm_cd   = _item["itm_cd"]
        _pname    = _item["product_name"]
        _ads      = _item["avg_daily_sales"]
        _curr     = _item["current_stock"]
        _days_cov = _item["days_cover"]
        _price    = _item["sell_price"]

        _target_qty = max(_ads * 7.0, 1.0) if _ads > 0 else 2.0
        _shortfall  = max(0.0, _target_qty - _curr)
        if _shortfall < 0.1:
            continue

        _donors = _nmap.find_donors(
            _itm_cd, _rec_org,
            product_name=_pname,
            distance_calc=_decider._calculate_distance_km,
        )
        if not _donors:
            continue

        _best = _donors[0]
        _xfer_qty = min(_best.excess * 0.5, _shortfall)
        _uom = _item.get("uom", "EA")
        if _uom != "KG":
            _xfer_qty = math.ceil(_xfer_qty)
        else:
            _xfer_qty = round(_xfer_qty, 1)
        if _xfer_qty < 1:
            continue

        _value_kes = _xfer_qty * _price
        _xfer_opps.append({
            "Type":           "PULL",
            "Product":        _pname[:45],
            "From":           org_name_map.get(_best.org_cd, _best.org_cd),
            "To":             _rec_name,
            "Transfer Qty":   round(_xfer_qty, 1),
            "Donor Days Cover": round(_best.current_stock / max(_best.avg_daily_sales, 0.001), 1),
            "Rcpt Days Cover": _days_cov,
            "Donor Excess":   round(_best.excess, 1),
            "Value (KES)":    round(_value_kes, 0),
            "Department":     _item["department"],
            "Supplier":       _item.get("supplier", ""),
        })

_seen_push = set()
for _pi in _push_items[:200]:
    _pk = (_pi["itm_cd"], _pi["from_org"], _pi["to_org"])
    if _pk in _seen_push:
        continue
    _seen_push.add(_pk)
    _value_kes = _pi["transfer_qty"] * _pi["sell_price"]
    _xfer_opps.append({
        "Type":           "PUSH",
        "Product":        _pi["product_name"][:45],
        "From":           org_name_map.get(_pi["from_org"], _pi["from_org"]),
        "To":             org_name_map.get(_pi["to_org"], _pi["to_org"]),
        "Transfer Qty":   _pi["transfer_qty"],
        "Donor Days Cover": _pi["donor_days"],
        "Rcpt Days Cover": _pi["recip_days"],
        "Donor Excess":   _pi["donor_excess"],
        "Value (KES)":    round(_value_kes, 0),
        "Department":     _pi["department"],
        "Supplier":       _pi["supplier"],
    })

_xfer_opps.sort(key=lambda x: -x["Value (KES)"])

print(f"Total opportunities found: {len(_xfer_opps)}")

df = pd.DataFrame(_xfer_opps)
csv_path = r'C:\Users\iLink\.gemini\antigravity\scratch\transfer_opportunities.csv'
df.to_csv(csv_path, index=False)
print(f"Saved to {csv_path}")

md_path = r'C:\Users\iLink\.gemini\antigravity-ide\brain\74de5ab8-0567-40f7-9300-fe8b3139058c\transfer_opportunities_top100.md'
with open(md_path, 'w', encoding='utf-8') as f:
    f.write('# Top 100 Recommended Item-Level Transfers\n\n')
    f.write(f'A total of **{len(_xfer_opps)} transfer opportunities** were identified across the network. Below are the top 100 by value. The full list has been saved to a CSV file.\n\n')
    
    headers = list(df.columns)
    f.write('| ' + ' | '.join(headers) + ' |\n')
    f.write('| ' + ' | '.join(['---'] * len(headers)) + ' |\n')
    
    for _, row in df.head(100).iterrows():
        row_str = ' | '.join(str(val) for val in row.values)
        f.write(f'| {row_str} |\n')

print(f"Saved MD artifact to {md_path}")
