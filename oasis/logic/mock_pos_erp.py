"""
Mock POS/ERP Database Builder
=============================
Creates a SQLite database replicating the core POS/ERP schema
(ITEM_MST, STOCK_MASTER, POS_SALES_HDR/DTL, BI_SALES_REPORT, etc.)
with realistic data seeded from stores_network.json (all 14 stores).

Usage:
    python -m oasis.logic.mock_pos_erp               # Full rebuild (all SKUs)
    python -m oasis.logic.mock_pos_erp --reset        # Drop & recreate
    python -m oasis.logic.mock_pos_erp --reset --fast # Fast mode (2,000 SKUs)
"""

import os
import json
import random
import sqlite3
import logging
from datetime import datetime, timedelta
from typing import Dict, List, Optional

logger = logging.getLogger("MockPosErp")

# Default paths
DATA_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..', 'data'))
DEFAULT_DB_PATH = os.path.join(DATA_DIR, "mock_pos_erp.db")

# Network JSON (sits at project root, 2 levels above this file)
NETWORK_JSON = os.path.abspath(
    os.path.join(os.path.dirname(__file__), '..', '..', 'stores_network.json')
)
FAST_MODE_SKU_LIMIT = 5000   # cap for --fast demo runs

# ---------------------------------------------------------------------------
# Schema Definitions (Mirrors RXL POS/ERP SQL Server tables)
# ---------------------------------------------------------------------------

SCHEMA_SQL = """
-- Organization / Store hierarchy
CREATE TABLE IF NOT EXISTS ORGANIZATION_MST (
    ORG_CD          TEXT PRIMARY KEY,
    ORG_NAME        TEXT NOT NULL,
    ORG_SHORT_NAME  TEXT,
    ORG_ADDRESS     TEXT,
    ORG_CITY        TEXT,
    ORG_STATE       TEXT,
    ORG_COUNTRY     TEXT DEFAULT 'KE',
    ORG_PIN         TEXT,
    ORG_PHONE       TEXT,
    ORG_EMAIL       TEXT,
    CURRENCY_CD     TEXT DEFAULT 'KES',
    GST_NO          TEXT,
    LEVEL_NUMBER    INTEGER DEFAULT 1,
    PARENT_ORG_CD   TEXT,
    ACTIVE_FLAG     TEXT DEFAULT 'Y'
);

-- Item / Product Master
CREATE TABLE IF NOT EXISTS ITEM_MST (
    ITM_CD          TEXT PRIMARY KEY,
    ITM_LONG_NAME   TEXT NOT NULL,
    ITM_SHORT_NAME  TEXT,
    SCAN_ITM_CD     TEXT,           -- Barcode
    HSN_CD          TEXT,           -- HSN/SAC Code
    UOM_CD          TEXT DEFAULT 'EA',
    UOM_DESC        TEXT DEFAULT 'Each',
    ITM_GROUP_CD    TEXT,
    ITM_TYPE        TEXT DEFAULT 'F',  -- F=Finished, R=Raw, S=Semi
    TAX_PLAN_CD     TEXT,
    WEIGHT_FLAG     TEXT DEFAULT 'N',
    SERIAL_FLAG     TEXT DEFAULT 'N',
    PRODUCTION_FLAG TEXT DEFAULT 'N',
    CATEGORY        TEXT,
    DEPARTMENT      TEXT,
    SUPPLIER_CD     TEXT,
    ACTIVE_FLAG     TEXT DEFAULT 'Y'
);

-- Stock levels per org/item/location
CREATE TABLE IF NOT EXISTS STOCK_MASTER (
    SM_ORG_CD       TEXT NOT NULL,
    SM_ITM_CD       TEXT NOT NULL,
    SM_LOC_CD       TEXT DEFAULT 'MAIN',
    SM_QTY          REAL DEFAULT 0,
    SM_WAC           REAL DEFAULT 0,   -- Weighted Average Cost
    SM_LAST_RECV_DT TEXT,
    SM_LAST_ISSUE_DT TEXT,
    PRIMARY KEY (SM_ORG_CD, SM_ITM_CD, SM_LOC_CD),
    FOREIGN KEY (SM_ORG_CD) REFERENCES ORGANIZATION_MST(ORG_CD),
    FOREIGN KEY (SM_ITM_CD) REFERENCES ITEM_MST(ITM_CD)
);

-- Selling Price Master
CREATE TABLE IF NOT EXISTS BASIC_SP_MST (
    BSP_ORG_CD      TEXT NOT NULL,
    BSP_ITEM_CD     TEXT NOT NULL,
    BSP_SP           REAL DEFAULT 0,
    BSP_MRP          REAL DEFAULT 0,
    BSP_EFF_DATE    TEXT,
    PRIMARY KEY (BSP_ORG_CD, BSP_ITEM_CD),
    FOREIGN KEY (BSP_ITEM_CD) REFERENCES ITEM_MST(ITM_CD)
);

-- Cost Price Master
CREATE TABLE IF NOT EXISTS BASIC_CP_MST (
    BCP_ORG_CD      TEXT NOT NULL,
    BCP_ITEM_CD     TEXT NOT NULL,
    BCP_CP           REAL DEFAULT 0,
    BCP_EFF_DATE    TEXT,
    PRIMARY KEY (BCP_ORG_CD, BCP_ITEM_CD),
    FOREIGN KEY (BCP_ITEM_CD) REFERENCES ITEM_MST(ITM_CD)
);

-- Customer Master
CREATE TABLE IF NOT EXISTS CUSTOMER_MST (
    CUST_CD         TEXT PRIMARY KEY,
    CUST_NAME       TEXT,
    CUST_SALUTATION TEXT,
    MOBILE_NO       TEXT,
    EMAIL           TEXT,
    LOYALTY_CARD_NO TEXT,
    ACTIVE_FLAG     TEXT DEFAULT 'Y'
);

-- POS Sales Header
CREATE TABLE IF NOT EXISTS POS_SALES_HDR (
    ORG_CD          TEXT NOT NULL,
    BILL_NO         TEXT NOT NULL,
    BILL_DT         TEXT NOT NULL,
    CUST_CD         TEXT,
    COUNTER_CD      TEXT,
    LEVEL_NUMBER    INTEGER DEFAULT 1,
    TOTAL_QTY       REAL DEFAULT 0,
    TOTAL_AMT       REAL DEFAULT 0,
    NET_AMT         REAL DEFAULT 0,
    TAX_AMT         REAL DEFAULT 0,
    DISC_AMT        REAL DEFAULT 0,
    PAYMENT_MODE    TEXT DEFAULT 'CASH',
    VOID_FLAG       TEXT DEFAULT 'F',
    CUS_REF_CODE    TEXT,
    CUS_REF_REMARKS TEXT,
    PRIMARY KEY (ORG_CD, BILL_NO, BILL_DT),
    FOREIGN KEY (ORG_CD) REFERENCES ORGANIZATION_MST(ORG_CD)
);

-- POS Sales Detail
CREATE TABLE IF NOT EXISTS POS_SALES_DTL (
    ORG_CD          TEXT NOT NULL,
    BILL_NO         TEXT NOT NULL,
    BILL_DT         TEXT NOT NULL,
    SERIAL_NO       INTEGER NOT NULL,
    ITM_CD          TEXT NOT NULL,
    ITEM_NAME       TEXT,
    QTY             REAL DEFAULT 0,
    SELL_PRICE      REAL DEFAULT 0,
    NET_AMT         REAL DEFAULT 0,
    TAX_AMT         REAL DEFAULT 0,
    DISC_AMT        REAL DEFAULT 0,
    NET_TAX_AMT     REAL DEFAULT 0,
    TOTAL_VALUE     REAL DEFAULT 0,
    UOM_CD          TEXT DEFAULT 'EA',
    UOM_DESC        TEXT DEFAULT 'Each',
    VOID_FLAG       TEXT DEFAULT 'F',
    PROMO_ITEM_FLAG TEXT DEFAULT 'N',
    SCAN_ITM_CD     TEXT,
    TAX_PLAN_CD     TEXT,
    PRIMARY KEY (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO),
    FOREIGN KEY (ORG_CD) REFERENCES ORGANIZATION_MST(ORG_CD),
    FOREIGN KEY (ITM_CD) REFERENCES ITEM_MST(ITM_CD)
);

-- BI Sales Report (Pre-aggregated -- mirrors iAnalytics)
CREATE TABLE IF NOT EXISTS BI_SALES_REPORT (
    ORG_CD          TEXT NOT NULL,
    ITM_CD          TEXT NOT NULL,
    REPORT_MONTH    TEXT NOT NULL,      -- YYYY-MM
    QUANTITY        REAL DEFAULT 0,
    TAX_INCL        REAL DEFAULT 0,
    TAX_EXCL        REAL DEFAULT 0,
    TOTAL_TAX       REAL DEFAULT 0,
    COST            REAL DEFAULT 0,
    TRANSACTION_COUNT INTEGER DEFAULT 0,
    PRIMARY KEY (ORG_CD, ITM_CD, REPORT_MONTH)
);

-- Counter / Till Master
CREATE TABLE IF NOT EXISTS COUNTER_MST (
    COUNTER_CD      TEXT PRIMARY KEY,
    COUNTER_NAME    TEXT,
    ORG_CD          TEXT,
    ONLINE_FLAG     TEXT DEFAULT 'Y',
    BILL_REFUND     TEXT DEFAULT 'Y',
    ACTIVE_FLAG     TEXT DEFAULT 'Y',
    FOREIGN KEY (ORG_CD) REFERENCES ORGANIZATION_MST(ORG_CD)
);

-- System Preferences (Key-Value config)
CREATE TABLE IF NOT EXISTS BASE_SYSTEM_PREFERENCES (
    BSP_PREF_ID     INTEGER PRIMARY KEY AUTOINCREMENT,
    BSP_PREF_DESC   TEXT NOT NULL UNIQUE,
    BSP_PREF_VALUE  TEXT,
    BSP_MODULE      TEXT DEFAULT 'POS'
);

-- Tax Plan Header
CREATE TABLE IF NOT EXISTS TAX_PLAN_HDR (
    TAX_PLAN_CD     TEXT PRIMARY KEY,
    TAX_PLAN_NAME   TEXT,
    TAX_PLAN_TYPE   TEXT DEFAULT 'GST',
    ACTIVE_FLAG     TEXT DEFAULT 'Y'
);

-- Tax Master
CREATE TABLE IF NOT EXISTS TAX_MST (
    TAX_CD          TEXT PRIMARY KEY,
    TAX_NAME        TEXT,
    TAX_PERC        REAL DEFAULT 0,
    TAX_TYPE        TEXT,   -- G=CGST, S=SGST, B=IGST, A=CESS
    TAX_PLAN_CD     TEXT,
    FOREIGN KEY (TAX_PLAN_CD) REFERENCES TAX_PLAN_HDR(TAX_PLAN_CD)
);

-- Supplier Master (extension for POS/ERP)
CREATE TABLE IF NOT EXISTS SUPPLIER_MST (
    SUPPLIER_CD     TEXT PRIMARY KEY,
    SUPPLIER_NAME   TEXT NOT NULL,
    CONTACT_PERSON  TEXT,
    PHONE           TEXT,
    EMAIL           TEXT,
    ADDRESS         TEXT,
    PAYMENT_TERMS   TEXT DEFAULT 'NET30',
    ORDER_FREQUENCY TEXT DEFAULT 'weekly',
    LEAD_TIME_DAYS  INTEGER DEFAULT 7,
    RELIABILITY_SCORE REAL DEFAULT 0.9,
    ACTIVE_FLAG     TEXT DEFAULT 'Y'
);

-- GRN (Goods Received Note) Header -- for supplier tracking
CREATE TABLE IF NOT EXISTS GRN_HDR (
    GRN_NO          TEXT NOT NULL,
    ORG_CD          TEXT NOT NULL,
    SUPPLIER_CD     TEXT,
    GRN_DT          TEXT NOT NULL,
    TOTAL_AMT       REAL DEFAULT 0,
    PRIMARY KEY (GRN_NO, ORG_CD)
);

-- Integration Purchase Orders (write-back from OASIS)
CREATE TABLE IF NOT EXISTS INTEGRATION_PURCHASE_ORDERS (
    PO_ID           INTEGER PRIMARY KEY AUTOINCREMENT,
    ORG_CD          TEXT,
    ITM_CD          TEXT,
    PRODUCT_NAME    TEXT,
    SUPPLIER_CD     TEXT,
    QUANTITY        REAL DEFAULT 0,
    UNIT_COST       REAL DEFAULT 0,
    TOTAL_COST      REAL DEFAULT 0,
    REASONING       TEXT,
    STATUS          TEXT DEFAULT 'PENDING',
    CREATED_DT      TEXT,
    APPROVED_DT     TEXT
);
"""


# ---------------------------------------------------------------------------
# Data Loading Helpers
# ---------------------------------------------------------------------------

def _load_json(filename: str) -> dict:
    """Load JSON file from the data directory, trying multiple name patterns."""
    # Try exact filename first
    path = os.path.join(DATA_DIR, filename)
    if os.path.exists(path):
        with open(path, 'r', encoding='utf-8') as f:
            return json.load(f)
    
    # Try finding by partial match
    for f in os.listdir(DATA_DIR):
        if filename.replace('.json', '') in f and f.endswith('.json'):
            with open(os.path.join(DATA_DIR, f), 'r', encoding='utf-8') as fh:
                return json.load(fh)
    
    logger.warning(f"Could not find {filename} in {DATA_DIR}")
    return {}


def _generate_barcode(item_idx: int) -> str:
    """Generate a realistic EAN-13 barcode."""
    return f"690{item_idx:010d}"


def _generate_item_code(item_idx: int) -> str:
    """Generate a POS item code."""
    return f"ITM{item_idx:05d}"


def _generate_bill_number(org_cd: str, day_idx: int, bill_idx: int) -> str:
    """Generate a POS bill number."""
    return f"{org_cd}-{day_idx:04d}-{bill_idx:04d}"


# ---------------------------------------------------------------------------
# Mock Database Builder
# ---------------------------------------------------------------------------

class MockPosErpBuilder:
    """Builds a SQLite mock of the POS/ERP database from stores_network.json."""

    def __init__(self, db_path: str = DEFAULT_DB_PATH, seed: int = 42,
                 fast_mode: bool = False):
        self.db_path   = db_path
        self.seed      = seed
        self.fast_mode = fast_mode
        self.conn: Optional[sqlite3.Connection] = None
        self.product_catalog: Dict[str, dict] = {}   # ITM_CD -> product info
        self.supplier_catalog: Dict[str, dict] = {}  # name.upper() -> {cd, ...}
        self.org_codes: List[str]  = []
        self.store_meta: List[dict] = []              # parallel to org_codes
        self._network: dict = {}

    def build(self, reset: bool = False) -> str:
        """Build the complete mock database. Returns the database path."""
        random.seed(self.seed)

        # Load network JSON once
        if os.path.exists(NETWORK_JSON):
            with open(NETWORK_JSON, 'r', encoding='utf-8') as f:
                self._network = json.load(f)
            logger.info(f"Loaded stores_network.json ({len(self._network.get('stores',[]))} stores)")
        else:
            logger.warning("stores_network.json not found — falling back to 3-store defaults")

        if reset and os.path.exists(self.db_path):
            os.remove(self.db_path)
            logger.info(f"Removed existing database: {self.db_path}")

        self.conn = sqlite3.connect(self.db_path)
        self.conn.execute("PRAGMA journal_mode=WAL")
        self.conn.execute("PRAGMA foreign_keys=ON")

        try:
            self._create_schema()
            self._seed_organizations()
            self._seed_suppliers()
            self._seed_system_preferences()
            self._seed_tax_plans()
            self._seed_counters()
            self._seed_customers()
            self._seed_products()
            self._seed_prices()
            self._seed_stock()
            self._seed_sales_transactions()
            self._seed_bi_report()
            self.conn.commit()
            mode_tag = f"FAST ({FAST_MODE_SKU_LIMIT} SKUs)" if self.fast_mode else "FULL"
            logger.info(f"Mock POS/ERP database built [{mode_tag}]: {self.db_path}")
        finally:
            self.conn.close()

        return self.db_path

    def _create_schema(self):
        """Create all tables."""
        self.conn.executescript(SCHEMA_SQL)
        logger.info("Schema created (15 tables)")

    # ------------------------------------------------------------------
    # Seed: Organizations (3 store tiers)
    # ------------------------------------------------------------------
    def _seed_organizations(self):
        """Seed all 14 stores from stores_network.json as ORG entries."""
        stores = self._network.get('stores', [])

        # Map network store_ids to short ORG codes
        orgs = []
        self.store_meta = []
        for i, s in enumerate(stores):
            org_cd   = f"ORG{i+1:03d}"   # ORG001 ... ORG014
            short    = s['store_id'].replace('-', '')  # CFP003 etc
            address  = f"{s.get('region','Nairobi')}, Nairobi"
            phone    = f"+254-20-{1000000 + i*111111}"
            email    = f"{short.lower()}@chandarana.co.ke"

            orgs.append((
                org_cd, s['name'], short, address, 'Nairobi', 'Nairobi',
                'KE', '00100', phone, email, 'KES', None, 1, None
            ))
            self.store_meta.append({
                'org_cd':  org_cd,
                'store_id': s['store_id'],
                'name':    s['name'],
                'dsf':     s.get('demand_scale_factor', 1.0),
                'stock_profile': s.get('stock_profile', []),
            })

        # Fallback if JSON not loaded
        if not orgs:
            orgs = [
                ('ORG001','Chandarana Foodplus - Rhapta Road','RHAPTA',
                 'Rhapta Road, Westlands','Nairobi','Nairobi','KE','00100',
                 '+254-20-1234567','rhapta@chandarana.co.ke','KES',None,1,None),
                ('ORG002','Chandarana Foodplus - Lavington','LAVINGT',
                 'James Gichuru Road','Nairobi','Nairobi','KE','00100',
                 '+254-20-2345678','lavington@chandarana.co.ke','KES',None,1,None),
                ('ORG003','Chandarana Foodplus - Karen','KAREN',
                 'Karen Road, Hardy','Nairobi','Nairobi','KE','00200',
                 '+254-20-3456789','karen@chandarana.co.ke','KES',None,1,None),
            ]
            self.store_meta = [
                {'org_cd':'ORG001','store_id':'CFP-007','name':'Rhapta Road','dsf':1.0,'stock_profile':[]},
                {'org_cd':'ORG002','store_id':'CFP-005','name':'Lavington Mall','dsf':1.4,'stock_profile':[]},
                {'org_cd':'ORG003','store_id':'CFP-002','name':'Karen Well','dsf':0.6,'stock_profile':[]},
            ]

        self.conn.executemany(
            """INSERT OR IGNORE INTO ORGANIZATION_MST
               (ORG_CD, ORG_NAME, ORG_SHORT_NAME, ORG_ADDRESS, ORG_CITY,
                ORG_STATE, ORG_COUNTRY, ORG_PIN, ORG_PHONE, ORG_EMAIL,
                CURRENCY_CD, GST_NO, LEVEL_NUMBER, PARENT_ORG_CD)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            orgs
        )
        self.org_codes = [o[0] for o in orgs]
        logger.info(f"  Organizations: {len(orgs)}")

    # ------------------------------------------------------------------
    # Seed: Suppliers (from supplier_patterns JSON)
    # ------------------------------------------------------------------
    def _seed_suppliers(self):
        patterns = _load_json("supplier_patterns_2025")
        if not patterns:
            # Fallback minimal suppliers
            patterns = {
                "BROOKSIDE DAIRY LIMITED": {"order_frequency": "daily", "estimated_delivery_days": 1, "reliability_score": 0.905},
                "COCA COLA BEVERAGES KENYA LTD": {"order_frequency": "every_2_3_days", "estimated_delivery_days": 2, "reliability_score": 0.905},
                "UNILEVER KENYA LTD": {"order_frequency": "weekly", "estimated_delivery_days": 7, "reliability_score": 0.9},
                "KENCHIC LIMITED": {"order_frequency": "daily", "estimated_delivery_days": 1, "reliability_score": 0.95},
                "PROCTER AND GAMBLE": {"order_frequency": "bi_weekly", "estimated_delivery_days": 10, "reliability_score": 0.85},
            }

        rows = []
        for idx, (name, data) in enumerate(patterns.items()):
            supp_cd = f"SUP{idx+1:04d}"
            freq = data.get("order_frequency", "weekly")
            lead = data.get("estimated_delivery_days", 7)
            rel = data.get("reliability_score", 0.9)
            rows.append((supp_cd, name, None, None, None, None, "NET30", freq, lead, rel))
            self.supplier_catalog[name.upper()] = {"cd": supp_cd, **data}

        self.conn.executemany(
            """INSERT OR IGNORE INTO SUPPLIER_MST 
               (SUPPLIER_CD, SUPPLIER_NAME, CONTACT_PERSON, PHONE, EMAIL, 
                ADDRESS, PAYMENT_TERMS, ORDER_FREQUENCY, LEAD_TIME_DAYS, RELIABILITY_SCORE)
               VALUES (?,?,?,?,?,?,?,?,?,?)""",
            rows
        )
        logger.info(f"  Suppliers: {len(rows)}")

    # ------------------------------------------------------------------
    # Seed: Products (from sales_profitability intelligence)
    # ------------------------------------------------------------------
    def _seed_products(self):
        """Load top-N SKUs by ADS from the reference store's stock_profile.

        If stores_network.json is available we use its stock_profile directly
        (barcode, department, supplier, price, cost, ads_scaled).  We sort by
        ads_scaled descending so the most important items are always included.
        """
        # ── Source: stores_network reference store (highest dsf = most complete)
        ref_profile: List[dict] = []
        if self.store_meta:
            # Use the store with dsf >= 1.0 (reference = Rhapta or Yaya)
            ref = next((m for m in self.store_meta if m['dsf'] >= 1.0), self.store_meta[0])
            ref_profile = ref['stock_profile']

        limit = FAST_MODE_SKU_LIMIT if self.fast_mode else len(ref_profile)

        if ref_profile:
            # Sort by ADS desc, take top `limit`
            ref_profile_sorted = sorted(
                ref_profile, key=lambda x: x.get('ads_scaled', 0), reverse=True
            )[:limit]
        else:
            # Fallback to intelligence JSON
            intel = _load_json("sales_profitability_intelligence_2025")
            ref_profile_sorted = [
                {
                    'sku': name,
                    'barcode': _generate_barcode(i),
                    'department': data.get('category', 'GROCERY'),
                    'supplier': 'DEFAULT SUPPLIER',
                    'qty': 20,
                    'price': data.get('revenue', 1) / max(1, data.get('total_qty_sold', 1)),
                    'cost': data.get('revenue', 1) / max(1, data.get('total_qty_sold', 1)) * 0.75,
                    'ads_scaled': data.get('total_qty_sold', 0) / 300,
                    'velocity_tier': 'B (Core)',
                }
                for i, (name, data) in enumerate(intel.items())
            ][:limit]

        rows = []
        for idx, sku_entry in enumerate(ref_profile_sorted):
            itm_cd  = _generate_item_code(idx + 1)
            name    = sku_entry.get('sku', f'Item {idx+1}')
            barcode = sku_entry.get('barcode', _generate_barcode(idx + 1))
            dept    = sku_entry.get('department', 'GROCERY').strip().upper()
            supp_name = sku_entry.get('supplier', 'UNKNOWN SUPPLIER').strip().upper()
            sell_price = float(sku_entry.get('price', 100.0))
            cost_price = float(sku_entry.get('cost', sell_price * 0.75))
            ads        = float(sku_entry.get('ads_scaled', 0.1))
            velocity   = sku_entry.get('velocity_tier', 'C (Filler)')

            # Department -> type flags
            is_fresh = any(k in dept for k in [
                'MILK', 'DAIRY', 'FRESH', 'MEAT', 'CHICKEN', 'FISH', 'BREAD',
                'BAKERY', 'FLOWERS', 'VEGETABLE', 'FRUIT', 'BUTCH'
            ])
            uom = 'KG' if 'KG' in name.upper() else 'EA'

            # Category
            category = dept

            # Supplier lookup / assign
            supp_cd = self.supplier_catalog.get(supp_name, {}).get('cd')
            if not supp_cd and self.supplier_catalog:
                supp_cd = list(self.supplier_catalog.values())[idx % len(self.supplier_catalog)]['cd']

            margin_pct = round((sell_price - cost_price) / sell_price * 100, 1) if sell_price else 20.0

            rows.append((
                itm_cd, name, name[:30], barcode, None, uom,
                'Each' if uom == 'EA' else 'Kilogram',
                category, 'F', None,
                'Y' if uom == 'KG' else 'N', 'N', 'N',
                category, dept, supp_cd
            ))

            self.product_catalog[itm_cd] = {
                'name':        name,
                'barcode':     barcode,
                'category':    category,
                'department':  dept,
                'sell_price':  sell_price,
                'cost_price':  cost_price,
                'margin_pct':  margin_pct,
                'ads':         ads,
                'is_fresh':    is_fresh,
                'supplier_cd': supp_cd,
                'supplier_name': supp_name,
                'velocity_tier': velocity,
                # Store the original ref qty for per-store scaling later
                '_ref_qty':    max(0.0, float(sku_entry.get('qty', 0))),
            }

        self.conn.executemany(
            """INSERT OR IGNORE INTO ITEM_MST
               (ITM_CD, ITM_LONG_NAME, ITM_SHORT_NAME, SCAN_ITM_CD, HSN_CD,
                UOM_CD, UOM_DESC, ITM_GROUP_CD, ITM_TYPE, TAX_PLAN_CD,
                WEIGHT_FLAG, SERIAL_FLAG, PRODUCTION_FLAG, CATEGORY, DEPARTMENT, SUPPLIER_CD)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            rows
        )
        mode_tag = f"FAST-{FAST_MODE_SKU_LIMIT}" if self.fast_mode else 'FULL'
        logger.info(f"  Products: {len(rows)} [{mode_tag}]")

    # ------------------------------------------------------------------
    # Seed: Prices
    # ------------------------------------------------------------------
    def _seed_prices(self):
        sp_rows = []
        cp_rows = []
        today = datetime.now().strftime("%Y-%m-%d")

        for org_cd in self.org_codes:
            for itm_cd, info in self.product_catalog.items():
                sell_price = info['sell_price']
                cost_price = info['cost_price']

                # Slight price variation across stores (+/- 2%)
                sv = 1.0 + random.uniform(-0.02, 0.02)
                sp = round(sell_price * sv, 2)
                cp = round(cost_price * sv, 2)

                sp_rows.append((org_cd, itm_cd, sp, sp, today))
                cp_rows.append((org_cd, itm_cd, cp, today))

        self.conn.executemany(
            "INSERT OR IGNORE INTO BASIC_SP_MST (BSP_ORG_CD, BSP_ITEM_CD, BSP_SP, BSP_MRP, BSP_EFF_DATE) VALUES (?,?,?,?,?)",
            sp_rows
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO BASIC_CP_MST (BCP_ORG_CD, BCP_ITEM_CD, BCP_CP, BCP_EFF_DATE) VALUES (?,?,?,?)",
            cp_rows
        )
        logger.info(f"  Selling prices: {len(sp_rows)}, Cost prices: {len(cp_rows)}")

    # ------------------------------------------------------------------
    # Seed: Stock levels
    # ------------------------------------------------------------------
    def _seed_stock(self):
        """Seed opening stock for each store using that store's own demand_scale_factor.

        We use the reference SKU qty (from the network reference store) and scale it
        by each store's dsf.  Fresh items are capped at 3-day cover.
        """
        rows = []
        today = datetime.now()

        # Build org -> dsf map
        dsf_map = {m['org_cd']: m['dsf'] for m in self.store_meta}

        for org_cd in self.org_codes:
            dsf = dsf_map.get(org_cd, 1.0)

            for itm_cd, info in self.product_catalog.items():
                ads       = info['ads']
                is_fresh  = info['is_fresh']
                ref_qty   = info.get('_ref_qty', 0.0)
                wac       = info['cost_price']

                if ref_qty > 0:
                    # Scale the real reference qty by this store's demand factor
                    base_qty = ref_qty * dsf
                    # Add +-15% noise for realism
                    noise = random.uniform(0.85, 1.15)
                    stock_qty = round(base_qty * noise, 1)
                else:
                    # Zero stock in reference: simulate with ADS-based estimate
                    if is_fresh:
                        stock_qty = round(ads * dsf * random.uniform(0, 2), 1)
                    else:
                        stock_qty = 0.0 if random.random() < 0.12 else round(ads * dsf * random.uniform(2, 14), 1)

                # Cap fresh at max 3-day cover
                if is_fresh and ads > 0:
                    max_fresh = ads * dsf * 3
                    stock_qty = min(stock_qty, max_fresh)

                stock_qty = max(0.0, stock_qty)

                days_ago_recv  = random.randint(0, 3)  if is_fresh else random.randint(0, 30)
                days_ago_issue = random.randint(0, 1)
                last_recv  = (today - timedelta(days=days_ago_recv)).strftime("%Y-%m-%d")
                last_issue = (today - timedelta(days=days_ago_issue)).strftime("%Y-%m-%d")

                rows.append((org_cd, itm_cd, 'MAIN', stock_qty, round(wac, 2), last_recv, last_issue))

        self.conn.executemany(
            """INSERT OR IGNORE INTO STOCK_MASTER
               (SM_ORG_CD, SM_ITM_CD, SM_LOC_CD, SM_QTY, SM_WAC, SM_LAST_RECV_DT, SM_LAST_ISSUE_DT)
               VALUES (?,?,?,?,?,?,?)""",
            rows
        )
        logger.info(f"  Stock records: {len(rows)} ({len(self.org_codes)} stores x {len(self.product_catalog)} SKUs)")

    # ------------------------------------------------------------------
    # Seed: Sales Transactions (90 days of POS data)
    # ------------------------------------------------------------------
    def _seed_sales_transactions(self):
        """Generate 90 days of POS transactions for all 14 stores.

        Transaction volume and basket sizes are scaled by each store's
        demand_scale_factor relative to the reference store (dsf=1.0 ~ 120 bills/day).
        """
        today = datetime.now()
        hdr_rows = []
        dtl_rows = []
        bill_counter = 0
        items_list = list(self.product_catalog.items())

        # Build org -> dsf map
        dsf_map = {m['org_cd']: m['dsf'] for m in self.store_meta}

        for day_offset in range(90, 0, -1):
            bill_date = (today - timedelta(days=day_offset))
            bill_dt_str = bill_date.strftime("%Y-%m-%d")
            day_of_week = bill_date.weekday()

            # Weekend multiplier
            weekend_mult = 1.3 if day_of_week >= 4 else 1.0  # Fri-Sun

            for org_cd in self.org_codes:
                dsf     = dsf_map.get(org_cd, 1.0)
                # Reference store (dsf=1.0) ~ 120 bills/day; scale linearly
                n_bills = int(random.gauss(120 * dsf, 20 * dsf) * weekend_mult)
                n_bills = max(20, min(int(500 * dsf), n_bills))

                # Create bills
                for b in range(n_bills):
                    bill_counter += 1
                    bill_no = f"B{bill_counter:08d}"

                    # Basket: 2-10 items, A-tier items weighted
                    basket_size = max(2, int(random.gauss(5, 2)))
                    basket_items = random.choices(
                        [itm for itm, _ in items_list],
                        # Weight by ADS so staples appear more often
                        weights=[max(0.01, info['ads']) for _, info in items_list],
                        k=min(basket_size, len(items_list))
                    )
                    # Deduplicate within bill
                    seen = set()
                    basket_items = [x for x in basket_items if not (x in seen or seen.add(x))]

                    total_qty = total_amt = total_tax = 0
                    serial_no = 0

                    for itm_cd in basket_items:
                        serial_no += 1
                        info = self.product_catalog[itm_cd]

                        # Qty: ADS-based with variance
                        base_qty = max(1, info['ads'] * dsf * random.uniform(0.2, 3.0) * weekend_mult / max(1, n_bills) * 100)
                        qty = round(base_qty, 1 if info.get('is_fresh') else 0)
                        qty = max(1, qty)

                        sell_price = info['sell_price']
                        line_amt   = round(qty * sell_price, 2)
                        line_tax   = round(line_amt * 0.16, 2)
                        net_amt    = round(line_amt - line_tax, 2) if random.random() > 0.5 else line_amt

                        dtl_rows.append((
                            org_cd, bill_no, bill_dt_str, serial_no,
                            itm_cd, info['name'], qty, sell_price,
                            net_amt, line_tax, 0, line_tax, line_amt,
                            'EA', 'Each', 'F', 'N', info['barcode'], None
                        ))
                        total_qty += qty
                        total_amt += line_amt
                        total_tax += line_tax

                    cust_cd    = f"CUST{random.randint(1,50):04d}" if random.random() > 0.6 else None
                    counter_cd = f"CNT{random.randint(1,4):03d}"
                    payment    = random.choice(['CASH', 'MPESA', 'CARD', 'CASH'])

                    hdr_rows.append((
                        org_cd, bill_no, bill_dt_str, cust_cd, counter_cd,
                        1, round(total_qty, 1), round(total_amt, 2),
                        round(total_amt - total_tax, 2), round(total_tax, 2),
                        0, payment, 'F', None, None
                    ))

                    # Batch insert every 5000 rows to avoid memory issues
                    if len(hdr_rows) >= 5000:
                        self.conn.executemany(
                            """INSERT INTO POS_SALES_HDR 
                               (ORG_CD, BILL_NO, BILL_DT, CUST_CD, COUNTER_CD,
                                LEVEL_NUMBER, TOTAL_QTY, TOTAL_AMT, NET_AMT, TAX_AMT,
                                DISC_AMT, PAYMENT_MODE, VOID_FLAG, CUS_REF_CODE, CUS_REF_REMARKS)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            hdr_rows
                        )
                        self.conn.executemany(
                            """INSERT INTO POS_SALES_DTL 
                               (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO,
                                ITM_CD, ITEM_NAME, QTY, SELL_PRICE,
                                NET_AMT, TAX_AMT, DISC_AMT, NET_TAX_AMT, TOTAL_VALUE,
                                UOM_CD, UOM_DESC, VOID_FLAG, PROMO_ITEM_FLAG, SCAN_ITM_CD, TAX_PLAN_CD)
                               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                            dtl_rows
                        )
                        hdr_rows.clear()
                        dtl_rows.clear()

        # Final batch insert
        if hdr_rows:
            self.conn.executemany(
                """INSERT INTO POS_SALES_HDR 
                   (ORG_CD, BILL_NO, BILL_DT, CUST_CD, COUNTER_CD,
                    LEVEL_NUMBER, TOTAL_QTY, TOTAL_AMT, NET_AMT, TAX_AMT,
                    DISC_AMT, PAYMENT_MODE, VOID_FLAG, CUS_REF_CODE, CUS_REF_REMARKS)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                hdr_rows
            )
        if dtl_rows:
            self.conn.executemany(
                """INSERT INTO POS_SALES_DTL 
                   (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO,
                    ITM_CD, ITEM_NAME, QTY, SELL_PRICE,
                    NET_AMT, TAX_AMT, DISC_AMT, NET_TAX_AMT, TOTAL_VALUE,
                    UOM_CD, UOM_DESC, VOID_FLAG, PROMO_ITEM_FLAG, SCAN_ITM_CD, TAX_PLAN_CD)
                   VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
                dtl_rows
            )

        total_bills = bill_counter
        logger.info(f"  Sales transactions: {total_bills} bills (90 days × {len(self.org_codes)} stores)")

    # ------------------------------------------------------------------
    # Seed: BI Sales Report (monthly aggregation)
    # ------------------------------------------------------------------
    def _seed_bi_report(self):
        """Aggregate sales into monthly BI report table."""
        self.conn.execute("""
            INSERT OR REPLACE INTO BI_SALES_REPORT (ORG_CD, ITM_CD, REPORT_MONTH, QUANTITY, TAX_INCL, TAX_EXCL, TOTAL_TAX, COST, TRANSACTION_COUNT)
            SELECT 
                d.ORG_CD,
                d.ITM_CD,
                SUBSTR(d.BILL_DT, 1, 7) AS REPORT_MONTH,
                SUM(d.QTY) AS QUANTITY,
                SUM(d.TOTAL_VALUE) AS TAX_INCL,
                SUM(d.NET_AMT) AS TAX_EXCL,
                SUM(d.TAX_AMT) AS TOTAL_TAX,
                0 AS COST,
                COUNT(DISTINCT d.BILL_NO) AS TRANSACTION_COUNT
            FROM POS_SALES_DTL d
            WHERE d.VOID_FLAG = 'F'
            GROUP BY d.ORG_CD, d.ITM_CD, SUBSTR(d.BILL_DT, 1, 7)
        """)
        count = self.conn.execute("SELECT COUNT(*) FROM BI_SALES_REPORT").fetchone()[0]
        logger.info(f"  BI Report: {count} monthly aggregations")

    # ------------------------------------------------------------------
    # Seed: Customers
    # ------------------------------------------------------------------
    def _seed_customers(self):
        rows = []
        first_names = ["John", "Jane", "Peter", "Mary", "James", "Sarah", "David", "Grace",
                       "Michael", "Alice", "Robert", "Faith", "Joseph", "Mercy", "Daniel"]
        last_names = ["Kamau", "Wanjiru", "Ochieng", "Akinyi", "Mwangi", "Njeri", "Otieno",
                      "Wambui", "Kipchoge", "Chebet", "Mutua", "Muthoni", "Omondi", "Nyawira"]

        for i in range(1, 51):
            cust_cd = f"CUST{i:04d}"
            first = random.choice(first_names)
            last = random.choice(last_names)
            mobile = f"+2547{random.randint(10000000, 99999999)}"
            loyalty = f"LC{random.randint(100000, 999999)}" if random.random() > 0.3 else None

            rows.append((cust_cd, f"{first} {last}", "Mr" if i % 2 else "Ms", mobile, None, loyalty))

        self.conn.executemany(
            """INSERT OR IGNORE INTO CUSTOMER_MST 
               (CUST_CD, CUST_NAME, CUST_SALUTATION, MOBILE_NO, EMAIL, LOYALTY_CARD_NO)
               VALUES (?,?,?,?,?,?)""",
            rows
        )
        logger.info(f"  Customers: {len(rows)}")

    # ------------------------------------------------------------------
    # Seed: Counters / Tills
    # ------------------------------------------------------------------
    def _seed_counters(self):
        rows = []
        for org_cd in self.org_codes:
            for c in range(1, 6):   # 5 tills per store
                cnt_cd = f"{org_cd}_CNT{c:03d}"
                rows.append((cnt_cd, f"Counter {c}", org_cd, 'Y', 'Y'))
        self.conn.executemany(
            "INSERT OR IGNORE INTO COUNTER_MST (COUNTER_CD, COUNTER_NAME, ORG_CD, ONLINE_FLAG, BILL_REFUND) VALUES (?,?,?,?,?)",
            rows
        )
        logger.info(f"  Counters: {len(rows)}")

    # ------------------------------------------------------------------
    # Seed: System Preferences
    # ------------------------------------------------------------------
    def _seed_system_preferences(self):
        prefs = [
            ("REQD_GST_MODULE", "1", "SYSTEM"),
            ("REQD_QR_PRINT", "0", "POS"),
            ("ALLOW_EINVOICING_IN_POS", "0", "POS"),
            ("REQD_POS_BILL_PRINT_FILE_NAME", "rptPosBillGST_40.rpt", "POS"),
            ("REQD_CENTERALIZED_POS_PREF", "1", "POS"),
            ("MRP_REQD", "0", "POS"),
            ("REQD_POS_INV_AMT_DISPLAY_IN_LAKH", "0", "SYSTEM"),
            ("PP_PRINT_LOY_CARD_DETAILS_FLAG", "1", "POS"),
            ("PP_REMOVE_LEADING_ZERO", "0", "POS"),
            ("PRINT_SHORT_NAME", "0", "POS"),
            ("REQD_LOYALTY_FLAG", "1", "POS"),
            ("OASIS_INTEGRATION_ENABLED", "1", "SYSTEM"),
            ("OASIS_SYNC_INTERVAL_MINUTES", "30", "SYSTEM"),
            ("DEFAULT_CURRENCY", "KES", "SYSTEM"),
            ("TAX_RATE_DEFAULT", "16", "SYSTEM"),
        ]
        self.conn.executemany(
            "INSERT OR IGNORE INTO BASE_SYSTEM_PREFERENCES (BSP_PREF_DESC, BSP_PREF_VALUE, BSP_MODULE) VALUES (?,?,?)",
            prefs
        )
        logger.info(f"  System preferences: {len(prefs)}")

    # ------------------------------------------------------------------
    # Seed: Tax Plans
    # ------------------------------------------------------------------
    def _seed_tax_plans(self):
        plans = [
            ("TP001", "VAT 16%", "VAT"),
            ("TP002", "VAT Exempt", "VAT"),
            ("TP003", "Zero Rated", "VAT"),
        ]
        taxes = [
            ("TX001", "VAT 16%", 16.0, "V", "TP001"),
            ("TX002", "VAT Exempt", 0.0, "V", "TP002"),
            ("TX003", "Zero Rated", 0.0, "V", "TP003"),
        ]
        self.conn.executemany(
            "INSERT OR IGNORE INTO TAX_PLAN_HDR (TAX_PLAN_CD, TAX_PLAN_NAME, TAX_PLAN_TYPE) VALUES (?,?,?)",
            plans
        )
        self.conn.executemany(
            "INSERT OR IGNORE INTO TAX_MST (TAX_CD, TAX_NAME, TAX_PERC, TAX_TYPE, TAX_PLAN_CD) VALUES (?,?,?,?,?)",
            taxes
        )
        logger.info(f"  Tax plans: {len(plans)}, Tax codes: {len(taxes)}")


# ---------------------------------------------------------------------------
# Utility: Quick summary of mock database
# ---------------------------------------------------------------------------

def summarize_mock_db(db_path: str = DEFAULT_DB_PATH) -> dict:
    """Returns row counts for all tables in the mock database."""
    conn = sqlite3.connect(db_path)
    tables = conn.execute(
        "SELECT name FROM sqlite_master WHERE type='table' ORDER BY name"
    ).fetchall()

    summary = {}
    for (table_name,) in tables:
        count = conn.execute(f"SELECT COUNT(*) FROM [{table_name}]").fetchone()[0]
        summary[table_name] = count

    conn.close()
    return summary


# ---------------------------------------------------------------------------
# CLI Entry Point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    import sys
    logging.basicConfig(level=logging.INFO, format="%(message)s")

    reset     = '--reset' in sys.argv
    fast_mode = '--fast'  in sys.argv

    builder  = MockPosErpBuilder(fast_mode=fast_mode)
    db_path  = builder.build(reset=reset)

    print("\n--- Mock Database Summary ---")
    for table, count in summarize_mock_db(db_path).items():
        print(f"  {table:40s} {count:>10,} rows")

    print(f"\nDatabase : {db_path}")
    print(f"SKU mode : {'FAST (' + str(FAST_MODE_SKU_LIMIT) + ')' if fast_mode else 'FULL'}")
    print(f"Stores   : {len(builder.org_codes)}")
