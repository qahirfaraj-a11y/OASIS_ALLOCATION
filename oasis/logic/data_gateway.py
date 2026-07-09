"""
O.A.S.I.S. DataGateway — Unified Ingestion Router
====================================================
Abstracts the data source behind a single interface.
Routes to SQL (Pathway 1), File (Pathway 2), or Hybrid mode.
Configuration read from oasis_client_config.json.
"""

import os
import json
import logging
import pandas as pd
from datetime import datetime
from typing import Optional, Dict, List, Any

logger = logging.getLogger("OASIS.DataGateway")

_CONFIG_CACHE: Optional[Dict[str, Any]] = None
_CONFIG_MTIME: float = 0.0


def load_client_config(config_path: Optional[str] = None) -> Dict[str, Any]:
    """Load and cache the client configuration with hot-reload."""
    global _CONFIG_CACHE, _CONFIG_MTIME
    if config_path is None:
        config_path = os.environ.get("OASIS_CLIENT_CONFIG", _find_config_file())
    if not config_path or not os.path.exists(config_path):
        logger.warning(f"Client config not found at {config_path}. Using defaults.")
        return _default_config()
    current_mtime = os.path.getmtime(config_path)
    if _CONFIG_CACHE is not None and current_mtime == _CONFIG_MTIME:
        return _CONFIG_CACHE
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)
    _CONFIG_CACHE = config
    _CONFIG_MTIME = current_mtime
    logger.info(f"Loaded client config: client={config.get('client',{}).get('client_id','?')}")
    return config


def _find_config_file() -> Optional[str]:
    """Search standard locations for oasis_client_config.json."""
    candidates = [
        os.path.join(os.getcwd(), "oasis_client_config.json"),
        os.path.join(os.path.dirname(__file__), "..", "..", "oasis_client_config.json"),
        "/app/oasis_client_config.json",
        "/data/oasis_client_config.json",
    ]
    for path in candidates:
        resolved = os.path.abspath(path)
        if os.path.exists(resolved):
            return resolved
    return None


def _default_config() -> Dict[str, Any]:
    return {
        "client": {"client_id": "dev_local", "client_name": "Development"},
        "data_pathway": "file", "ingestion_cycle": "24_HOUR",
        "sql_connection": {"enabled": False},
        "file_dump": {"watch_dir": os.path.join(os.getcwd(), "inbound_drops"),
                      "required_columns": ["Item_Name", "SOH", "ADS", "Unit_Cost"]},
        "stores": [],
        "engines": {"amit": {"enabled": True}, "lata": {"enabled": True},
                     "dharam": {"enabled": True}, "shadow_mode": True},
        "paths": {"data_dir": os.path.join(os.getcwd(), "oasis", "data"),
                  "db_path": os.path.join(os.getcwd(), "oasis.db")},
    }


class DataGateway:
    """
    Unified data access layer for O.A.S.I.S.

    Usage::
        gw = DataGateway()
        gw.bootstrap_retail_universe()   # Day-0
        stock = gw.get_stock_snapshot("ORG001")
        sales = gw.get_sales_history("ORG001", days=90)
    """

    def __init__(self, config_path: Optional[str] = None, tenant_id: str = 'default_tenant'):
        self.tenant_id = tenant_id
        self.config = load_client_config(config_path)
        self.pathway = self.config.get("data_pathway", "file")
        self._sql_bridge = None
        paths = self.config.get("paths", {})
        self.data_dir = paths.get("data_dir", os.path.join(os.getcwd(), "oasis", "data"))
        self.db_path = paths.get("db_path", os.path.join(os.getcwd(), "oasis.db"))
        logger.info(f"DataGateway: pathway={self.pathway}, stores={len(self.config.get('stores', []))}")

    def _get_sql_bridge(self):
        if self._sql_bridge is not None:
            return self._sql_bridge
        sql_cfg = self.config.get("sql_connection", {})
        if not sql_cfg.get("enabled", False):
            raise RuntimeError("SQL pathway requested but sql_connection.enabled is false.")
        from .iretail_integration import IRetailBridge
        bridge = IRetailBridge(
            server=sql_cfg["server"], database=sql_cfg["database"],
            username=sql_cfg.get("username"), password=sql_cfg.get("password"),
            trusted_connection=sql_cfg.get("trusted_connection", True),
            data_dir=self.data_dir,
        )
        if not bridge.connect():
            raise ConnectionError(f"SQL connect failed: {sql_cfg['server']}/{sql_cfg['database']}")
        self._sql_bridge = bridge
        return self._sql_bridge

    # ── Stock Snapshot ────────────────────────────────────────────────

    def get_stock_snapshot(self, org_cd: str) -> List[Dict[str, Any]]:
        if self.pathway in ("sql", "hybrid"):
            return self._sql_stock(org_cd)
        return self._file_stock(org_cd)

    def _sql_stock(self, org_cd: str) -> List[Dict[str, Any]]:
        bridge = self._get_sql_bridge()
        store_cfg = self._find_store(org_cd)
        sid = store_cfg.get("sql_store_id") if store_cfg else None
        return bridge.sync_stock_snapshot(store_id=sid)

    def _file_stock(self, org_cd: str) -> List[Dict[str, Any]]:
        scorecard_path = self._resolve_scorecard_path()
        if not scorecard_path or not os.path.exists(scorecard_path):
            return []
        df = pd.read_csv(scorecard_path)
        # Support BOTH standard schema (Item_Name/SOH/ADS/Unit_Cost)
        # AND real production schema (Product/Current_Stock/Avg_Daily_Sales/Unit_Price)
        col_map = {
            # Standard → internal
            "Item_Name": "product_name", "SOH": "current_stocks",
            "Unit_Cost": "cost_price", "ADS": "avg_daily_sales",
            # Real production scorecard → internal
            "Product": "product_name", "Current_Stock": "current_stocks",
            "Unit_Price": "cost_price", "Avg_Daily_Sales": "avg_daily_sales",
            "Lead_Time_Days": "lead_time",
            # Common across both
            "Department": "department", "Supplier": "supplier_name",
            "Pack_Size": "pack_size", "Selling_Price": "selling_price", "Barcode": "barcode",
            "Margin_Pct": "margin_pct",
        }
        df = df.rename(columns={k: v for k, v in col_map.items() if k in df.columns})
        # Tenant isolation mock
        if 'TENANT_ID' in df.columns:
            df = df[df['TENANT_ID'] == self.tenant_id]
        products = []
        for _, row in df.iterrows():
            products.append({
                "product_name": str(row.get("product_name", "UNKNOWN")).strip().upper(),
                "current_stocks": int(row.get("current_stocks", 0)),
                "barcode": str(row.get("barcode", "")),
                "department": str(row.get("department", "GENERAL")).upper(),
                "pack_size": int(row.get("pack_size", 1)) or 1,
                "selling_price": float(row.get("selling_price", 0)),
                "cost_price": float(row.get("cost_price", 0)),
                "supplier_name": str(row.get("supplier_name", "Unknown")).strip(),
                "avg_daily_sales": float(row.get("avg_daily_sales", 0)),
            })
        logger.info(f"File stock: {len(products)} products from {scorecard_path}")
        return products

    # ── Sales & GRN ───────────────────────────────────────────────────

    def get_sales_history(self, org_cd: str, days: int = 90) -> pd.DataFrame:
        if self.pathway in ("sql", "hybrid"):
            bridge = self._get_sql_bridge()
            store_cfg = self._find_store(org_cd)
            sid = store_cfg.get("sql_store_id") if store_cfg else None
            return bridge.sync_sales_history(days=days, store_id=sid)
        sales_path = os.path.join(self.data_dir, "historical_sales.csv")
        if os.path.exists(sales_path):
            return pd.read_csv(sales_path)
        scorecard_path = self._resolve_scorecard_path()
        if scorecard_path and os.path.exists(scorecard_path):
            df = pd.read_csv(scorecard_path)
            if "ADS" in df.columns and "Item_Name" in df.columns:
                return pd.DataFrame({
                    "product_name": df["Item_Name"],
                    "avg_daily_sales": pd.to_numeric(df["ADS"], errors="coerce").fillna(0),
                    "total_qty_sold": (pd.to_numeric(df["ADS"], errors="coerce").fillna(0) * days).astype(int),
                })
        return pd.DataFrame()

    def get_grn_history(self, days: int = 180) -> pd.DataFrame:
        if self.pathway in ("sql", "hybrid"):
            return self._get_sql_bridge().sync_grn_history(days=days)
        grn_path = os.path.join(self.data_dir, "historical_grn.csv")
        return pd.read_csv(grn_path) if os.path.exists(grn_path) else pd.DataFrame()

    # ── PO Push ───────────────────────────────────────────────────────

    def push_purchase_order(self, org_cd: str, order_rows: List[Dict[str, Any]]) -> int:
        if self.pathway in ("sql", "hybrid"):
            return self._get_sql_bridge().push_purchase_order(order_rows)
        ts = datetime.now().strftime("%Y%m%d_%H%M%S")
        po_dir = os.path.join(self.data_dir, "approved_pos")
        os.makedirs(po_dir, exist_ok=True)
        po_path = os.path.join(po_dir, f"PO_{org_cd}_{ts}.csv")
        pd.DataFrame(order_rows).to_csv(po_path, index=False, encoding="utf-8")
        logger.info(f"PO saved: {po_path} ({len(order_rows)} lines)")
        return len(order_rows)

    # ── Day-0 Bootstrap ───────────────────────────────────────────────

    def bootstrap_retail_universe(self) -> Dict[str, Any]:
        """Day-0: ingest historical data and warm up all engines."""
        report = {"timestamp": datetime.now().isoformat(),
                  "client_id": self.config.get("client", {}).get("client_id"), "steps": {}}

        # DB init
        try:
            from .db_connector import ensure_oasis_tables
            ensure_oasis_tables(self.db_path)
            report["steps"]["db_init"] = {"status": "OK"}
        except Exception as e:
            report["steps"]["db_init"] = {"status": "FAILED", "error": str(e)}

        # Stock ingestion per store
        total = 0
        for store in self.config.get("stores", []):
            try:
                prods = self.get_stock_snapshot(store["org_cd"])
                total += len(prods)
                report["steps"][f"stock_{store['org_cd']}"] = {"status": "OK", "products": len(prods)}
            except Exception as e:
                report["steps"][f"stock_{store['org_cd']}"] = {"status": "FAILED", "error": str(e)}
        report["steps"]["total_products"] = total

        # Engine warm-up
        engine_cfg = self.config.get("engines", {})
        sc = self._resolve_scorecard_path()
        nn = engine_cfg.get("gnn_model_path") or os.path.join(self.data_dir, "..", "..", "neutral_network_export")

        for name, mod, fn_args in [
            ("amit", "amit_governance", lambda: (self.data_dir,)),
            ("lata", "lata_shield", lambda: (self.data_dir, nn)),
            ("dharam", "dharam_revenue", lambda: (nn, self.data_dir)),
        ]:
            if engine_cfg.get(name, {}).get("enabled", True):
                try:
                    import importlib
                    m = importlib.import_module(f".{mod}", package="oasis.logic")
                    if name == "amit" and sc:
                        gov = m.AMITGovernance(self.data_dir)
                        gov.generate_negative_list(sc)
                    elif name == "lata":
                        m.run_lata(*fn_args())
                    elif name == "dharam":
                        m.run_dharam(*fn_args())
                    report["steps"][f"{name}_warmup"] = {"status": "OK"}
                except Exception as e:
                    report["steps"][f"{name}_warmup"] = {"status": "FAILED", "error": str(e)}

        # Save report
        rp = os.path.join(self.data_dir, "bootstrap_report.json")
        os.makedirs(os.path.dirname(rp), exist_ok=True)
        with open(rp, "w", encoding="utf-8") as f:
            json.dump(report, f, indent=2)
        logger.info(f"Day-0 bootstrap complete. Report: {rp}")
        return report

    # ── Health & Helpers ──────────────────────────────────────────────

    def health_check(self) -> Dict[str, Any]:
        status = {"pathway": self.pathway,
                  "client_id": self.config.get("client", {}).get("client_id"),
                  "stores": len(self.config.get("stores", [])),
                  "db_exists": os.path.isfile(self.db_path),
                  "sql_connected": False}
        if self.pathway in ("sql", "hybrid"):
            try:
                status["sql_connected"] = self._get_sql_bridge().health_check().get("sql_connected", False)
            except Exception:
                pass
        return status

    def _find_store(self, org_cd: str) -> Optional[Dict[str, Any]]:
        for s in self.config.get("stores", []):
            if s.get("org_cd") == org_cd:
                return s
        return None

    def _resolve_scorecard_path(self) -> Optional[str]:
        explicit = self.config.get("paths", {}).get("scorecard_path")
        if explicit and os.path.exists(explicit):
            return explicit
        from pathlib import Path
        for d in [Path(self.data_dir), Path(self.data_dir).parent]:
            cands = list(d.glob("Full_Product_Allocation_Scorecard*.csv"))
            if cands:
                try:
                    return str(max(cands, key=lambda p: int(p.stem.split("_v")[-1])))
                except (ValueError, IndexError):
                    return str(cands[0])
        return None

    def get_configured_stores(self) -> List[Dict[str, Any]]:
        return self.config.get("stores", [])

    def close(self):
        if self._sql_bridge:
            self._sql_bridge.close()
            self._sql_bridge = None
