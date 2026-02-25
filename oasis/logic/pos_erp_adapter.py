"""
POS/ERP Adapter
===============
Translation layer between POS/ERP SQL Server tables and OASIS product dicts.
Contains pre-built SQL queries for each data extraction need.

Works identically against:
  - SQLite mock (development)   →  sqlite:///path/to/mock_pos_erp.db
  - SQL Server (production)     →  mssql+pyodbc://user:pass@server/db

Usage:
    from oasis.logic.db_connector import UniversalConnector, SchemaMapper
    from oasis.logic.pos_erp_adapter import PosErpAdapter

    connector = UniversalConnector("sqlite:///mock_pos_erp.db", SchemaMapper.for_pos_erp())
    adapter = PosErpAdapter(connector)
    products = adapter.fetch_product_master("ORG001")
"""

import logging
import statistics
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any

import pandas as pd
from sqlalchemy import text

logger = logging.getLogger("PosErpAdapter")


class PosErpAdapter:
    """
    Adapter translating POS/ERP SQL tables → OASIS engine dicts.
    Each fetch_* method returns data in the exact format OrderEngine expects.
    """

    def __init__(self, connector):
        """
        Args:
            connector: UniversalConnector instance (already connected)
        """
        self.connector = connector
        self.engine = connector.engine

    # ------------------------------------------------------------------
    # 1. Product Master  →  List[dict] for OrderEngine.parse_inventory_file()
    # ------------------------------------------------------------------
    def fetch_product_master(self, org_cd: str) -> List[dict]:
        """
        Fetch product catalog with current stock, prices, and supplier info.
        Returns list of dicts matching OrderEngine's product dict format.
        """
        query = text("""
            SELECT 
                i.ITM_CD,
                i.ITM_LONG_NAME,
                i.ITM_SHORT_NAME,
                i.SCAN_ITM_CD,
                i.CATEGORY,
                i.DEPARTMENT,
                i.UOM_CD,
                i.UOM_DESC,
                i.WEIGHT_FLAG,
                i.SUPPLIER_CD,
                s.SM_QTY,
                s.SM_WAC,
                s.SM_LAST_RECV_DT,
                sp.BSP_SP,
                sp.BSP_MRP,
                cp.BCP_CP,
                sup.SUPPLIER_NAME,
                sup.ORDER_FREQUENCY,
                sup.LEAD_TIME_DAYS,
                sup.RELIABILITY_SCORE
            FROM ITEM_MST i
            LEFT JOIN STOCK_MASTER s 
                ON s.SM_ITM_CD = i.ITM_CD AND s.SM_ORG_CD = :org_cd
            LEFT JOIN BASIC_SP_MST sp 
                ON sp.BSP_ITEM_CD = i.ITM_CD AND sp.BSP_ORG_CD = :org_cd
            LEFT JOIN BASIC_CP_MST cp 
                ON cp.BCP_ITEM_CD = i.ITM_CD AND cp.BCP_ORG_CD = :org_cd
            LEFT JOIN SUPPLIER_MST sup 
                ON sup.SUPPLIER_CD = i.SUPPLIER_CD
            WHERE i.ACTIVE_FLAG = 'Y'
            ORDER BY i.ITM_CD
        """)

        products = []
        with self.engine.connect() as conn:
            result = conn.execute(query, {"org_cd": org_cd})
            for row in result.mappings():
                # Calculate days since last delivery
                days_since_delivery = 0
                if row["SM_LAST_RECV_DT"]:
                    try:
                        last_recv = datetime.strptime(str(row["SM_LAST_RECV_DT"])[:10], "%Y-%m-%d")
                        days_since_delivery = (datetime.now() - last_recv).days
                    except (ValueError, TypeError):
                        days_since_delivery = 0

                # Determine freshness from department
                dept = str(row["DEPARTMENT"] or "").upper()
                is_fresh = dept in ("DAIRY", "FRESH PRODUCE", "BUTCHERY", "BAKERY")

                product = {
                    "item_code": row["ITM_CD"],
                    "product_name": row["ITM_LONG_NAME"],
                    "barcode": row["SCAN_ITM_CD"] or "",
                    "current_stocks": float(row["SM_QTY"] or 0),
                    "selling_price": float(row["BSP_SP"] or 0),
                    "cost_price": float(row["BCP_CP"] or row["SM_WAC"] or 0),
                    "mrp": float(row["BSP_MRP"] or 0),
                    "supplier_name": row["SUPPLIER_NAME"] or "Unknown",
                    "supplier_cd": row["SUPPLIER_CD"] or "",
                    "category": row["CATEGORY"] or "GENERAL",
                    "department": row["DEPARTMENT"] or "GROCERY",
                    "uom": row["UOM_CD"] or "EA",
                    "is_fresh": is_fresh,
                    "last_days_since_last_delivery": days_since_delivery,
                    "blocked_open_for_order": "open",
                    "pack_size": 1,
                    "estimated_delivery_days": int(row["LEAD_TIME_DAYS"] or 7),
                    "supplier_reliability": float(row["RELIABILITY_SCORE"] or 0.9),
                    "supplier_frequency": row["ORDER_FREQUENCY"] or "weekly",
                    # Placeholders enriched later by fetch_sales_intelligence
                    "estimated_daily_sales": 0.0,
                    "units_sold_last_month": 0.0,
                }
                products.append(product)

        logger.info(f"Fetched {len(products)} products for {org_cd}")
        return products

    # ------------------------------------------------------------------
    # 2. Stock Snapshot  →  List[dict] for real-time stock checks
    # ------------------------------------------------------------------
    def fetch_stock_snapshot(self, org_cd: str) -> List[dict]:
        """
        Lightweight real-time stock query.
        Returns: [{item_code, product_name, current_stocks, wac, last_received}, ...]
        """
        query = text("""
            SELECT 
                s.SM_ITM_CD AS item_code,
                i.ITM_LONG_NAME AS product_name,
                s.SM_QTY AS current_stocks,
                s.SM_WAC AS wac,
                s.SM_LAST_RECV_DT AS last_received
            FROM STOCK_MASTER s
            JOIN ITEM_MST i ON i.ITM_CD = s.SM_ITM_CD
            WHERE s.SM_ORG_CD = :org_cd AND i.ACTIVE_FLAG = 'Y'
            ORDER BY s.SM_QTY ASC
        """)

        with self.engine.connect() as conn:
            result = conn.execute(query, {"org_cd": org_cd})
            rows = [dict(r) for r in result.mappings()]

        logger.info(f"Stock snapshot: {len(rows)} items for {org_cd}")
        return rows

    # ------------------------------------------------------------------
    # 3. Sales History  →  pd.DataFrame for analysis
    # ------------------------------------------------------------------
    def fetch_sales_history(self, org_cd: str, days: int = 90) -> pd.DataFrame:
        """
        Fetch raw POS sales detail for the last N days.
        Returns DataFrame with columns: bill_dt, itm_cd, item_name, qty, sell_price, net_amt
        """
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        query = text("""
            SELECT 
                d.BILL_DT AS bill_dt,
                d.ITM_CD AS itm_cd,
                d.ITEM_NAME AS item_name,
                d.QTY AS qty,
                d.SELL_PRICE AS sell_price,
                d.NET_AMT AS net_amt,
                d.TAX_AMT AS tax_amt,
                d.TOTAL_VALUE AS total_value,
                h.PAYMENT_MODE AS payment_mode,
                h.CUST_CD AS cust_cd
            FROM POS_SALES_DTL d
            JOIN POS_SALES_HDR h 
                ON h.ORG_CD = d.ORG_CD AND h.BILL_NO = d.BILL_NO AND h.BILL_DT = d.BILL_DT
            WHERE d.ORG_CD = :org_cd 
                AND d.BILL_DT >= :cutoff
                AND d.VOID_FLAG = 'F'
                AND h.VOID_FLAG = 'F'
            ORDER BY d.BILL_DT DESC, d.BILL_NO, d.SERIAL_NO
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"org_cd": org_cd, "cutoff": cutoff})
        logger.info(f"Sales history: {len(df)} rows for {org_cd} (last {days} days)")
        return df

    # ------------------------------------------------------------------
    # 4. Sales Intelligence  →  dict matching sales_forecasting JSON format
    # ------------------------------------------------------------------
    def fetch_sales_intelligence(self, org_cd: str, days: int = 300) -> dict:
        """
        Aggregate POS sales into the exact format OrderEngine.databases['sales_forecasting'] expects:
        {
            "PRODUCT NAME": {
                "monthly_sales": {"January": X, "February": Y, ...},
                "total_10mo_sales": N,
                "months_active": M,
                "avg_monthly_sales": A,
                "avg_daily_sales": D,
                "trend": "growing" | "stable" | "declining",
                "trend_pct": P
            }
        }
        """
        cutoff = (datetime.now() - timedelta(days=days)).strftime("%Y-%m-%d")

        # Query monthly aggregation from BI_SALES_REPORT if available, else raw POS
        query = text("""
            SELECT 
                i.ITM_LONG_NAME AS product_name,
                d.ITM_CD AS itm_cd,
                SUBSTR(d.BILL_DT, 1, 7) AS year_month,
                SUM(d.QTY) AS total_qty,
                SUM(d.TOTAL_VALUE) AS total_revenue,
                COUNT(DISTINCT d.BILL_NO) AS transaction_count
            FROM POS_SALES_DTL d
            JOIN ITEM_MST i ON i.ITM_CD = d.ITM_CD
            WHERE d.ORG_CD = :org_cd 
                AND d.BILL_DT >= :cutoff
                AND d.VOID_FLAG = 'F'
            GROUP BY d.ITM_CD, i.ITM_LONG_NAME, SUBSTR(d.BILL_DT, 1, 7)
            ORDER BY d.ITM_CD, year_month
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"org_cd": org_cd, "cutoff": cutoff})

        if df.empty:
            logger.warning(f"No sales data found for {org_cd}")
            return {}

        # Month name mapping
        month_names = {
            "01": "January", "02": "February", "03": "March", "04": "April",
            "05": "May", "06": "June", "07": "July", "08": "August",
            "09": "September", "10": "October", "11": "November", "12": "December"
        }

        sales_intel = {}
        for itm_cd, group in df.groupby("itm_cd"):
            product_name = group.iloc[0]["product_name"]

            # Build monthly_sales dict
            monthly_sales = {}
            for _, row in group.iterrows():
                ym = str(row["year_month"])
                month_num = ym.split("-")[1] if "-" in ym else "01"
                month_name = month_names.get(month_num, "January")
                monthly_sales[month_name] = int(row["total_qty"])

            total_qty = sum(monthly_sales.values())
            months_active = len(monthly_sales)
            avg_monthly = round(total_qty / months_active, 1) if months_active > 0 else 0
            avg_daily = round(total_qty / (months_active * 30), 4) if months_active > 0 else 0

            # Calculate trend (compare first half vs second half)
            sorted_months = list(monthly_sales.values())
            trend = "stable"
            trend_pct = 0.0
            if len(sorted_months) >= 2:
                mid = len(sorted_months) // 2
                first_half = sum(sorted_months[:mid]) / max(mid, 1)
                second_half = sum(sorted_months[mid:]) / max(len(sorted_months) - mid, 1)
                if first_half > 0:
                    trend_pct = round(((second_half - first_half) / first_half) * 100, 1)
                    if trend_pct > 15:
                        trend = "growing"
                    elif trend_pct < -15:
                        trend = "declining"

            sales_intel[product_name] = {
                "monthly_sales": monthly_sales,
                "total_10mo_sales": total_qty,
                "months_active": months_active,
                "avg_monthly_sales": avg_monthly,
                "avg_daily_sales": avg_daily,
                "trend": trend,
                "trend_pct": trend_pct,
            }

        logger.info(f"Sales intelligence: {len(sales_intel)} products for {org_cd}")
        return sales_intel

    # ------------------------------------------------------------------
    # 5. Supplier Patterns  →  dict matching supplier_patterns JSON format
    # ------------------------------------------------------------------
    def fetch_supplier_patterns(self, org_cd: str) -> dict:
        """
        Extract supplier delivery patterns from the SUPPLIER_MST table
        and GRN history. Returns format matching supplier_patterns_2025.json.
        """
        query = text("""
            SELECT 
                s.SUPPLIER_NAME,
                s.ORDER_FREQUENCY,
                s.LEAD_TIME_DAYS,
                s.RELIABILITY_SCORE,
                COUNT(DISTINCT g.GRN_NO) AS total_grns,
                AVG(g.TOTAL_AMT) AS avg_grn_value
            FROM SUPPLIER_MST s
            LEFT JOIN GRN_HDR g ON g.SUPPLIER_CD = s.SUPPLIER_CD AND g.ORG_CD = :org_cd
            WHERE s.ACTIVE_FLAG = 'Y'
            GROUP BY s.SUPPLIER_CD, s.SUPPLIER_NAME, s.ORDER_FREQUENCY, 
                     s.LEAD_TIME_DAYS, s.RELIABILITY_SCORE
            ORDER BY s.SUPPLIER_NAME
        """)

        patterns = {}
        with self.engine.connect() as conn:
            result = conn.execute(query, {"org_cd": org_cd})
            for row in result.mappings():
                freq = row["ORDER_FREQUENCY"] or "weekly"
                lead = int(row["LEAD_TIME_DAYS"] or 7)

                # Derive gap days from frequency
                freq_to_gap = {
                    "daily": 1.0,
                    "every_2_3_days": 2.5,
                    "weekly": 7.0,
                    "bi_weekly": 14.0,
                    "monthly": 30.0,
                }
                median_gap = freq_to_gap.get(freq, 7.0)

                patterns[row["SUPPLIER_NAME"]] = {
                    "order_frequency": freq,
                    "avg_gap_days": median_gap,
                    "median_gap_days": median_gap,
                    "estimated_delivery_days": lead,
                    "total_orders_2025": int(row["total_grns"] or 0),
                    "avg_order_value_kes": round(float(row["avg_grn_value"] or 0), 2),
                    "reliability_score": float(row["RELIABILITY_SCORE"] or 0.9),
                }

        logger.info(f"Supplier patterns: {len(patterns)} suppliers for {org_cd}")
        return patterns

    # ------------------------------------------------------------------
    # 6. BI Summary  →  pd.DataFrame from pre-aggregated BI_SALES_REPORT
    # ------------------------------------------------------------------
    def fetch_bi_summary(self, org_cd: str) -> pd.DataFrame:
        """
        Fetch pre-aggregated BI sales data (mirrors iAnalytics output).
        """
        query = text("""
            SELECT 
                b.ITM_CD AS item_code,
                i.ITM_LONG_NAME AS product_name,
                b.REPORT_MONTH,
                b.QUANTITY,
                b.TAX_INCL,
                b.TAX_EXCL,
                b.TOTAL_TAX,
                b.COST,
                b.TRANSACTION_COUNT
            FROM BI_SALES_REPORT b
            JOIN ITEM_MST i ON i.ITM_CD = b.ITM_CD
            WHERE b.ORG_CD = :org_cd
            ORDER BY b.ITM_CD, b.REPORT_MONTH
        """)

        with self.engine.connect() as conn:
            df = pd.read_sql(query, conn, params={"org_cd": org_cd})
        logger.info(f"BI summary: {len(df)} rows for {org_cd}")
        return df

    # ------------------------------------------------------------------
    # 7. Organization Info  →  dict for store profile
    # ------------------------------------------------------------------
    def fetch_organization(self, org_cd: str) -> Optional[dict]:
        """Fetch store profile information."""
        query = text("""
            SELECT * FROM ORGANIZATION_MST WHERE ORG_CD = :org_cd
        """)
        with self.engine.connect() as conn:
            result = conn.execute(query, {"org_cd": org_cd})
            row = result.mappings().fetchone()
            return dict(row) if row else None

    def fetch_all_organizations(self) -> List[dict]:
        """Fetch all active organizations."""
        query = text("SELECT * FROM ORGANIZATION_MST WHERE ACTIVE_FLAG = 'Y'")
        with self.engine.connect() as conn:
            return [dict(r) for r in conn.execute(query).mappings()]

    # ------------------------------------------------------------------
    # 8. System Preferences  →  dict for config
    # ------------------------------------------------------------------
    def fetch_system_preferences(self) -> dict:
        """Fetch all system preferences as a key-value dict."""
        query = text("SELECT BSP_PREF_DESC, BSP_PREF_VALUE FROM BASE_SYSTEM_PREFERENCES")
        with self.engine.connect() as conn:
            result = conn.execute(query)
            return {row["BSP_PREF_DESC"]: row["BSP_PREF_VALUE"] for row in result.mappings()}

    # ------------------------------------------------------------------
    # 9. Write-Back: Push Purchase Order to ERP
    # ------------------------------------------------------------------
    def push_purchase_order(self, org_cd: str, recommendations: List[dict]) -> int:
        """
        Write OASIS recommendations back to the ERP integration table.
        Returns number of PO lines written.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        rows = []

        for rec in recommendations:
            qty = rec.get("recommended_quantity", 0)
            if qty <= 0:
                continue

            cost = rec.get("cost_price", rec.get("selling_price", 0) * 0.75)
            rows.append({
                "ORG_CD": org_cd,
                "ITM_CD": rec.get("item_code", ""),
                "PRODUCT_NAME": rec.get("product_name", ""),
                "SUPPLIER_CD": rec.get("supplier_cd", ""),
                "QUANTITY": qty,
                "UNIT_COST": cost,
                "TOTAL_COST": round(qty * cost, 2),
                "REASONING": rec.get("reasoning", ""),
                "STATUS": "PENDING",
                "CREATED_DT": now,
            })

        if rows:
            df = pd.DataFrame(rows)
            df.to_sql("INTEGRATION_PURCHASE_ORDERS", self.engine, if_exists="append", index=False)

        logger.info(f"Pushed {len(rows)} PO lines for {org_cd}")
        return len(rows)

    # ------------------------------------------------------------------
    # 10. Write-Back: Push Transfer Request
    # ------------------------------------------------------------------
    def push_transfer_request(self, from_org: str, to_org: str, items: List[dict]) -> bool:
        """
        Push inter-store transfer request. 
        In production, this would create a Transfer Out document in the ERP.
        """
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        logger.info(f"Transfer request: {len(items)} items from {from_org} → {to_org}")
        # For now, log it. In production, this maps to TRANSFER_OUT_HDR/DTL tables.
        return True

    # ------------------------------------------------------------------
    # 11. Enriched Products (Convenience: combines master + stock + sales)
    # ------------------------------------------------------------------
    def fetch_enriched_products(self, org_cd: str, sales_days: int = 90) -> List[dict]:
        """
        All-in-one: fetches products with stock levels AND enriches with
        sales-derived metrics (ADS, trend, CV). This single call replaces:
          - OrderEngine.parse_inventory_file()
          - OrderEngine.load_local_databases() 
          - OrderEngine.enrich_product_data() [partially]
        
        Returns list of product dicts ready for OrderEngine.enrich_product_data().
        """
        # 1. Get base products
        products = self.fetch_product_master(org_cd)
        product_map = {p["product_name"]: p for p in products}

        # 2. Get sales intelligence
        sales_intel = self.fetch_sales_intelligence(org_cd, days=sales_days)

        # 3. Merge sales data into products
        for product in products:
            name = product["product_name"]
            intel = sales_intel.get(name, {})

            product["avg_daily_sales"] = intel.get("avg_daily_sales", 0.0)
            product["estimated_daily_sales"] = intel.get("avg_daily_sales", 0.0)
            product["units_sold_last_month"] = intel.get("avg_monthly_sales", 0.0)
            product["sales_trend"] = intel.get("trend", "stable")
            product["months_active"] = intel.get("months_active", 0)

            if intel:
                # Calculate CV from monthly data
                monthly_vals = list(intel.get("monthly_sales", {}).values())
                if len(monthly_vals) >= 2:
                    mean_val = statistics.mean(monthly_vals)
                    if mean_val > 0:
                        product["demand_cv"] = round(statistics.stdev(monthly_vals) / mean_val, 3)
                    else:
                        product["demand_cv"] = 1.0
                else:
                    product["demand_cv"] = 0.5
            else:
                product["demand_cv"] = 0.5

        logger.info(f"Enriched {len(products)} products for {org_cd}")
        return products
