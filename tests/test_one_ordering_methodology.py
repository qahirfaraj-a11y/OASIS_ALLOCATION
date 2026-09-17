"""One ordering methodology, whichever ERP the store runs and whichever path orders.

Offline Smart Ordering (PosErpAdapter) and the Odoo review queue (OdooAdapter)
both call generate_smart_orders, so the quantity maths was always shared. The
drift was in what reached it, and in which engine path applied policy:

  demand   POS: 60/30/10 recency weighting on the as-of clock, observed-window
           guard.  Odoo: units / 90 on the wall clock.
  fresh    POS: exact department match.  Odoo and Zoho: substring match, which
           marked AIR FRESHNERS perishable on the Rhapta catalogue.
  AMIT /   classic path: engines.<name>.mode "report" flags, only "enforce"
  MANDE    blocks.  Order-up-to path: blocked whenever the engine was enabled.

Each test below fails if either side drifts away again.
"""
import json
import os
import sqlite3
from datetime import datetime, timedelta

import pytest

from oasis.logic import demand_rate as dr
from oasis.logic.department_constants import is_fresh_department


# ── demand: one definition of d ───────────────────────────────────────────
class TestOneDemandRate:
    def test_full_window_is_the_60_30_10_blend(self):
        assert dr.weighted_daily_rate(300, 150, 60, 90) == pytest.approx(
            0.6 * 10 + 0.3 * 5 + 0.1 * 2)

    def test_young_store_divides_by_days_it_has_and_renormalises(self):
        # 40 days of history: full last-30 bucket, 10 days in the next, none after
        assert dr.bucket_days(40) == (30.0, 10.0, 0.0)
        assert dr.weighted_daily_rate(300, 100, 0, 40) == pytest.approx(
            (0.6 * 10 + 0.3 * 10) / 0.9)

    def test_no_first_sale_means_full_window(self):
        assert dr.observed_days(None, datetime(2025, 12, 1)) == 90.0
        assert dr.observed_days(datetime(2025, 11, 22), datetime(2025, 12, 1)) == 10.0

    def _pos_store(self, tmp_path, bills):
        """A minimal POS database: ORG001 with the given (date, item, qty) bills."""
        from oasis.logic.db_connector import UniversalConnector
        from oasis.logic.pos_erp_adapter import PosErpAdapter
        db = tmp_path / "pos.db"
        con = sqlite3.connect(db)
        con.execute("CREATE TABLE POS_SALES_DTL (TENANT_ID TEXT DEFAULT 'default_tenant', ORG_CD TEXT, "
                    "BILL_NO TEXT, BILL_DT TEXT, SERIAL_NO INTEGER, ITM_CD TEXT, QTY REAL, VOID_FLAG TEXT DEFAULT 'F')")
        con.executemany("INSERT INTO POS_SALES_DTL (ORG_CD, BILL_NO, BILL_DT, SERIAL_NO, ITM_CD, QTY) VALUES ('ORG001',?,?,?,?,?)",
                        [(f"B{i}", d, i, itm, q) for i, (d, itm, q) in enumerate(bills)])
        con.commit(); con.close()
        return PosErpAdapter(UniversalConnector(f"sqlite:///{db}"))

    def test_pos_adapter_and_odoo_adapter_give_the_same_rate(self, tmp_path, monkeypatch):
        """Identical daily sales history through both adapters -> identical d."""
        monkeypatch.setenv("OASIS_AS_OF", "2025-12-01")
        now = datetime(2025, 12, 1)
        # 50 days of history for one item: 4/day recently, 2/day before that
        daily = [(now - timedelta(days=k), 4.0 if k < 30 else 2.0) for k in range(0, 50)]
        pos = self._pos_store(tmp_path, [(d.strftime("%Y-%m-%d"), "SKU1", q) for d, q in daily])
        pos_rate = pos._calc_weighted_ads("ORG001")["SKU1"]["weighted_ads"]

        from oasis.logic.odoo_adapter import OdooAdapter
        a = OdooAdapter.__new__(OdooAdapter)
        a._wh_cache = {}

        def fake_sales(days=90, org_cd=None, since=None, until=None):
            units = sum(q for d, q in daily if d >= since and (until is None or d < until))
            return {1: {"units": units, "revenue": 0.0}}
        monkeypatch.setattr(a, "_sales_by_product", fake_sales, raising=False)
        monkeypatch.setattr(a, "_first_sale_date", lambda org_cd=None: min(d for d, _ in daily), raising=False)
        monkeypatch.setattr(a, "_ex", lambda model, method, args, kw=None: (
            [{"id": 1, "default_code": "SKU1", "display_name": "Item", "categ_id": [1, "BREAD"],
              "list_price": 65, "standard_price": 58, "uom_id": [1, "Units"], "barcode": "",
              "product_tmpl_id": [1, "Item"], "create_date": "2025-01-01 00:00:00"}]
            if model == "product.product" else []), raising=False)
        for name, val in (("_on_hand", {}), ("_supplier_of", {}), ("fetch_pending_po_by_sku", {})):
            monkeypatch.setattr(a, name, lambda *x, _v=val, **k: _v, raising=False)
        monkeypatch.setattr(a, "_last_receipt", lambda *x, **k: ({}, None, False), raising=False)
        monkeypatch.setattr(a, "_warn_if_truncated", lambda *x, **k: None, raising=False)
        odoo_rate = a.fetch_enriched_products("C001")[0]["avg_daily_sales"]

        assert pos_rate == pytest.approx(odoo_rate, abs=1e-4), (
            "the same shop read through its POS database and through Odoo must "
            "give the engine the same demand rate")
        assert pos_rate > 2.0 * 1.0 and pos_rate != pytest.approx(sum(q for _, q in daily) / 90), (
            "the rate must be the recency-weighted one, not units / 90")


class TestOneClock:
    def test_odoo_sales_window_follows_as_of_not_the_wall_clock(self, monkeypatch):
        monkeypatch.setenv("OASIS_AS_OF", "2025-12-01")
        from oasis.logic.odoo_adapter import OdooAdapter
        a = OdooAdapter.__new__(OdooAdapter)
        seen = []

        def rec(model, method, args, kw=None):
            if model == "pos.order.line":
                seen.append(args[0])
            return []
        monkeypatch.setattr(a, "_ex", rec, raising=False)
        monkeypatch.setattr(a, "_warn_if_truncated", lambda *x, **k: None, raising=False)
        monkeypatch.setattr(a, "_warehouse_scope", lambda org: None, raising=False)
        monkeypatch.setattr(a, "_read_paged", lambda *x, **k: [], raising=False)
        a._sales_by_product(90, "C001")
        since = [c for c in seen[0] if c[0] == "order_id.date_order"][0][2]
        assert since.startswith("2025-09-02"), since


# ── fresh: one rule ────────────────────────────────────────────────────────
class TestOneFreshRule:
    @pytest.mark.parametrize("dept,fresh", [
        ("DAIRY", True), ("BAKERY", True), ("FRESH PRODUCE", True), ("BUTCHERY", True),
        ("  bakery ", True),
        ("AIR FRESHNERS", False), ("MUKHUWAS(MOUTH FRESHNER)", False),
        ("GROCERY", False), ("", False), (None, False),
    ])
    def test_exact_department_match(self, dept, fresh):
        assert is_fresh_department(dept) is fresh

    def test_every_adapter_uses_the_shared_rule(self):
        import inspect
        from oasis.logic import odoo_adapter, pos_erp_adapter, zoho_adapter
        for mod in (odoo_adapter, pos_erp_adapter, zoho_adapter):
            src = inspect.getsource(mod)
            assert "is_fresh_department(" in src, f"{mod.__name__} has its own fresh rule"
            assert "for f in FRESH_DEPARTMENTS" not in src, f"{mod.__name__} still substring-matches"


# ── policy: both order paths honour engines.<name>.mode ───────────────────
class TestOnePolicyForBlockingEngines:
    def _util(self, tmp_path, mode):
        cfg = {"engines": {"amit": {"enabled": True, "mode": mode},
                           "mande": {"enabled": True, "mode": mode}}}
        (tmp_path / "oasis_engines_config.json").write_text(json.dumps(cfg), encoding="utf-8")
        from oasis.logic.simulation_bridge import SimulationOrderUtil
        u = SimulationOrderUtil(str(tmp_path))
        u.engine.engines_config = cfg
        return u

    def _line(self, util):
        p = {"sku": "L1", "itm_cd": "L1", "product_name": "BLOCKED LOAF", "supplier_name": "PURGED LTD",
             "avg_daily_sales": 10.0, "current_stock": 0.0, "current_stocks": 0.0, "unit_cost": 50.0,
             "selling_price": 65.0, "department": "GENERAL", "pack_size": 1, "lead_time_days": 1.0}
        util.engine.databases["amit_enforcement"] = {util.engine.normalize_product_name(p["product_name"])}
        util.engine.databases["mande_purge_list"] = {"PURGED LTD"}
        return p

    def test_report_mode_flags_but_does_not_block(self, tmp_path):
        u = self._util(tmp_path, "report")
        r = u.calculate_order_quantity([self._line(u)], use_real_date=True)[0]
        assert "Blocked: AMIT" not in str(r.get("reasoning"))
        assert "Blocked: MANDE" not in str(r.get("reasoning"))
        assert r.get("amit_flag") is True and r.get("mande_flag") is True

    def test_enforce_mode_blocks(self, tmp_path):
        u = self._util(tmp_path, "enforce")
        r = u.calculate_order_quantity([self._line(u)], use_real_date=True)[0]
        assert float(r.get("recommended_quantity") or 0) == 0.0
        assert "Blocked: AMIT" in str(r.get("reasoning"))
