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
        from oasis.logic import odoo_adapter, pos_erp_adapter, tally_adapter, zoho_adapter
        for mod in (odoo_adapter, pos_erp_adapter, zoho_adapter, tally_adapter):
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


# ── demand: Zoho and Tally too ─────────────────────────────────────────────
# Both divided 90 days of units by 90 on the wall clock -- no recency
# weighting, no observed-window guard, no line window, no sell-out correction
# -- so a shop on Zoho or Tally ordered from a different d than the same shop
# on its POS database or Odoo.
AS_OF = datetime(2025, 12, 1)


def _history(days, recent=4.0, older=2.0):
    return [(AS_OF - timedelta(days=k), recent if k < 30 else older) for k in range(days)]


def _pos_rate(tmp_path, daily):
    pos = TestOneDemandRate()._pos_store(tmp_path, [(d.strftime("%Y-%m-%d"), "SKU1", q) for d, q in daily])
    return pos._calc_weighted_ads("ORG001")["SKU1"]["weighted_ads"]


def _zoho(monkeypatch, sales, items):
    from oasis.logic.zoho_adapter import ZohoAdapter
    z = ZohoAdapter.__new__(ZohoAdapter)
    z._loc_cache = {}
    orders = [{"salesorder_id": f"S{i}", "date": d.strftime("%Y-%m-%d"),
               "line_items": [{"item_id": iid, "quantity": q, "rate": 50}]}
              for i, (d, iid, q) in enumerate(sorted(sales, key=lambda s: s[0], reverse=True))]
    monkeypatch.setattr(z, "_call", lambda method, path, params=None, **k: (
        {"salesorders": orders if params.get("page") == 1 else [], "page_context": {"has_more_page": False}}
        if path == "/salesorders" else {}), raising=False)
    monkeypatch.setattr(z, "_paged", lambda path, key, params=None: items if path == "/items" else [], raising=False)
    monkeypatch.setattr(z, "_vendor_names", lambda: {}, raising=False)
    return z


def _zoho_item(iid, created=None, dept="BREAD"):
    return {"item_id": iid, "sku": iid, "name": iid, "status": "active", "category_name": dept,
            "rate": 60, "purchase_rate": 50, "stock_on_hand": 3,
            **({"created_time": created.strftime("%Y-%m-%dT09:00:00+0300")} if created else {})}


class TestEveryAdapterGivesTheSameRate:
    @pytest.fixture(autouse=True)
    def clock(self, monkeypatch):
        monkeypatch.setenv("OASIS_AS_OF", AS_OF.strftime("%Y-%m-%d"))
        monkeypatch.delenv("OASIS_GRN_EXPORT", raising=False)

    def test_zoho_gives_the_pos_rate_on_a_young_store(self, tmp_path, monkeypatch):
        daily = _history(50)
        z = _zoho(monkeypatch, [(d, "SKU1", q) for d, q in daily], [_zoho_item("SKU1")])
        got = z.fetch_enriched_products(None)[0]
        assert got["avg_daily_sales"] == pytest.approx(_pos_rate(tmp_path, daily), abs=1e-4)
        assert got["avg_daily_sales"] != pytest.approx(sum(q for _, q in daily) / 90)

    def test_zoho_measures_a_new_line_from_its_creation(self, monkeypatch):
        # a store with a full history; a line created 20 days ago selling 1 a day
        old = [(d, "OLD", 5.0) for d, _ in _history(90)]
        new = [(AS_OF - timedelta(days=k), "NEW", 1.0) for k in range(20)]
        z = _zoho(monkeypatch, old + new, [_zoho_item("OLD"), _zoho_item("NEW", created=AS_OF - timedelta(days=19))])
        rows = {r["item_code"]: r for r in z.fetch_enriched_products(None)}
        assert rows["NEW"]["avg_daily_sales"] == pytest.approx(1.0, abs=0.02)

    def test_tally_gives_the_pos_rate(self, tmp_path, monkeypatch):
        from xml.etree import ElementTree as ET
        from oasis.logic.tally_adapter import TallyAdapter
        daily = _history(90)
        t = TallyAdapter.__new__(TallyAdapter)
        t.company = ""
        vouchers = "".join(
            f"<VOUCHER><DATE>{d:%Y%m%d}</DATE><ALLINVENTORYENTRIES.LIST><STOCKITEMNAME>SKU1</STOCKITEMNAME>"
            f"<ACTUALQTY>-{q} Nos</ACTUALQTY><AMOUNT>-{q * 50}</AMOUNT></ALLINVENTORYENTRIES.LIST></VOUCHER>"
            for d, q in daily)
        monkeypatch.setattr(t, "_post", lambda xml, timeout=120: ET.fromstring(f"<ENVELOPE>{vouchers}</ENVELOPE>"),
                            raising=False)
        monkeypatch.setattr(t, "_stock_items", lambda: [ET.fromstring(
            '<STOCKITEM NAME="SKU1"><PARENT>BREAD</PARENT><CLOSINGBALANCE>10 Nos</CLOSINGBALANCE>'
            '<CLOSINGVALUE>500</CLOSINGVALUE></STOCKITEM>')], raising=False)
        got = t.fetch_enriched_products(None)[0]
        assert got["avg_daily_sales"] == pytest.approx(_pos_rate(tmp_path, daily), abs=1e-4)

    def test_zoho_applies_the_sell_out_correction(self, monkeypatch):
        # a one-unit shelf with true demand 1.1 a day sells out most days
        import numpy as np
        from oasis.logic import censored_demand as cd
        rng = np.random.default_rng(5)
        start = (AS_OF - timedelta(days=89)).date()
        left, rec, sales = 0.0, {}, []
        for i in range(90):
            day = start + timedelta(days=i)
            q = max(0.0, 1 - left)
            rec[day] = q
            s = float(min(rng.poisson(1.1), left + q))
            left = left + q - s
            if s:
                sales.append((datetime.combine(day, datetime.min.time()), "LOAF", s))

        class Src:
            def items(self):
                return ["LOAF"]

            def coverage(self):
                return (start, AS_OF.date())

            def daily_receipts(self, keys, lo, hi):
                return {"LOAF": {d: q for d, q in rec.items() if lo <= d <= hi}}

        monkeypatch.setattr(cd, "configured_receipts_source", lambda: Src())
        z = _zoho(monkeypatch, sales, [_zoho_item("LOAF")])
        row = z.fetch_enriched_products(None)[0]
        assert row["ads_source"] == "zoho_weighted+censored"
        assert row["avg_daily_sales"] > row["ads_uncensored"]


# ── one pipeline, every surface ────────────────────────────────────────────
# The desktop tab, web app and Odoo queue called generate_smart_orders; the
# Command Center and the Operations Console each assembled the stages
# themselves and had drifted (a degraded transfer service, a second registry
# file, a network built from the stores the viewer may see).
ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


class TestOnePipeline:
    def test_every_ordering_surface_calls_the_pipeline(self):
        import inspect
        from oasis.desktop import data as D
        from oasis.ui import shell
        assert "run_ordering_pipeline(" in inspect.getsource(D.generate_smart_orders)
        assert "run_ordering_pipeline(" in inspect.getsource(shell.render_ordering)
        dash = open(os.path.join(ROOT, "ops_dashboard.py"), encoding="utf-8").read()
        assert "run_ordering_pipeline(" in dash

    def test_one_place_builds_the_transfer_service(self):
        import glob
        files = glob.glob(os.path.join(ROOT, "oasis", "**", "*.py"), recursive=True)
        files.append(os.path.join(ROOT, "ops_dashboard.py"))
        builders = []
        for f in files:
            if f.endswith("consolidated_transfer_service.py"):
                continue                                   # the class itself
            src = open(f, encoding="utf-8").read()
            if "ConsolidatedTransferService(" in src:
                builders.append(os.path.relpath(f, ROOT).replace("\\", "/"))
        assert builders == ["oasis/desktop/data.py"], builders

    def test_one_registry(self):
        dash = open(os.path.join(ROOT, "ops_dashboard.py"), encoding="utf-8").read()
        assert 'REGISTRY_PATH = os.path.join(DATA_DIR, "network_registry.json")' in dash
        assert 'os.path.join(DATA_DIR, "transfers_registry.json")' not in dash

    def test_the_network_is_every_store_not_the_viewers(self, tmp_path, monkeypatch):
        from oasis.desktop import data as D
        from oasis.logic import gnn_service
        import oasis.logic.simulation_bridge as sb
        seen = {}

        class Util:
            def __init__(self, *a, **k):
                self.thresholds = {}

            def prepare_sku_data(self, p):
                return p

            def calculate_order_quantity(self, e, **k):
                return e

            def finalize_orders(self, r):
                return r

            def apply_minimum_order_gate(self, r):
                return {"po_recs": r, "transfer_recs": [], "no_order": []}

        class Plan:
            def __init__(self, orders):
                self.adjusted_orders, self.transfers = orders, []

        class Cts:
            def optimize_network(self, orders, risk_scores=None):
                return Plan(orders)

        def build(org_names, stock, root=None, **k):
            seen["orgs"], seen["stock"] = sorted(org_names), sorted(stock)
            return Cts()

        class Adapter:
            def fetch_enriched_products(self, o):
                return [{"sku": f"{o}-1"}]

            def fetch_all_organizations(self):
                return [{"ORG_CD": c} for c in ("A", "B", "C")]

        monkeypatch.setattr(D, "build_transfer_service", build)
        monkeypatch.setattr(sb, "SimulationOrderUtil", Util)
        monkeypatch.setattr(gnn_service, "ordering_risk", lambda *a, **k: 0.0)
        run = D.run_ordering_pipeline("A", root=str(tmp_path), adapter=Adapter(), engine=object(),
                                      record_failures=False)
        assert seen["orgs"] == seen["stock"] == ["A", "B", "C"]
        assert run["mot_result"]["po_recs"] == [{"sku": "A-1"}]


class TestTheAdapterSwitch:
    @pytest.fixture(autouse=True)
    def fresh_adapter(self, tmp_path, monkeypatch):
        from oasis.desktop import data as D
        monkeypatch.setenv("OASIS_DB_PATH", str(tmp_path / "store.db"))
        D.reset_adapter()
        yield
        D.reset_adapter()

    def test_oasis_erp_zoho_reads_zoho(self, monkeypatch):
        # used to fall through to the POS database without a word
        from oasis.desktop import data as D
        from oasis.logic.zoho_adapter import ZohoAdapter
        monkeypatch.setenv("OASIS_ERP", "zoho")
        assert isinstance(D.get_adapter(), ZohoAdapter)

    def test_an_unknown_backend_is_an_error_not_the_pos(self, monkeypatch):
        from oasis.desktop import data as D
        monkeypatch.setenv("OASIS_ERP", "nosuch")
        with pytest.raises(Exception):
            D.get_adapter()
