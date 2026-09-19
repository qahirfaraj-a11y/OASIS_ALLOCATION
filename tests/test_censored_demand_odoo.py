"""The sell-out correction on the Odoo adapter: same estimator, native receipts.

Odoo records receipts itself (incoming moves from supplier locations), so the
correction reads receipts and daily sales from stock.move and applies the same
censored_demand.correct_line PosErpAdapter uses -- one methodology whichever
front end the store runs.
"""
import os
import sys
from datetime import date, datetime, timedelta

import numpy as np
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))


def _shelf(S, lam, n=90, seed=5, start=date(2026, 6, 17)):
    rng = np.random.default_rng(seed)
    D = rng.poisson(lam, n)
    left, rec, sold = 0.0, {}, {}
    for i in range(n):
        day = start + timedelta(days=i)
        q = max(0.0, S - left)
        rec[day] = q
        stock = left + q
        s = float(min(D[i], stock))
        sold[day] = s
        left = stock - s
    return sold, rec, start + timedelta(days=n - 1)


def _adapter(sold, rec, fail=False):
    from oasis.logic.odoo_adapter import OdooAdapter
    a = OdooAdapter.__new__(OdooAdapter)
    a._warehouse_scope = lambda org: None

    def ex(model, method, args, kw=None):
        if fail:
            raise RuntimeError("read denied")
        assert (model, method) == ("stock.move", "search_read")
        dom = args[0]
        incoming = ["location_id.usage", "=", "supplier"] in dom
        src = rec if incoming else sold
        if (kw or {}).get("offset"):
            return []
        return [{"product_id": [7, "Loaf"], "date": f"{d.isoformat()} 06:00:00", "product_uom_qty": q}
                for d, q in src.items() if q]
    a._ex = ex
    return a


def test_raises_a_censored_line_from_native_receipts(monkeypatch):
    sold, rec, end = _shelf(1, 1.1)
    monkeypatch.setenv("OASIS_AS_OF", end.isoformat())
    rows = [{"odoo_product_id": 7, "product_name": "LOAF", "department": "BREAD",
             "avg_daily_sales": 0.67, "ads_source": "odoo_weighted"}]
    assert _adapter(sold, rec)._apply_censored_demand("ORG", rows) == 1
    assert rows[0]["avg_daily_sales"] > 0.9 and rows[0]["ads_source"] == "odoo_weighted+censored"


def test_a_read_failure_leaves_every_rate_as_measured(monkeypatch):
    sold, rec, end = _shelf(1, 1.1)
    monkeypatch.setenv("OASIS_AS_OF", end.isoformat())
    rows = [{"odoo_product_id": 7, "product_name": "LOAF", "department": "BREAD", "avg_daily_sales": 0.67}]
    assert _adapter(sold, rec, fail=True)._apply_censored_demand("ORG", rows) == 0
    assert rows[0]["avg_daily_sales"] == 0.67


def test_it_can_be_switched_off(monkeypatch):
    from oasis.logic import engines_config
    sold, rec, end = _shelf(1, 1.1)
    monkeypatch.setenv("OASIS_AS_OF", end.isoformat())
    real = engines_config.load_engines_config
    monkeypatch.setattr(engines_config, "load_engines_config",
                        lambda *a, **k: dict(real(*a, **k), censored_demand={"odoo_native_receipts": False}))
    rows = [{"odoo_product_id": 7, "product_name": "LOAF", "department": "BREAD", "avg_daily_sales": 0.67}]
    assert _adapter(sold, rec)._apply_censored_demand("ORG", rows) == 0
