"""
One battery every ERP backend must pass, encoding the traps already paid for.

Each finding this month came from an adapter that agreed with itself and
disagreed with the real system. A per-adapter test written by whoever wrote the
adapter reproduces the author's assumptions; a SHARED battery does not.

TWO LAYERS, DELIBERATELY
------------------------
1. **Static** — capability honesty and contract shape. Runs always, needs no
   credentials, and catches the "declared but not implemented" class.
2. **Live** — the same assertions against a real instance, skipped unless that
   backend's credentials are in the environment. This is the layer that matters:
   nothing is "done" until its live block runs green.

A new adapter therefore starts with its live tests SKIPPED, which is honest, and
turns green only when someone points it at a real system. That is the gate.
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import erp_contract as C
from oasis.logic.odoo_adapter import OdooAdapter          # noqa: F401  (registers)
from oasis.logic.pos_erp_adapter import PosErpAdapter     # noqa: F401  (registers)
from oasis.logic.zoho_adapter import ZohoAdapter          # noqa: F401  (registers)
from oasis.logic.tally_adapter import TallyAdapter        # noqa: F401  (registers)

#: name -> the env var that, when set, means a live instance is reachable.
#: Add a row when a backend is built; the live tests light up automatically.
#:
#: A backend whose row is here but whose var is unset shows as SKIPPED, which is
#: the honest state for an adapter written from an API reference and not yet
#: pointed at a real organisation. It is not done until these run green.
LIVE_ENV = {
    "odoo": "OASIS_TEST_ODOO",
    "zoho": "OASIS_TEST_ZOHO",
    "tally": "OASIS_TEST_TALLY",
}

ADAPTERS = [OdooAdapter, PosErpAdapter, ZohoAdapter, TallyAdapter]


# ── layer 1: static contract conformance ─────────────────────────────────
@pytest.mark.parametrize("cls", ADAPTERS, ids=lambda c: c.ERP_NAME)
def test_backend_declares_a_name_and_capabilities(cls):
    assert cls.ERP_NAME and cls.ERP_NAME != "base", "every backend needs a name"
    assert cls.CAPABILITIES, f"{cls.ERP_NAME} declares no capabilities"
    unknown = set(cls.CAPABILITIES) - set(C.ALL_CAPABILITIES)
    assert not unknown, f"{cls.ERP_NAME} declares unknown capabilities: {unknown}"


@pytest.mark.parametrize("cls", ADAPTERS, ids=lambda c: c.ERP_NAME)
def test_declared_capabilities_are_actually_implemented(cls):
    """The "declared but missing" class of bug.

    OdooAdapter listed update_po_status, fetch_transfers and
    push_transfer_request on its contract for days while none of them existed.
    A declared capability whose method is still the base stub is a lie the
    console will act on.
    """
    method_for = {
        C.READ_CATALOGUE: "fetch_enriched_products",
        C.READ_DEMAND: "fetch_sales_history",
        C.READ_OPEN_POS: "fetch_pending_po_by_sku",
        C.READ_TRANSFERS: "fetch_transfers",
        C.WRITE_PO: "push_purchase_order",
        C.WRITE_PO_STATUS: "update_po_status",
        C.WRITE_TRANSFER: "push_transfer_request",
        C.WRITE_TRANSFER_STATUS: "update_transfer_status",
    }
    for cap, name in method_for.items():
        if cap not in cls.CAPABILITIES:
            continue
        assert getattr(cls, name) is not getattr(C.ErpAdapter, name), (
            f"{cls.ERP_NAME} declares {cap} but {name}() is still the base stub"
        )


@pytest.mark.parametrize("cls", ADAPTERS, ids=lambda c: c.ERP_NAME)
def test_undeclared_capabilities_raise_rather_than_no_op(cls):
    """An unsupported write must RAISE, never quietly do nothing.

    Silently returning 0 or [] is the worst outcome: the operator presses
    "push order", sees no error, and believes it worked.
    """
    for cap in C.ALL_CAPABILITIES - set(cls.CAPABILITIES):
        method = {
            C.WRITE_PO: ("push_purchase_order", ("ORG", [])),
            C.WRITE_TRANSFER: ("push_transfer_request", ("A", "B", [])),
            C.READ_TRANSFERS: ("fetch_transfers", (None,)),
        }.get(cap)
        if not method:
            continue
        name, args = method
        with pytest.raises(C.Unsupported):
            getattr(cls, name)(cls.__new__(cls), *args)


@pytest.mark.parametrize("cls", ADAPTERS, ids=lambda c: c.ERP_NAME)
def test_backend_meets_the_minimum_to_be_worth_installing(cls):
    missing = cls.missing_for_viability()
    assert not missing, (
        f"{cls.ERP_NAME} cannot supply {missing} — the engine would produce "
        f"nothing and the console would show zeroes rather than an explanation"
    )


def test_the_registry_resolves_every_known_backend():
    assert set(C.available()) >= {"odoo", "pos"}
    with pytest.raises(KeyError, match="unknown ERP backend"):
        C.build("definitely-not-an-erp")


def test_unsupported_names_the_backend_and_the_capability():
    """The message is what a support engineer reads at 2am."""
    e = C.Unsupported("square", C.WRITE_PO, "Square has no purchase order object")
    assert "square" in str(e) and C.WRITE_PO in str(e)
    assert "purchase order object" in str(e)


# ── layer 2: live conformance ────────────────────────────────────────────
def _live(name):
    """The adapter for `name` if its credentials are present, else skip."""
    env = LIVE_ENV.get(name)
    if not env or not os.getenv(env):
        pytest.skip(f"set {env or 'credentials'} to run live conformance for {name}")
    return C.build(name)


@pytest.mark.parametrize("name", sorted(LIVE_ENV), ids=lambda n: f"live-{n}")
class TestLiveConformance:
    """Runs against a REAL instance. This is the layer that makes a backend done."""

    def test_connects(self, name):
        h = _live(name).health_check()
        assert h.get("connected") is True, f"{name}: {h.get('error')}"

    def test_sites_have_codes_the_engine_can_scope_by(self, name):
        orgs = _live(name).fetch_all_organizations()
        assert orgs, "no sites returned — every scoped read would be company-wide"
        for o in orgs:
            assert o.get("ORG_CD"), f"site without ORG_CD: {o}"

    def test_catalogue_returns_the_engine_dict_shape(self, name):
        a = _live(name)
        org = a.fetch_all_organizations()[0]["ORG_CD"]
        prods = a.fetch_enriched_products(org)
        assert prods, "empty catalogue — the engine will recommend nothing"
        required = {"item_code", "product_name", "current_stocks",
                    "cost_price", "selling_price", "department",
                    "avg_daily_sales", "days_since_delivery"}
        missing = required - set(prods[0])
        assert not missing, f"{name} product dict missing {missing}"

    def test_org_cd_actually_scopes(self, name):
        """THE bug this contract exists for.

        fetch_enriched_products accepted org_cd and ignored it, so every store
        in a chain read the whole company's stock and ordered as though it held
        it — systematic under-ordering, no error, for months.
        """
        a = _live(name)
        if not a.supports(C.MULTI_SITE):
            pytest.skip(f"{name} is single-site")
        orgs = [o["ORG_CD"] for o in a.fetch_all_organizations()]
        if len(orgs) < 2:
            pytest.skip("needs two sites to prove scoping")

        def position(org):
            ps = a.fetch_enriched_products(org)
            return (sum(float(p.get("current_stocks") or 0) for p in ps),
                    sum(float(p.get("avg_daily_sales") or 0) for p in ps))

        positions = {o: position(o) for o in orgs[:3]}
        assert len(set(positions.values())) > 1, (
            f"{name}: every site reported an identical position {positions} — "
            f"org_cd is being accepted and ignored"
        )

    def test_no_site_reports_negative_stock(self, name):
        """Negative on-hand does not error; it silently inflates every order
        quantity computed from it. Worth failing the conformance run."""
        a = _live(name)
        org = a.fetch_all_organizations()[0]["ORG_CD"]
        neg = [p for p in a.fetch_enriched_products(org)
               if float(p.get("current_stocks") or 0) < 0]
        assert not neg, (
            f"{name}/{org}: {len(neg)} SKUs with negative stock, e.g. "
            f"{[p.get('item_code') for p in neg[:3]]}"
        )

    def test_active_filter_matches_rows_by_value_not_just_by_name(self, name):
        """RXL marked an active item ITM_STATUS='O' where the contract means
        'Y'. The mapping was structurally perfect and returned ZERO rows. So
        conformance asserts the filter MATCHES, not that a column exists."""
        a = _live(name)
        org = a.fetch_all_organizations()[0]["ORG_CD"]
        assert a.fetch_enriched_products(org), (
            f"{name}: catalogue filter matched no rows — check mapped VALUES, "
            f"not just column presence"
        )

    def test_declared_receipt_dates_are_actually_populated(self, name):
        """A guard keyed on a NULL column is worse than no guard: it looks
        present. RXL mapped SM_LAST_RECV_DT to NULL, days_since_delivery
        defaulted to 0, and `> 200` was never true — KES 10.4M of dead-stock
        ordering went through with confident-looking reasoning strings."""
        a = _live(name)
        if not a.supports(C.READ_RECEIPTS):
            pytest.skip(f"{name} does not claim receipt dates")
        org = a.fetch_all_organizations()[0]["ORG_CD"]
        prods = a.fetch_enriched_products(org)
        with_dates = [p for p in prods
                      if int(p.get("days_since_delivery") or 0) > 0]
        assert with_dates, (
            f"{name} declares READ_RECEIPTS but days_since_delivery is 0 for "
            f"all {len(prods)} products — the dead-stock guard cannot fire "
            f"(fails OPEN)"
        )

    def test_transfers_carry_the_console_columns_even_when_empty(self, name):
        """notification_service masks on df['TO_ORG_CD']; a bare DataFrame()
        raises KeyError instead of returning nothing."""
        a = _live(name)
        if not a.supports(C.READ_TRANSFERS):
            pytest.skip(f"{name} has no transfers")
        df = a.fetch_transfers(None)
        for col in ("TRANSFER_ID", "FROM_ORG_CD", "TO_ORG_CD", "STATUS"):
            assert col in df.columns, f"{name}: transfers missing {col}"

    def test_diagnose_explains_an_empty_order(self, name):
        d = _live(name).diagnose()
        assert d.get("connected") is True
        for k in ("products", "with_demand", "with_cost"):
            assert k in d, f"{name}: diagnose() omits {k}"
