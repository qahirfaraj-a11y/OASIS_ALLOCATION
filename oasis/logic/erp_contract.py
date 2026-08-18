"""
The ERP adapter contract — what OASIS needs from a client's system, what each
backend can actually supply, and the registry that maps a name to a builder.

WHY THIS EXISTS
---------------
Two adapters (``PosErpAdapter``, ``OdooAdapter``) were written independently
against the same informal contract, and the same class of bug appeared in both:
a method accepts a parameter and ignores it, or returns an empty result where it
should have raised, and NOTHING fails. ``fetch_enriched_products`` took an
``org_cd`` and read the whole company for months. ``ACTIVE_FLAG`` mapped by name
but not by value and filtered 39,728 rows to zero. Neither raised.

Adding eight more backends by copying the pattern would multiply that. So the
contract is written down here, capabilities are DECLARED rather than assumed,
and ``tests/test_erp_conformance.py`` runs one battery against every registered
adapter.

CAPABILITIES, AND WHY THEY ARE NOT OPTIONAL
-------------------------------------------
The target systems are not interchangeable. Square and Shopify have no purchase
order concept at all; QuickBooks Online has purchase orders but no real
multi-location stock below its top tiers; Pastel and Sage 50 are desktop
products reached through their database, exactly like RXL.

An adapter that silently no-ops an unsupported call is the worst outcome — the
operator presses "push order" and nothing happens, with no error. So an adapter
DECLARES what it supports, unsupported calls raise :class:`Unsupported`, and the
console can disable the control and say why instead of lying about it.
"""

from __future__ import annotations

import logging
import os
from typing import Any, Callable, Dict, FrozenSet, List, Optional

logger = logging.getLogger("ErpContract")


# ── capabilities ─────────────────────────────────────────────────────────
#: Reads. An adapter missing one of these degrades the intelligence but does
#: not break it — except READ_DEMAND, without which the engine recommends
#: nothing at all and looks broken rather than empty.
READ_CATALOGUE   = "read_catalogue"
READ_STOCK       = "read_stock"
READ_DEMAND      = "read_demand"
READ_COST        = "read_cost"
READ_RECEIPTS    = "read_receipts"      # gates the dead-stock / stale-fresh rules
READ_SUPPLIERS   = "read_suppliers"
READ_OPEN_POS    = "read_open_pos"      # feeds on_order_qty; without it, day 2 re-orders day 1
MULTI_SITE       = "multi_site"

#: Writes. Everything OASIS writes is a PROPOSAL — a draft a human confirms.
WRITE_PO         = "write_purchase_order"
WRITE_PO_STATUS  = "write_po_status"
READ_TRANSFERS   = "read_transfers"
WRITE_TRANSFER   = "write_transfer"
WRITE_TRANSFER_STATUS = "write_transfer_status"

ALL_CAPABILITIES: FrozenSet[str] = frozenset({
    READ_CATALOGUE, READ_STOCK, READ_DEMAND, READ_COST, READ_RECEIPTS,
    READ_SUPPLIERS, READ_OPEN_POS, MULTI_SITE,
    WRITE_PO, WRITE_PO_STATUS, READ_TRANSFERS, WRITE_TRANSFER,
    WRITE_TRANSFER_STATUS,
})

#: The minimum that makes OASIS worth installing. Below this the engine cannot
#: produce a defensible order and onboarding should say so rather than ship a
#: console full of zeroes.
MINIMUM_VIABLE: FrozenSet[str] = frozenset({
    READ_CATALOGUE, READ_STOCK, READ_DEMAND,
})


class Unsupported(NotImplementedError):
    """This backend genuinely cannot do this — not "not built yet".

    Raised rather than returning an empty result, because an empty result is
    indistinguishable from "nothing to do" and that ambiguity has already cost
    this codebase real debugging time. The console catches it to disable a
    control and explain why.
    """

    def __init__(self, erp: str, capability: str, detail: str = ""):
        self.erp, self.capability = erp, capability
        msg = f"{erp} does not support {capability}"
        if detail:
            msg += f" — {detail}"
        super().__init__(msg)


# ── the contract ─────────────────────────────────────────────────────────
class ErpAdapter:
    """Base for every backend. Subclasses override only what they support.

    Every method defaults to raising :class:`Unsupported`, so a half-written
    adapter fails loudly at the exact missing method instead of returning
    something empty and plausible.
    """

    #: short lowercase identifier, matches the OASIS_ERP value
    ERP_NAME: str = "base"
    #: what this backend can actually do — see module docstring
    CAPABILITIES: FrozenSet[str] = frozenset()

    # ── capability surface ───────────────────────────────────────────────
    @classmethod
    def supports(cls, capability: str) -> bool:
        return capability in cls.CAPABILITIES

    @classmethod
    def capabilities(cls) -> FrozenSet[str]:
        return cls.CAPABILITIES

    @classmethod
    def missing_for_viability(cls) -> FrozenSet[str]:
        """Which minimum capabilities this backend cannot meet."""
        return MINIMUM_VIABLE - cls.CAPABILITIES

    def _require(self, capability: str, detail: str = "") -> None:
        if capability not in self.CAPABILITIES:
            raise Unsupported(self.ERP_NAME, capability, detail)

    # ── reads ────────────────────────────────────────────────────────────
    def health_check(self) -> Dict[str, Any]:
        """``{connected, latency_ms, error}`` — never raises."""
        raise Unsupported(self.ERP_NAME, "health_check")

    def fetch_all_organizations(self) -> List[dict]:
        """``[{ORG_CD, ORG_NAME, ACTIVE_FLAG}]`` — the sites OASIS can scope to."""
        raise Unsupported(self.ERP_NAME, READ_CATALOGUE)

    def fetch_enriched_products(self, org_cd: Optional[str] = None,
                                sales_days: int = 90) -> List[dict]:
        """The engine's input, in PosErpAdapter's dict shape.

        ``org_cd`` MUST scope the result when the adapter declares MULTI_SITE.
        Accepting it and returning company-wide numbers is the defect this
        contract exists to prevent; the conformance suite asserts against it.
        """
        raise Unsupported(self.ERP_NAME, READ_CATALOGUE)

    def fetch_sales_history(self, org_cd: Optional[str] = None,
                            days: int = 90) -> List[dict]:
        """``[{item_code, units, revenue}]`` over the window, scoped to the site."""
        raise Unsupported(self.ERP_NAME, READ_DEMAND)

    def fetch_pending_pos(self, org_cd: Optional[str] = None):
        """Open purchase orders awaiting approval or receipt."""
        raise Unsupported(self.ERP_NAME, READ_OPEN_POS)

    def fetch_pending_po_by_sku(self, org_cd: Optional[str] = None) -> Dict[str, dict]:
        """``{sku: {qty, eta_days}}`` still inbound.

        Without this the next run re-orders everything already in transit — so
        an adapter that cannot supply it should NOT declare READ_OPEN_POS.
        """
        raise Unsupported(self.ERP_NAME, READ_OPEN_POS)

    def fetch_transfers(self, org_cd: Optional[str] = None):
        """Transfer history as a DataFrame in the console's column shape."""
        raise Unsupported(self.ERP_NAME, READ_TRANSFERS)

    # ── writes: proposals only ───────────────────────────────────────────
    def push_purchase_order(self, org_cd: str,
                            recommendations: List[dict]) -> int:
        """Create DRAFT purchase orders. Returns lines written.

        Draft is not a limitation, it is the design: OASIS proposes and a human
        commits. An adapter that confirms orders outright is a bug, not a
        feature.
        """
        raise Unsupported(self.ERP_NAME, WRITE_PO)

    def update_po_status(self, po_id: int, status: str, approved_by: str,
                         new_quantity: Optional[float] = None,
                         reason: Optional[str] = None) -> bool:
        raise Unsupported(self.ERP_NAME, WRITE_PO_STATUS)

    def can_transfer(self, from_org: str, to_org: str) -> Dict[str, Any]:
        """Would ``push_transfer_request(from_org, to_org, ...)`` be accepted?

        ``{"ok": bool, "reason": str}``. Cheap enough to ask per store pair
        before showing a recommendation.

        This exists because the READ side and the WRITE side were disagreeing.
        ``network_transfer_scan`` happily recommended four moves between two
        Odoo warehouses that turned out to be in different companies — a
        transfer Odoo cannot confirm and the adapter correctly refuses. The
        operator would have seen four sensible moves, clicked, and been told
        no. The stock imbalance was real; the execution route was not.

        Implementations MUST be the same rule ``push_transfer_request``
        enforces — the writer calls this, rather than repeating the check —
        so the two can never drift apart again.
        """
        if WRITE_TRANSFER not in self.CAPABILITIES:
            return {"ok": False,
                    "reason": f"{self.ERP_NAME} cannot execute transfers"}
        return {"ok": True, "reason": ""}

    def push_transfer_request(self, from_org: str, to_org: str,
                              items: List[dict]) -> bool:
        raise Unsupported(self.ERP_NAME, WRITE_TRANSFER)

    def update_transfer_status(self, transfer_id: int, status: str) -> bool:
        raise Unsupported(self.ERP_NAME, WRITE_TRANSFER_STATUS)

    # ── observability ────────────────────────────────────────────────────
    def diagnose(self, org_cd: Optional[str] = None) -> Dict[str, Any]:
        """Why is the order empty? Connection, catalogue, demand, or cost —
        each needs a different fix, and an adapter is otherwise a black box."""
        raise Unsupported(self.ERP_NAME, "diagnose")


# ── registry ─────────────────────────────────────────────────────────────
#: name -> zero-argument factory. Registration is lazy (a module-path string)
#: so importing this module never drags in an SDK the client has not installed.
_REGISTRY: Dict[str, Callable[..., ErpAdapter]] = {}
_LAZY: Dict[str, str] = {}


def register(name: str, factory: Callable[..., ErpAdapter]) -> None:
    _REGISTRY[name.strip().lower()] = factory


def register_lazy(name: str, dotted_path: str) -> None:
    """Register ``pkg.module:Class`` without importing it yet.

    A client with Odoo installed should not need the Shopify SDK on disk for
    OASIS to start.
    """
    _LAZY[name.strip().lower()] = dotted_path


def available() -> List[str]:
    """Every backend name OASIS knows, registered or lazily registrable."""
    return sorted(set(_REGISTRY) | set(_LAZY))


def _resolve(name: str) -> Callable[..., ErpAdapter]:
    key = name.strip().lower()
    if key in _REGISTRY:
        return _REGISTRY[key]
    if key in _LAZY:
        import importlib
        mod_path, _, attr = _LAZY[key].partition(":")
        try:
            mod = importlib.import_module(mod_path)
        except ImportError as e:
            raise Unsupported(
                key, "import",
                f"backend '{key}' needs a dependency that is not installed ({e})"
            ) from e
        factory = getattr(mod, attr)
        _REGISTRY[key] = factory
        return factory
    raise KeyError(
        f"unknown ERP backend {name!r}. Known: {', '.join(available()) or 'none'}"
    )


def build(name: str, **kwargs) -> ErpAdapter:
    """Instantiate a backend by name, with a clear error for a typo'd one."""
    return _resolve(name)(**kwargs)


def selected_backend() -> str:
    """What OASIS_ERP asks for, normalised. Empty means the default POS path."""
    return (os.getenv("OASIS_ERP") or "").strip().lower()
