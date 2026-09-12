"""A reported order reduction must be an order that was actually reduced.

_adjust_order returned None whether it zeroed an order or scanned the whole
list and found nothing, and optimize_network incremented
total_orders_reduced and estimated_savings_kes either way.

Rescued shortfalls -- lines the PO engine bypassed -- carry an original_rec
stub of exactly {'department', 'reasoning'}: no product_name, no itm_cd. Both
lookups matched the empty string against nothing and fell out. Measured on the
real book: called 113 times, matched 0, and the plan announced
"113 orders reduced, est. savings KES -34,264" while the purchase order was
byte-identical.

Finding nothing is CORRECT for those lines: the engine never ordered them, so
no order exists to reduce. Only the claim was wrong. These tests pin the
helper's contract -- it reports what it did -- and that the caller counts on
that report rather than on faith.
"""
import ast
import os

import pytest

from oasis.logic.consolidated_transfer_service import ConsolidatedTransferService

SERVICE = ConsolidatedTransferService
SRC = os.path.join(os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                   "oasis", "logic", "consolidated_transfer_service.py")


def _order(**kw):
    rec = {"product_name": "ZESTA 400G CHOMA SAUCE", "itm_cd": "SKU-1",
           "recommended_quantity": 12, "reasoning": "[base]"}
    rec.update(kw)
    return rec


def test_a_rescued_shortfall_stub_reduces_nothing_and_says_so():
    """The exact shape optimize_network builds for a bypassed line."""
    orders = [_order()]
    stub = {"department": "GROCERY",
            "reasoning": "[RESCUED SHORTFALL] <3 days cover (1.2d), "
                         "bypassed by PO engine."}
    assert SERVICE._adjust_order(orders, stub, 0.0, "[NETWORK: x]") is False
    assert orders[0]["recommended_quantity"] == 12, "an order was touched"
    assert "NETWORK" not in orders[0]["reasoning"]
    assert "network_adjusted" not in orders[0]


def test_a_real_line_is_zeroed_and_reports_true():
    orders = [_order()]
    original = {"product_name": "ZESTA 400G CHOMA SAUCE", "itm_cd": "SKU-1"}
    assert SERVICE._adjust_order(orders, original, 0.0, "[NETWORK: x]") is True
    assert orders[0]["recommended_quantity"] == 0.0
    assert orders[0]["original_quantity"] == 12
    assert orders[0]["network_adjusted"] is True
    assert "[NETWORK: x]" in orders[0]["reasoning"]


def test_the_itm_cd_fallback_still_works():
    """Names drift; codes are the fallback the original code relied on."""
    orders = [_order(product_name="RENAMED IN THE POS")]
    original = {"product_name": "ZESTA 400G CHOMA SAUCE", "itm_cd": "SKU-1"}
    assert SERVICE._adjust_order(orders, original, 0.0, "[NETWORK: x]") is True
    assert orders[0]["recommended_quantity"] == 0.0


def test_an_empty_identity_must_not_match_an_empty_record():
    """The subtle half of the bug: '' == '' is a match.

    A record with no product_name would have been silently zeroed by any
    identity-less stub -- reducing an arbitrary line rather than none.
    """
    orders = [_order(product_name="", itm_cd="")]
    stub = {"department": "GROCERY", "reasoning": "[RESCUED SHORTFALL]"}
    assert SERVICE._adjust_order(orders, stub, 0.0, "[NETWORK: x]") is False
    assert orders[0]["recommended_quantity"] == 12


def test_no_match_leaves_every_order_alone():
    orders = [_order(), _order(product_name="OTHER", itm_cd="SKU-2")]
    original = {"product_name": "NOT STOCKED HERE", "itm_cd": "SKU-99"}
    assert SERVICE._adjust_order(orders, original, 0.0, "[NETWORK: x]") is False
    assert [o["recommended_quantity"] for o in orders] == [12, 12]


def test_the_counter_is_gated_on_the_adjustment_actually_happening():
    """Source-level, because the increment is what lied, not the helper.

    A future edit could restore `_adjust_order(...)` followed by an
    unconditional `total_orders_reduced += 1` and every unit test above would
    still pass while the plan resumed reporting savings that do not exist.
    """
    with open(SRC, encoding="utf-8") as f:
        tree = ast.parse(f.read(), filename=SRC)

    def _increments_reduced(node):
        return (isinstance(node, ast.AugAssign)
                and getattr(node.target, "attr", None) == "total_orders_reduced")

    guarded, total = 0, 0
    for node in ast.walk(tree):
        if _increments_reduced(node):
            total += 1
    for node in ast.walk(tree):
        if not isinstance(node, ast.If):
            continue
        calls_adjust = any(
            isinstance(n, ast.Call)
            and getattr(n.func, "attr", None) == "_adjust_order"
            for n in ast.walk(node.test))
        if not calls_adjust:
            continue
        guarded += sum(1 for n in ast.walk(node) if _increments_reduced(n))

    assert total, "total_orders_reduced is no longer incremented anywhere"
    assert guarded == total, (
        f"{guarded} of {total} total_orders_reduced increments sit behind an "
        f"`if self._adjust_order(...)`. An unguarded one counts reductions "
        f"that never happened.")
