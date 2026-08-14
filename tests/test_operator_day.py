"""The operator's day, end to end, against a real sample store.

Every other test proves a piece works. This one proves the pieces connect in
the order a person actually uses them:

    look at the store -> generate orders -> push them -> approve one
    -> see the queue shrink -> back up -> restore

A green unit suite does not imply this loop closes. "The view builds a control
tree" and "an operator can complete a day" are different claims, and only the
second one is what a client buys.

Runs entirely inside tmp_path — never the developer's own store (see the
autouse guard in conftest.py).
"""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D


@pytest.fixture
def store(tmp_path, monkeypatch):
    """A real sample store, adapter cache cleared around it.

    Deliberately opts OUT of conftest's small-catalogue caps. Those exist so
    shape assertions do not pay for 4,000 SKUs, and they are right for that —
    but this test asserts the ordering ENGINE recommends something, and on 150
    SKUs with 7 days of history it correctly recommends nothing. Capped, this
    test would skip forever and quietly assert nothing at all.
    """
    monkeypatch.delenv("OASIS_DEMO_MAX_SKUS", raising=False)
    monkeypatch.delenv("OASIS_DEMO_HISTORY_DAYS", raising=False)
    (tmp_path / "oasis" / "data").mkdir(parents=True)
    monkeypatch.setenv("OASIS_DB_PATH",
                       str(tmp_path / "oasis" / "data" / "store.db"))
    monkeypatch.delenv("OASIS_DB_URL", raising=False)
    monkeypatch.delenv("OASIS_POS_DB_URL", raising=False)
    D.reset_adapter()
    from oasis.logic import onboarding as OB
    OB.apply_demo(root=str(tmp_path))
    D.reset_adapter()
    yield str(tmp_path)
    D.reset_adapter()


def _org(root):
    stores = D.list_stores(root)
    assert stores, "onboarding produced no store — the day cannot start"
    return stores[0]["org_cd"]


def test_the_operator_can_complete_a_days_ordering_cycle(store):
    """Generate -> push -> approve, checking the queue moves at each step."""
    org = _org(store)

    # 1. Morning: what does the store look like?
    stock = D.stock_overview(org, store)
    assert not stock.get("error"), f"cannot read stock: {stock.get('error')}"
    assert stock["skus"] > 0, "a store with no SKUs gives the operator nothing to do"

    before = D.pending_orders(org, store)
    assert not before.get("error"), f"cannot read the order book: {before.get('error')}"

    # 2. Generate. This is the paid capability and the heart of the product.
    res = D.generate_smart_orders(org, root=store)
    assert not res.get("error"), f"order generation failed: {res.get('error')}"
    recs = [r for r in res.get("po_recs", [])
            if float(r.get("recommended_quantity", 0) or 0) > 0]
    # NOT a skip. If the engine recommends nothing on a full sample store, the
    # product does not work and this test must say so.
    assert recs, (
        "ordering engine recommended nothing on a full sample store — "
        f"{len(res.get('po_recs', []))} rows returned, none with quantity > 0"
    )

    # 3. Push to the approval queue.
    push = D.push_purchase_order(org, "test_operator", recs, store)
    assert push.get("success"), f"push failed: {push}"
    assert push["pushed_count"] > 0

    after_push = D.pending_orders(org, store)
    assert after_push["count"] > before["count"], (
        "pushed POs did not appear in the pending queue — the operator would "
        "generate orders into a void"
    )

    # 4. Approve one, as the approver would.
    po_id = after_push["rows"][0].get("PO_ID")
    assert po_id is not None, "a pending PO with no PO_ID cannot be approved"
    upd = D.update_po_status(po_id, "APPROVED", "test_operator", org, root=store)
    assert not upd.get("error"), f"approval failed: {upd}"

    # 5. The queue must actually shrink. An approval that does not clear the
    #    item leaves the operator approving the same PO forever.
    after_approve = D.pending_orders(org, store)
    assert after_approve["count"] == after_push["count"] - 1, (
        f"approved PO #{po_id} is still pending "
        f"({after_push['count']} -> {after_approve['count']})"
    )


def test_the_operator_can_back_up_and_restore_their_store(store):
    """The worst-day path. Verified by round-tripping real data."""
    from oasis.logic.backup_util import (backup_db, list_backups,
                                         resolve_backup, restore_db)
    org = _org(store)
    db_path = os.path.join(store, "oasis", "data", "store.db")
    assert os.path.exists(db_path)

    baseline = D.stock_overview(org, store)["skus"]

    res = backup_db(db_path)
    assert os.path.exists(res["backup"])

    entries = list_backups(db_path)
    assert entries, "backup taken but list_backups cannot see it"
    assert entries[0]["index"] == 1

    # an operator types the NUMBER they were shown, not a path
    assert resolve_backup(db_path, "1") == entries[0]["path"]

    D.reset_adapter()          # release the file handle before restoring
    out = restore_db(db_path, resolve_backup(db_path, "1"))
    assert out["restored"] == db_path
    assert out["previous_saved_as"], "restore must keep the pre-restore copy"

    D.reset_adapter()
    assert D.stock_overview(org, store)["skus"] == baseline, (
        "the store did not survive a backup/restore round trip"
    )


def test_a_restore_reference_that_does_not_exist_is_refused(store):
    """Guessing a number must not silently restore the wrong backup."""
    from oasis.logic.backup_util import backup_db, resolve_backup
    db_path = os.path.join(store, "oasis", "data", "store.db")
    backup_db(db_path)
    with pytest.raises(FileNotFoundError):
        resolve_backup(db_path, "99")


def test_a_second_day_does_not_re_order_what_is_already_on_order(store):
    """Continuous operation: day 2 must not duplicate day 1.

    The engine subtracts on_order_qty (PENDING + APPROVED POs) from the net
    requirement. Without that, running ordering every morning would re-order
    everything still in transit and compound the position daily — the single
    most expensive way a replenishment system can fail in production.
    """
    org = _org(store)

    day1 = D.generate_smart_orders(org, root=store)
    assert not day1.get("error"), day1.get("error")
    recs1 = {r["item_code"]: float(r["recommended_quantity"])
             for r in day1.get("po_recs", [])
             if r.get("item_code") and float(r.get("recommended_quantity", 0) or 0) > 0}
    assert recs1, "no orders generated on day 1 — cannot test day 2"

    pushed = D.push_purchase_order(
        org, "test_operator",
        [r for r in day1["po_recs"]
         if float(r.get("recommended_quantity", 0) or 0) > 0], store)
    assert pushed.get("success"), pushed

    # Day 2: same store, same stock, but the POs are now outstanding.
    day2 = D.generate_smart_orders(org, root=store)
    assert not day2.get("error"), day2.get("error")
    recs2 = {r["item_code"]: float(r["recommended_quantity"])
             for r in day2.get("po_recs", [])
             if r.get("item_code") and float(r.get("recommended_quantity", 0) or 0) > 0}

    repeated = {sku: (recs1[sku], recs2[sku])
                for sku in recs1 if sku in recs2 and recs2[sku] >= recs1[sku]}
    assert not repeated, (
        "these SKUs were re-ordered at the same or greater quantity while "
        f"already on order — on-order awareness is not applied: "
        f"{dict(list(repeated.items())[:5])}"
    )
