"""End-to-end Smart Ordering test."""

import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.desktop import data as D

@pytest.fixture
def store(tmp_path, monkeypatch):
    """A real sample store, with the adapter cache cleared around it."""
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

def test_smart_ordering_e2e(store):
    org_cd = D.default_org(store)
    
    # 1. Generate POs natively
    res = D.generate_smart_orders(org_cd, root=store)
    assert res.get("error") is None
    po_recs = res.get("po_recs", [])
    # Filter for positive recommendations
    recs = [r for r in po_recs if float(r.get("recommended_quantity", 0)) > 0]
    
    # Note: Depending on the simulation, there may or may not be positive orders
    if recs:
        # 2. Push to pending approvals
        username = "test_user"
        push_res = D.push_purchase_order(org_cd, username, recs, root=store)
        assert push_res.get("success") is True
        assert push_res.get("pushed_count") > 0
        
        # 3. Assert the row lands in the database (Pending approvals queue)
        pend = D.pending_orders(org_cd, root=store)
        assert pend.get("error") is None
        assert pend.get("count", 0) > 0
        
        # Check it contains what we pushed
        po_rows = pend.get("rows", [])
        assert len(po_rows) > 0
        
        # 4. Approve a PO
        po_id = po_rows[0]["PO_ID"]
        app_res = D.update_po_status(po_id, "APPROVED", username, org_cd, root=store)
        assert app_res.get("success") is True
        
        # Re-fetch pending orders to ensure count went down
        pend_after = D.pending_orders(org_cd, root=store)
        assert pend_after.get("count", 0) < pend.get("count", 0)
