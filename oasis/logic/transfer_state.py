"""
Transfer State Tracker
======================
Shared state object that tracks all pending, in-transit, and completed
inter-branch transfers across the network.

This module is part of the **consolidated transfer layer** and does NOT
modify any per-store OASIS engine logic.

Usage:
    tracker = TransferStateTracker()
    tracker.register_transfer(TransferRecord(...))
    inbound = tracker.get_inbound_qty("ORG001", "ITM-12345")
"""

import uuid
import logging
from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional, Tuple

logger = logging.getLogger("TransferState")


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

@dataclass
class TransferRecord:
    """Single inter-branch transfer."""
    from_org: str
    to_org: str
    itm_cd: str
    product_name: str
    qty: float
    status: str = "PENDING"        # PENDING | IN_TRANSIT | DELIVERED | CANCELLED
    created_at: datetime = field(default_factory=datetime.now)
    eta_hours: float = 4.0         # estimated hours to deliver (intra-city)
    cost_kes: float = 0.0          # logistics cost
    transfer_id: str = field(default_factory=lambda: str(uuid.uuid4())[:8])
    department: str = ""
    urgency: str = "MEDIUM"        # CRITICAL | HIGH | MEDIUM

    @property
    def is_active(self) -> bool:
        return self.status in ("PENDING", "IN_TRANSIT")


# ---------------------------------------------------------------------------
# Core tracker
# ---------------------------------------------------------------------------

class TransferStateTracker:
    """
    Tracks all active transfers across the store network.
    
    Thread-safe for single-process use (Streamlit). For multi-process,
    back this with SQLite or Redis.
    """

    def __init__(self):
        self._transfers: List[TransferRecord] = []
        self._index_inbound: Dict[Tuple[str, str], float] = {}   # (org, itm) -> qty
        self._index_outbound: Dict[Tuple[str, str], float] = {}  # (org, itm) -> qty

    # ------------------------------------------------------------------
    # Write operations
    # ------------------------------------------------------------------

    def register_transfer(self, transfer: TransferRecord) -> str:
        """Register a new transfer and update indices."""
        self._transfers.append(transfer)
        self._rebuild_indices()
        logger.info(
            f"Transfer {transfer.transfer_id}: "
            f"{transfer.qty} × {transfer.product_name} "
            f"{transfer.from_org} → {transfer.to_org} ({transfer.status})"
        )
        return transfer.transfer_id

    def register_batch(self, transfers: List[TransferRecord]) -> List[str]:
        """Register multiple transfers at once (more efficient)."""
        ids = []
        for t in transfers:
            self._transfers.append(t)
            ids.append(t.transfer_id)
        self._rebuild_indices()
        logger.info(f"Registered {len(transfers)} transfers in batch.")
        return ids

    def complete_transfer(self, transfer_id: str):
        """Mark a transfer as DELIVERED."""
        for t in self._transfers:
            if t.transfer_id == transfer_id:
                t.status = "DELIVERED"
                break
        self._rebuild_indices()

    def cancel_transfer(self, transfer_id: str):
        """Cancel a pending/in-transit transfer."""
        for t in self._transfers:
            if t.transfer_id == transfer_id and t.is_active:
                t.status = "CANCELLED"
                break
        self._rebuild_indices()

    def clear_all(self):
        """Reset all transfers (used between simulation cycles)."""
        self._transfers.clear()
        self._index_inbound.clear()
        self._index_outbound.clear()

    # ------------------------------------------------------------------
    # Read operations (used by consolidated layer)
    # ------------------------------------------------------------------

    def get_inbound_qty(self, org_cd: str, itm_cd: str) -> float:
        """Total active inbound quantity for a store+item."""
        return self._index_inbound.get((org_cd, itm_cd), 0.0)

    def get_outbound_qty(self, org_cd: str, itm_cd: str) -> float:
        """Total active outbound quantity for a store+item."""
        return self._index_outbound.get((org_cd, itm_cd), 0.0)

    def get_inbound_for_store(self, org_cd: str) -> Dict[str, float]:
        """All inbound quantities for a store: {itm_cd: total_qty}."""
        result: Dict[str, float] = {}
        for (org, itm), qty in self._index_inbound.items():
            if org == org_cd:
                result[itm] = qty
        return result

    def get_outbound_for_store(self, org_cd: str) -> Dict[str, float]:
        """All outbound quantities for a store: {itm_cd: total_qty}."""
        result: Dict[str, float] = {}
        for (org, itm), qty in self._index_outbound.items():
            if org == org_cd:
                result[itm] = qty
        return result

    def get_active_transfers(self, org_cd: Optional[str] = None) -> List[TransferRecord]:
        """Get all active transfers, optionally filtered by store (inbound or outbound)."""
        active = [t for t in self._transfers if t.is_active]
        if org_cd:
            active = [t for t in active if t.to_org == org_cd or t.from_org == org_cd]
        return active

    def get_all_transfers(self) -> List[TransferRecord]:
        """Get all transfers (including completed/cancelled)."""
        return list(self._transfers)

    def get_network_summary(self) -> Dict[str, int]:
        """Quick summary of transfer activity."""
        active = [t for t in self._transfers if t.is_active]
        return {
            "total_registered": len(self._transfers),
            "active": len(active),
            "pending": sum(1 for t in active if t.status == "PENDING"),
            "in_transit": sum(1 for t in active if t.status == "IN_TRANSIT"),
            "delivered": sum(1 for t in self._transfers if t.status == "DELIVERED"),
            "cancelled": sum(1 for t in self._transfers if t.status == "CANCELLED"),
            "total_units_in_transit": sum(t.qty for t in active),
            "total_value_kes": sum(t.cost_kes for t in active),
        }

    # ------------------------------------------------------------------
    # Persistence
    # ------------------------------------------------------------------

    def save_to_file(self, file_path: str):
        """Save all transfers to a JSON file."""
        import json
        data = []
        for t in self._transfers:
            data.append({
                "transfer_id": t.transfer_id,
                "from_org": t.from_org,
                "to_org": t.to_org,
                "itm_cd": t.itm_cd,
                "product_name": t.product_name,
                "qty": t.qty,
                "status": t.status,
                "created_at": t.created_at.isoformat(),
                "eta_hours": t.eta_hours,
                "cost_kes": t.cost_kes,
                "department": t.department,
                "urgency": t.urgency
            })
        try:
            with open(file_path, "w") as f:
                json.dump(data, f, indent=2)
            logger.info(f"Saved {len(data)} transfers to {file_path}")
        except Exception as e:
            logger.error(f"Failed to save transfers to {file_path}: {e}")

    def load_from_file(self, file_path: str):
        """Load transfers from a JSON file."""
        import json
        import os
        if not os.path.exists(file_path):
            return
        try:
            with open(file_path, "r") as f:
                data = json.load(f)
            self._transfers.clear()
            for d in data:
                t = TransferRecord(
                    from_org=d["from_org"],
                    to_org=d["to_org"],
                    itm_cd=d["itm_cd"],
                    product_name=d["product_name"],
                    qty=d["qty"],
                    status=d["status"],
                    created_at=datetime.fromisoformat(d["created_at"]),
                    eta_hours=d["eta_hours"],
                    cost_kes=d["cost_kes"],
                    transfer_id=d["transfer_id"],
                    department=d.get("department", ""),
                    urgency=d.get("urgency", "MEDIUM")
                )
                self._transfers.append(t)
            self._rebuild_indices()
            logger.info(f"Loaded {len(self._transfers)} transfers from {file_path}")
        except Exception as e:
            logger.error(f"Failed to load transfers from {file_path}: {e}")

    # ------------------------------------------------------------------
    # Internal
    # ------------------------------------------------------------------

    def _rebuild_indices(self):
        """Rebuild fast-lookup indices from active transfers."""
        self._index_inbound.clear()
        self._index_outbound.clear()
        for t in self._transfers:
            if not t.is_active:
                continue
            # Inbound to recipient
            key_in = (t.to_org, t.itm_cd)
            self._index_inbound[key_in] = self._index_inbound.get(key_in, 0.0) + t.qty
            # Outbound from donor
            key_out = (t.from_org, t.itm_cd)
            self._index_outbound[key_out] = self._index_outbound.get(key_out, 0.0) + t.qty
