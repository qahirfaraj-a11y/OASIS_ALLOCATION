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
    #: The stores whose ordering run planned this transfer ("ORG001" or
    #: "ORG001,ORG002"). A run replaces only its own records in the shared
    #: registry — keyed on who PLANNED it, not on where it goes, because one
    #: store's run also plans pushes into other stores.
    planned_for: str = ""

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

    def save_to_file(self, file_path: str, replace_orgs: Optional[set] = None):
        """Save transfers to the shared registry file, safely.

        ONE REGISTRY, MANY WRITERS. Every surface (desktop, web, both consoles)
        now plans into the same network_registry.json, from separate processes.
        The file used to be rewritten in place with no lock: two runs at once
        kept only the last one's plan, a reader landing mid-write saw truncated
        JSON, and a plan for one store erased every other store's planned
        transfers. So:

        * the write is ATOMIC — a temp file in the same folder, then
          os.replace, so a reader sees the old file or the new one, never half;
        * the read-merge-write happens under a cross-process LOCK;
        * with ``replace_orgs`` (the stores a run planned for) only THAT slice
          is replaced — records a run for any of those stores planned
          (``planned_for``) — and every other store's plan is kept. Without it
          the whole file is replaced, as before.
        """
        import json
        scope = set(replace_orgs or ())
        tag = ",".join(sorted(str(o) for o in scope))
        mine = []
        for t in self._transfers:
            if replace_orgs is not None and not t.planned_for:
                t.planned_for = tag
            mine.append(self._record(t))

        def planned_by_scope(d: dict) -> bool:
            owners = {o for o in str(d.get("planned_for") or "").split(",") if o}
            # records from before planned_for existed: fall back to recipient
            return bool(owners & scope) if owners else d.get("to_org") in scope

        try:
            with _registry_lock(file_path):
                data = mine
                if replace_orgs is not None:
                    keep = [d for d in _read_registry(file_path) if not planned_by_scope(d)]
                    data = keep + mine
                _atomic_write(file_path, json.dumps(data, indent=2))
            logger.info(f"Saved {len(mine)} transfers to {file_path}"
                        + (f" ({len(data) - len(mine)} kept for other stores)" if replace_orgs is not None else ""))
        except Exception as e:
            logger.error(f"Failed to save transfers to {file_path}: {e}")

    @staticmethod
    def _record(t: "TransferRecord") -> dict:
        return {
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
            "urgency": t.urgency,
            "planned_for": t.planned_for,
        }

    def load_from_file(self, file_path: str):
        """Load transfers from a JSON file."""
        import json
        import os
        if not os.path.exists(file_path):
            return
        try:
            with open(file_path, "r", encoding="utf-8") as f:
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
                    urgency=d.get("urgency", "MEDIUM"),
                    planned_for=d.get("planned_for", ""),
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


# ---------------------------------------------------------------------------
# The shared registry file: atomic writes under a cross-process lock
# ---------------------------------------------------------------------------

#: How long a writer waits for another to finish before giving up (seconds).
LOCK_TIMEOUT_S = 30.0


class _registry_lock:
    """Exclusive lock on ``<registry>.lock``, across processes and threads.

    The OS lock (msvcrt on Windows, fcntl elsewhere) is released by the kernel
    if the holder dies, so a crashed writer cannot wedge the registry.
    """

    def __init__(self, file_path: str):
        self.path = file_path + ".lock"
        self._fh = None

    def __enter__(self):
        import os
        import time
        os.makedirs(os.path.dirname(os.path.abspath(self.path)) or ".", exist_ok=True)
        self._fh = open(self.path, "a+b")
        deadline = time.monotonic() + LOCK_TIMEOUT_S
        while True:
            try:
                if os.name == "nt":
                    import msvcrt
                    self._fh.seek(0)
                    msvcrt.locking(self._fh.fileno(), msvcrt.LK_NBLCK, 1)
                else:
                    import fcntl
                    fcntl.flock(self._fh.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
                return self
            except OSError:
                if time.monotonic() > deadline:
                    self._fh.close()
                    raise TimeoutError(f"registry busy for {LOCK_TIMEOUT_S:.0f}s: {self.path}")
                time.sleep(0.02)

    def __exit__(self, *exc):
        import os
        try:
            if os.name == "nt":
                import msvcrt
                self._fh.seek(0)
                msvcrt.locking(self._fh.fileno(), msvcrt.LK_UNLCK, 1)
            else:
                import fcntl
                fcntl.flock(self._fh.fileno(), fcntl.LOCK_UN)
        finally:
            self._fh.close()
        return False


def _read_registry(file_path: str) -> List[dict]:
    """The registry's records, or [] if it is absent or unreadable."""
    import json
    import os
    if not os.path.exists(file_path):
        return []
    try:
        with open(file_path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return [d for d in data if isinstance(d, dict)] if isinstance(data, list) else []
    except (OSError, ValueError) as e:
        logger.error(f"Registry {file_path} unreadable ({e}); treating as empty")
        return []


def _atomic_write(file_path: str, text: str) -> None:
    """Write via a temp file in the same folder and os.replace: never half a file."""
    import os
    import tempfile
    folder = os.path.dirname(os.path.abspath(file_path)) or "."
    fd, tmp = tempfile.mkstemp(prefix=".registry-", suffix=".tmp", dir=folder)
    try:
        with os.fdopen(fd, "w", encoding="utf-8") as f:
            f.write(text)
            f.flush()
            os.fsync(f.fileno())
        # os.replace can be refused on Windows while a reader holds the file
        # open; the read is short, so retry briefly rather than fail the plan.
        import time
        for attempt in range(50):
            try:
                os.replace(tmp, file_path)
                return
            except PermissionError:
                if attempt == 49:
                    raise
                time.sleep(0.02)
    except BaseException:
        try:
            os.remove(tmp)
        except OSError:
            pass
        raise
