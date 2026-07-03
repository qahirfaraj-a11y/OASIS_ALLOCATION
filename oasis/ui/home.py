"""
O.A.S.I.S. Home — the suite launcher.

One front door for the whole platform: shows every console with a live/offline
badge and an Open link, the license posture per module, and a system snapshot
(store DB, today's activity, latest backup and value report). The consoles stay
separate apps (by design); this page ties them into one product.

Pure helpers (console_cards / port_live / system_snapshot / latest_file) are
unit-tested; render_home_page is the thin Streamlit layer. suite_links() renders
the small cross-console sidebar so every console links to its siblings.
"""

from __future__ import annotations

import os
import socket
from typing import List, Optional

#: the shipped surfaces — single source of truth for the launcher & suite bar
CONSOLES = [
    {"key": "ops", "title": "Operations Console", "icon": "◎", "port": 8500,
     "launcher": "run_oasis_live.bat",
     "desc": "Ordering, approvals, transfers, allocation, suppliers."},
    {"key": "command", "title": "Command Center", "icon": "🔮", "port": 8501,
     "launcher": "run_command_center_live.bat",
     "desc": "Multi-tab operations dashboard: live sales, stock, smart ordering."},
    {"key": "intel", "title": "Intelligence Console", "icon": "⚡", "port": 8510,
     "launcher": "run_oasis_intel_live.bat",
     "desc": "Pulse, velocity alerts, stock review, baskets, executive ROI."},
    {"key": "stgat", "title": "Market Intelligence", "icon": "📈", "port": 8502,
     "launcher": "run_market_intelligence_tool.bat",
     "desc": "ST-GAT network analysis and advisory transfers."},
]


def port_live(port: int, host: str = "127.0.0.1", timeout: float = 0.4) -> bool:
    """True if something is listening on the port (console up)."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def console_cards(check=port_live) -> List[dict]:
    """CONSOLES enriched with live status + url (check injectable for tests)."""
    out = []
    for c in CONSOLES:
        live = bool(check(c["port"]))
        out.append({**c, "live": live, "url": f"http://localhost:{c['port']}"})
    return out


def system_snapshot(db_path: str) -> dict:
    """Light, fail-soft store stats for the home panel."""
    out = {"db": os.path.basename(db_path or ""), "db_exists": False,
           "bills_today": 0, "stockouts": 0, "skus": 0}
    if not db_path or not os.path.exists(db_path):
        return out
    out["db_exists"] = True
    try:
        import sqlite3
        conn = sqlite3.connect(db_path, timeout=5.0)
        try:
            out["bills_today"] = int(conn.execute(
                "SELECT COUNT(*) FROM POS_SALES_HDR "
                "WHERE BILL_DT = date('now','localtime')").fetchone()[0] or 0)
            out["skus"] = int(conn.execute(
                "SELECT COUNT(*) FROM STOCK_MASTER").fetchone()[0] or 0)
            out["stockouts"] = int(conn.execute(
                "SELECT COUNT(*) FROM STOCK_MASTER WHERE SM_QTY < 1").fetchone()[0] or 0)
        finally:
            conn.close()
    except Exception:
        pass
    return out


def latest_file(directory: str, suffix: str = "") -> Optional[str]:
    """Newest file in directory (optionally filtered by suffix), or None."""
    try:
        entries = [os.path.join(directory, f) for f in os.listdir(directory)
                   if f.endswith(suffix)]
        return max(entries, key=os.path.getmtime) if entries else None
    except OSError:
        return None


def suite_links(st, current_key: str) -> None:
    """Small sidebar block linking the current console to its siblings."""
    try:
        parts = []
        for c in CONSOLES:
            if c["key"] == current_key:
                continue
            url = "http://localhost:%d" % c["port"]
            parts.append("[%s %s](%s)" % (c["icon"], c["title"].split()[0], url))
        st.sidebar.markdown("<small>O.A.S.I.S. suite: " + " · ".join(parts) + "</small>",
                            unsafe_allow_html=True)
    except Exception:
        pass


def render_home_page(st, project_root: str) -> None:
    """The launcher page (thin; all logic in the helpers above)."""
    from ..logic.license_manager import KNOWN_MODULES, OfflineLicenseManager

    st.markdown(
        "<h1 style='margin-bottom:0'>O.A.S.I.S.</h1>"
        "<p style='color:#888;margin-top:2px'>Omni-channel Autonomous Stock "
        "Intelligence System — suite launcher</p>", unsafe_allow_html=True)

    # ── consoles ────────────────────────────────────────────────────────
    cards = console_cards()
    cols = st.columns(len(cards))
    for col, c in zip(cols, cards):
        with col:
            badge = ("<span style='color:#2e7d32'>● LIVE</span>" if c["live"]
                     else "<span style='color:#9e9e9e'>○ offline</span>")
            st.markdown(
                f"<div style='border:1px solid #333;border-radius:10px;padding:14px;min-height:170px'>"
                f"<div style='font-size:22px'>{c['icon']} <b>{c['title']}</b></div>"
                f"<div style='color:#888;font-size:13px;margin:6px 0'>{c['desc']}</div>"
                f"<div>{badge} &nbsp;·&nbsp; :{c['port']}</div></div>",
                unsafe_allow_html=True)
            if c["live"]:
                st.link_button("Open", c["url"], use_container_width=True)
            else:
                st.caption(f"Start with `{c['launcher']}`")

    st.divider()
    left, right = st.columns(2)

    # ── license posture ─────────────────────────────────────────────────
    with left:
        st.markdown("#### License")
        mgr = OfflineLicenseManager()
        rows = [{"Module": m, "Status": mgr.status(m)["mode"]}
                for m in KNOWN_MODULES]
        import pandas as pd
        st.dataframe(pd.DataFrame(rows), hide_index=True, use_container_width=True)

    # ── system snapshot ─────────────────────────────────────────────────
    with right:
        st.markdown("#### System")
        db_path = os.getenv("OASIS_DB_PATH",
                            os.path.join(project_root, "oasis", "data", "rhapta_pos.db"))
        snap = system_snapshot(db_path)
        st.metric("Store DB", snap["db"] or "—",
                  "connected" if snap["db_exists"] else "missing")
        c1, c2, c3 = st.columns(3)
        c1.metric("Bills today", f"{snap['bills_today']:,}")
        c2.metric("SKUs", f"{snap['skus']:,}")
        c3.metric("Stockouts", f"{snap['stockouts']:,}")

        backup = latest_file(os.path.join(os.path.dirname(db_path), "backups"), ".db")
        report = latest_file(os.path.join(project_root, "reports"), ".md")
        st.caption(f"Latest backup: `{os.path.basename(backup) if backup else 'none — run --mode backup'}`")
        if report:
            with open(report, "r", encoding="utf-8") as f:
                st.download_button(f"⬇ {os.path.basename(report)}", f.read(),
                                   file_name=os.path.basename(report))
        else:
            st.caption("No value report yet — run `--mode value-report`.")
