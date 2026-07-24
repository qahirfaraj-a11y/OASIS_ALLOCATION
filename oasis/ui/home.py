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
     "desc": "Ordering, approvals, transfers, allocation, suppliers."},
    {"key": "command", "title": "Command Center", "icon": "🔮", "port": 8501,
     "desc": "Multi-tab operations dashboard: live sales, stock, smart ordering."},
    {"key": "intel", "title": "Intelligence Console", "icon": "⚡", "port": 8510,
     "desc": "Pulse, velocity alerts, stock review, baskets, executive ROI."},
    {"key": "stgat", "title": "Market Intelligence", "icon": "📈", "port": 8505,
     "desc": "ST-GAT network analysis and advisory transfers."},
    {"key": "hub", "title": "Cloud Hub", "icon": "☁", "port": 8700,
     "desc": "Online expansion: supplier portal, movement ingestion, consent.",
     "portal_path": "/portal-app/"},
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
    """Small sidebar block linking the current console to its siblings.

    Carries the suite SSO token (?sid=) so hopping consoles keeps you signed
    in — one login for the whole suite (audit F1).
    """
    try:
        from .auth import current_sid
        sid = current_sid(st)
        parts = []
        for c in CONSOLES:
            if c["key"] == current_key:
                continue
            url = "http://localhost:%d" % c["port"]
            if sid and c["key"] != "hub":     # hub has its own supplier auth
                url += "/?sid=%s" % sid
            parts.append("[%s %s](%s)" % (c["icon"], c["title"].split()[0], url))
        st.sidebar.markdown("<small>O.A.S.I.S. suite: " + " · ".join(parts) + "</small>",
                            unsafe_allow_html=True)
    except Exception:
        pass


def _version_str(project_root: str) -> str:
    """Read VERSION for the AUDITED LOGIC_ENGINE_V<n> spec tag."""
    try:
        with open(os.path.join(project_root, "VERSION"), "r", encoding="utf-8") as f:
            return f.read().strip()
    except OSError:
        return "0.0"


def render_home_page(st, project_root: str) -> None:
    """The launcher page (thin; all logic in the helpers above)."""
    from ..logic.branding import load_branding
    from ..logic.install_profile import is_multi_store, load_profile
    from ..logic.license_manager import KNOWN_MODULES, OfflineLicenseManager
    from .onboarding import render_onboarding, demo_badge, render_reset_control

    # First-run gate: a fresh install picks its data source before anything else,
    # rather than silently showing a mock (or broken/empty) store.
    if render_onboarding(st, project_root):
        return

    b = load_branding()
    _profile = load_profile()
    _is_multi = is_multi_store()
    logo = ""
    if b.logo_data_uri:
        logo = f'<img src="{b.logo_data_uri}" style="height:56px;margin-right:16px;vertical-align:middle;">'
    elif b.logo:
        logo = f'<img src="{b.logo}" style="height:56px;margin-right:16px;vertical-align:middle;">'
    subtitle = b.welcome_message or ("Algorithmic Retail Systems & Logic")
    subline = f" · {b.tenant_name}" if b.tenant_name != b.product_name else ""
    # Top spec tag — brand's "[ AUDITED LOGIC_ENGINE_V3.1 ]" motif
    from . import components as C
    from ..logic.license_manager import allowed_modules
    _sys_status = "PEAK" if OfflineLicenseManager().status("core")["mode"] != "locked" else "LOCKED"
    C.spec_tag(f"AUDITED LOGIC_ENGINE_V{_version_str(project_root)}", hot=True,
               st_module=st)
    st.markdown(
        f"<div style='display:flex;align-items:center;margin-top:8px;"
        f"margin-bottom:6px'>"
        f"{logo}<h1 style='margin:0'>{b.product_name}{subline}</h1></div>"
        f"<p style='color:var(--oasis-text-3);margin-top:2px;"
        f"font-family:var(--oasis-mono);letter-spacing:0.14em;"
        f"text-transform:uppercase;font-size:0.78em;'>"
        f"{subtitle}</p>",
        unsafe_allow_html=True)
    # Status ticker: the brand's signature "● SYSTEM READINESS: PEAK" line
    _snap = system_snapshot(
        os.getenv("OASIS_DB_PATH",
                  os.path.join(project_root, "oasis", "data",
                               "rhapta_pos.db")))
    _mods = allowed_modules()
    C.status_ticker([
        {"label": "System Readiness", "value": _sys_status},
        {"label": "Nodal Integrity",
         "value": "SECURE" if _snap["db_exists"] else "PENDING"},
        {"label": "Modules Live", "value": f"{len(_mods)}/5"},
    ], st_module=st)
    # Sample-data banner (only when the active store is the built-in demo).
    demo_badge(st)

    # Profile badge — tells the operator (and any support tech) which topology
    # is active and where the DB lives.
    if _profile:
        _label = "Multi-store network" if _is_multi else "Single store"
        st.caption(f"▸ {_label} · DB: `{_profile.get('db_path', '—')}`")

    # ── launch controls ─────────────────────────────────────────────────
    from ..logic.console_launcher import start_console, stop_all as _stop_all
    if "_launched_pids" not in st.session_state:
        st.session_state["_launched_pids"] = {}
    _pids = st.session_state["_launched_pids"]
    _db = os.getenv("OASIS_DB_PATH",
                    os.path.join(project_root, "oasis", "data", "rhapta_pos.db"))

    l_col, r_col = st.columns([1, 1])
    with l_col:
        if st.button("▶  Start All Consoles", use_container_width=True, type="primary"):
            for c in CONSOLES:
                pid = start_console(c["key"], db_path=_db)
                if pid:
                    _pids[c["key"]] = pid
            import time
            time.sleep(3)  # let processes bind ports
            st.rerun()
    with r_col:
        if st.button("■  Stop All", use_container_width=True):
            _stop_all(_pids)
            _pids.clear()
            import time
            time.sleep(1)
            st.rerun()

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
                # Hub links to the portal app, not the raw API root
                url = c["url"]
                if c.get("portal_path"):
                    url += c["portal_path"]
                st.link_button("Open", url, use_container_width=True)
            else:
                if st.button("Launch", key=f"launch_{c['key']}",
                             use_container_width=True):
                    pid = start_console(c["key"], db_path=_db)
                    if pid:
                        _pids[c["key"]] = pid
                    import time
                    time.sleep(3)
                    st.rerun()

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
        if mgr.status("core")["mode"] != "licensed":
            from ..logic.license_manager import render_license_activation
            with st.expander("🔑 Activate a license"):
                render_license_activation(st)

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

    st.divider()
    render_reset_control(st)
