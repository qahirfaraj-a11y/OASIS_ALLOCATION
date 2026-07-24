"""
First-run onboarding wizard (Streamlit) — the OASIS front door for a fresh install.

Shown by the Home launcher when no data source has been chosen yet. Instead of
silently seeding mock data, it asks the operator where their data comes from and
sets the store up accordingly. See oasis.logic.onboarding for the state/actions.
"""


from ..logic import onboarding as OB


def render_onboarding(st, project_root: str) -> bool:
    """Render the first-run wizard. Returns True (caller should stop) if the
    install is not yet onboarded; False if already onboarded (render nothing)."""
    if OB.is_onboarded():
        return False

    from ..logic.branding import load_branding
    from . import components as C
    b = load_branding()

    C.spec_tag("FIRST-RUN SETUP", hot=True, st_module=st)
    st.markdown(
        f"<h1 style='margin:6px 0 0'>Welcome to {b.product_name}</h1>"
        "<p style='color:var(--oasis-text-3);font-family:var(--oasis-mono);"
        "letter-spacing:.12em;text-transform:uppercase;font-size:.78em'>"
        "Choose how to begin — you can change this later</p>",
        unsafe_allow_html=True)
    st.write("")

    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown("### 🧪 Explore with sample data")
        st.caption("Spin up a realistic **sample store** (≈35 SKUs across 7 "
                   "departments) so you can tour every console right now. "
                   "Clearly badged as sample — nothing real.")
        if st.button("Load sample store", use_container_width=True, key="ob_demo"):
            with st.spinner("Building the sample store…"):
                s = OB.apply_demo()
            st.success(f"Sample store ready — {s.get('items', 0)} SKUs, "
                       f"{s.get('departments', 0)} departments.")
            st.rerun()

    with c2:
        st.markdown("### 📭 Start fresh")
        st.caption("Create an **empty store** with the full OASIS schema and no "
                   "products — a clean console you build up from your own data.")
        name = st.text_input("Store name", value=b.tenant_name or "My Store",
                             key="ob_empty_name")
        if st.button("Create empty store", use_container_width=True, key="ob_empty"):
            with st.spinner("Creating your store…"):
                OB.apply_empty(store_name=name or "My Store")
            st.success("Empty store created. Connect or import data to begin.")
            st.rerun()

    with c3:
        st.markdown("### 🔌 Connect a POS")
        st.caption("Point OASIS at an **existing POS/ERP database** (read-only). "
                   "Works out of the box with the OASIS canonical schema.")
        url = st.text_input("POS database URL",
                            placeholder="postgresql://user:pass@host/db  or  "
                                        "sqlite:///path/to/pos.db",
                            key="ob_conn_url")
        if st.button("Test & connect", use_container_width=True, key="ob_conn"):
            if not url.strip():
                st.warning("Enter a database URL first.")
            else:
                with st.spinner("Testing connection…"):
                    res = OB.apply_connect(url.strip())
                if res["ok"]:
                    st.success(res["detail"])
                    st.info("Set `OASIS_POS_DB_URL` to this URL (or in your "
                            ".env) so every console reads it, then reload.")
                    st.rerun()
                else:
                    st.error(res["detail"])

    with c4:
        st.markdown("### 🏢 Build from Catalogue")
        st.caption("Initialize a real database from your provided **product catalog** and optionally seed demand from historical sales files.")
        prof = st.selectbox("Store topology", ["single", "multi"], key="ob_profile")
        if st.button("Initialize DB", use_container_width=True, key="ob_init"):
            with st.spinner("Building database from catalog..."):
                res = OB.apply_init(prof)
            if "catalog_error" in res:
                st.error(f"Catalog error: {res['catalog_error']}")
            else:
                st.success(f"Initialized ({res['profile']} store). Catalog: {res.get('catalog', '0 SKUs')}")
                st.rerun()

    st.divider()
    st.caption("Odoo users: install the **OASIS Connector** in Odoo to stream "
               "your data — see the connector's INTEGRATION guide. Not sure? "
               "Start with the sample store; it commits to nothing.")
    return True


def demo_badge(st) -> None:
    """A persistent 'SAMPLE DATA' banner shown across the app when the active
    store is the built-in demo, so it never masquerades as real."""
    if OB.is_demo():
        st.markdown(
            "<div style='background:#3a2a00;border:1px solid #6b5200;"
            "color:#ffcf5c;border-radius:8px;padding:8px 14px;margin:6px 0;"
            "font-family:var(--oasis-mono,monospace);font-size:.8em;"
            "letter-spacing:.08em'>◆ SAMPLE DATA — this is the built-in demo "
            "store, not your live data. Reset from Home to onboard real data."
            "</div>", unsafe_allow_html=True)


def data_source_badge(st) -> None:
    """One-line provenance chip for EVERY console header.

    The honesty rule (lifecycle audit C1/G1): a surface must never imply data
    it doesn't have. Sample data gets the loud amber banner; real sources get a
    quiet chip naming what the console is actually looking at — which also
    covers the consoles when embedded inside Odoo (they show the OASIS store,
    not Odoo's own DB, until the adapter lands).
    """
    ob = OB.load_onboarding()
    src = ob.get("source")
    if src == "demo":
        demo_badge(st)
        return
    if src == "empty":
        label = f"DATA: {ob.get('store_name', 'My Store')} (your own store)"
    elif src == "connect":
        url = str(ob.get("db_url", ""))
        host = url.split("@")[-1].split("/")[0] if "@" in url else (
            url.rsplit("/", 1)[-1] or "external POS")
        label = f"DATA: connected POS ({host})"
    else:
        label = "DATA: not onboarded — run first-launch setup from Home"
    st.markdown(
        "<div style='color:var(--oasis-text-3,#8792ad);"
        "font-family:var(--oasis-mono,monospace);font-size:.72em;"
        f"letter-spacing:.1em;margin:2px 0 6px'>▸ {label}</div>",
        unsafe_allow_html=True)


def render_reset_control(st) -> None:
    """A small control (for Home) to re-run onboarding — switch demo→real, etc."""
    with st.expander("⚙︎ Change data source / re-run setup"):
        cur = OB.load_onboarding()
        st.caption(f"Current source: **{cur.get('source', 'none')}**"
                   + (f" · {cur.get('store_name')}" if cur.get("store_name") else ""))
        st.write("Re-running setup lets you switch between the sample store, an "
                 "empty store, or a connected POS. It does not delete a "
                 "connected POS's data (OASIS only reads it).")
        if st.button("Re-run first-run setup", key="ob_reset"):
            OB.reset_onboarding()
            st.rerun()
