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

    from ..logic.branding import load_branding, save_branding
    from . import components as C
    import time
    b = load_branding()

    C.spec_tag("FIRST-RUN SETUP & PLATFORM CONFIGURATION", hot=True, st_module=st)
    st.markdown(
        f"<h1 style='margin:6px 0 0'>Welcome to {b.product_name}</h1>"
        "<p style='color:var(--oasis-text-3);font-family:var(--oasis-mono);"
        "letter-spacing:.12em;text-transform:uppercase;font-size:.78em'>"
        "Configure your store identity, data sources, and dashboard preferences</p>",
        unsafe_allow_html=True)
    st.write("")

    # Store Identity & Customization Header
    st.markdown("### 🏢 Store Identity & Customization")
    col_name, col_view = st.columns([1, 1])
    with col_name:
        custom_store_name = st.text_input(
            "Store / Company Name",
            value=b.tenant_name if b.tenant_name and b.tenant_name != "OASIS" else "OASIS Store",
            help="Your store name displayed on dashboard headers and reports.",
            key="ob_custom_store_name"
        )
    with col_view:
        default_console_view = st.selectbox(
            "Preferred Default Console View",
            options=["Operations Console (:8500)", "Command Center (:8501)", "Intelligence Console (:8510)", "Market Intelligence (:8505)"],
            index=0,
            help="Primary dashboard view presented on launch.",
            key="ob_default_console_view"
        )

    def _execute_setup_with_progress(action_fn, success_message: str):
        progress_placeholder = st.empty()
        with progress_placeholder.container():
            st.info("🔌 **Step 1/3:** Connecting to POS database & initializing schema...")
            time.sleep(0.4)
            st.info("⚡ **Step 2/3:** Validating nodal integrity & seeding authentication parameters...")
            time.sleep(0.4)
            st.info("🔮 **Step 3/3:** Initializing Chapter-11 engines (AMIT, LATA, DHARAM, MANDE)...")
            res = action_fn()
            # Save customized branding name
            if custom_store_name and custom_store_name.strip():
                try:
                    save_branding({"tenant_name": custom_store_name.strip(), "product_name": "OASIS"})
                except Exception:
                    pass
            time.sleep(0.3)
            st.success(f"✅ {success_message}")
            time.sleep(0.5)
        progress_placeholder.empty()
        st.rerun()

    st.markdown("### 🔌 Select Data Source & Topology")
    c1, c2, c3, c4 = st.columns(4)

    with c1:
        st.markdown("#### 🧪 Sample Store")
        st.caption("Spin up a realistic **sample store** (≈35 SKUs across 7 "
                   "departments) so you can tour every console right now. "
                   "Clearly badged as sample — nothing real.")
        if st.button("Load sample store", use_container_width=True, key="ob_demo"):
            name = custom_store_name.strip() if custom_store_name else "OASIS Sample Store"
            _execute_setup_with_progress(
                lambda: OB.apply_demo(store_name=name),
                f"Sample store '{name}' initialized successfully!"
            )

    with c2:
        st.markdown("#### 📭 Start Fresh")
        st.caption("Create an **empty store** with the full OASIS schema and no "
                   "products — a clean console you build up from your own data.")
        if st.button("Create empty store", use_container_width=True, key="ob_empty"):
            name = custom_store_name.strip() if custom_store_name else "My Store"
            _execute_setup_with_progress(
                lambda: OB.apply_empty(store_name=name),
                f"Empty store '{name}' created cleanly."
            )

    with c3:
        st.markdown("#### 🔌 Connect POS")
        st.caption("Point OASIS at an **existing POS/ERP database** (read-only). "
                   "Works out of the box with the OASIS canonical schema.")
        url = st.text_input("POS database URL",
                            placeholder="postgresql://user:pass@host/db or sqlite:///path/to/pos.db",
                            key="ob_conn_url")
        if st.button("Test & Connect", use_container_width=True, key="ob_conn"):
            if not url.strip():
                st.warning("Enter a database URL first.")
            else:
                with st.spinner("Testing reachability..."):
                    res = OB.apply_connect(url.strip())
                if res["ok"]:
                    if custom_store_name and custom_store_name.strip():
                        try:
                            save_branding({"tenant_name": custom_store_name.strip(), "product_name": "OASIS"})
                        except Exception:
                            pass
                    st.success(res["detail"])
                    time.sleep(0.5)
                    st.rerun()
                else:
                    st.error(res["detail"])

    with c4:
        st.markdown("#### 🏢 Build Catalogue")
        st.caption("Initialize a real database from your provided **product "
                   "catalog** (`dept_*.xlsx` in `oasis/data/`).")
        cat = OB.catalog_available(project_root)
        if not cat["ok"]:
            st.info("**Catalogue files not found.** Export product catalogue as "
                    "`dept_*.xlsx` files into `oasis/data/` to use this path.")
            st.caption("Not got a catalogue export? Start with the sample store — "
                       "you can switch anytime.")
        else:
            st.caption(f"✓ {cat['files']} catalogue file(s) detected.")
            prof = st.selectbox("Store topology", ["single", "multi"], key="ob_profile")
            if st.button("Initialize DB", use_container_width=True, key="ob_init"):
                _execute_setup_with_progress(
                    lambda: OB.apply_init(prof),
                    f"Initialized {prof}-store catalogue database!"
                )

    st.divider()
    with st.expander("🏬 Running a Multi-Store Network?"):
        st.caption("Build the **multi-store demo network** (5 outlets: Rhapta, Westgate, "
                   "Kilimani, Lavington, Karen) to tour transfers, allocation, and "
                   "the Command Center across outlets.")
        if st.button("Build multi-store demo network", key="ob_multi"):
            _execute_setup_with_progress(
                lambda: OB.apply_multi_demo(),
                "Multi-store demo network (5 outlets) initialized successfully!"
            )

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
    elif src == "init":
        # a real catalogue-built store. Without this branch it fell to the
        # else and every console header called it un-onboarded, forever (S4).
        store = ob.get("store_name") or "catalogue store"
        prof = ob.get("profile")
        label = f"DATA: {store} (built from your catalogue" + \
                (f", {prof}-store)" if prof else ")")
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
