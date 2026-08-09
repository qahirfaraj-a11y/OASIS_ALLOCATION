"""
O.A.S.I.S. Desktop — Command Center (Phase 3).

The native twin of the Streamlit Command Center. Every tab here is licensed by
the SAME module SKU the browser console paywalls, because a native window that
runs Smart Ordering without an ``ordering`` licence is not a convenience — it is
the paid-module bypass finding R-2 warned about. The gate is applied to the tab
CONTENT, so a locked client still sees the tab exists and what it would buy.
"""

import flet as ft

from .. import data as D
from .. import theme as T
from .license_view import build_upsell
from .command_tabs.executive_roi_tab import build_executive_roi_tab
from .command_tabs.live_sales_tab import build_live_sales_tab
from .command_tabs.transfer_intel_tab import build_transfer_intel_tab
from .command_tabs.stock_review_tab import build_stock_review_tab
from .command_tabs.smart_ordering_tab import build_smart_ordering_tab
from .command_tabs.processor_tab import build_processor_tab
from .command_tabs.allocation_tab import build_allocation_tab
from .command_tabs.simulation_lab_tab import build_simulation_lab_tab
from .command_tabs.analytics_tab import build_analytics_tab
from .command_tabs.supplier_intel_tab import build_supplier_intel_tab

#: tab → module SKU. Mirrors ops_view.TAB_MODULES and ops_dashboard.TAB_MODULES
#: so all three front doors draw the paywall in the same place. Anything absent
#: is core and stays visible on every licence.
TAB_MODULES = {
    "executive_roi": "core",
    "live_sales": "core",
    "transfers": "network",       # transfer_intelligence
    "stock_review": "core",
    "ordering": "ordering",       # smart_ordering / PO generation
    "processor": "core",
    # Site planning is its own SKU: a chain buying "where should the next
    # store go and what should it carry" is not necessarily buying inter-store
    # transfers. See license_manager.MODULE_LABELS["greenfield"].
    "allocation": "greenfield",
    "simulation": "greenfield",
    "analytics": "core",
    "supplier_intelligence": "ordering",   # matches ops_dashboard.TAB_MODULES
}


#: tab → the ROLE permission key in auth_manager.ROLE_PERMISSIONS["tabs"].
#: Role and module are independent gates and BOTH must pass: a module SKU says
#: the install bought the capability, a role says this person may use it.
#: They fail differently on purpose — an unlicensed tab shows an upsell (it can
#: be bought), a forbidden one is not built at all (it cannot).
TAB_ROLE_KEYS = {
    "executive_roi": "executive_roi",
    "live_sales": "live_sales",
    "transfers": "transfer_intelligence",
    "stock_review": "stock_review",
    "ordering": "smart_ordering",
    "processor": "oasis_processor",
    "allocation": "allocation_engine",
    "simulation": "simulation_validation",
    "analytics": "analytics",
    "supplier_intelligence": "supplier_intelligence",
}


#: (tab key, label, icon, builder) — the console's order.
TAB_SPEC = [
    ("executive_roi", "Executive ROI", ft.Icons.EMOJI_EVENTS, build_executive_roi_tab),
    ("live_sales", "Live Sales", ft.Icons.SHOW_CHART, build_live_sales_tab),
    ("transfers", "Transfers", ft.Icons.SWAP_HORIZ, build_transfer_intel_tab),
    ("stock_review", "Stock Review", ft.Icons.INVENTORY_2, build_stock_review_tab),
    ("ordering", "Ordering", ft.Icons.SHOPPING_CART, build_smart_ordering_tab),
    ("processor", "Processor", ft.Icons.ROCKET_LAUNCH, build_processor_tab),
    ("allocation", "Allocation", ft.Icons.CALCULATE, build_allocation_tab),
    ("simulation", "Simulation", ft.Icons.SCIENCE, build_simulation_lab_tab),
    ("analytics", "Analytics", ft.Icons.INSIGHTS, build_analytics_tab),
    ("supplier_intelligence", "Suppliers", ft.Icons.FACTORY, build_supplier_intel_tab),
]


def build_command_view(page: ft.Page, project_root: str) -> ft.Column:
    """Main Command Center view."""
    mods = D.allowed_modules()
    try:
        role = (page.session.get("role") if page is not None else None) or "ops_admin"
    except Exception:
        role = "ops_admin"
    permitted = D.role_tabs(role)

    def _allowed_for_role(tab_key: str) -> bool:
        key = TAB_ROLE_KEYS.get(tab_key)
        if key is None:
            return True
        # Absent key = not granted. get_user_permissions already falls back to
        # the least-privileged role for an unknown one.
        return bool(permitted.get(key, False))

    def _content(tab_key: str, builder):
        """The tab's real content, or the upsell that replaces it when locked.

        Built lazily behind the gate: an unlicensed window must not run the
        pipeline at all, not merely decline to draw its output.
        """
        module = TAB_MODULES.get(tab_key, "core")
        if module not in mods:
            return ft.Column([build_upsell(module)], scroll=ft.ScrollMode.AUTO,
                             expand=True)
        return builder(page, project_root)


    # A role the operator does not hold removes the tab entirely — the console
    # never builds it either. Licensing shows an upsell instead, because an
    # unlicensed capability can be bought and a forbidden one cannot.
    visible = [t for t in TAB_SPEC if _allowed_for_role(t[0])]
    if not visible:
        return ft.Column([
            ft.Row([
                ft.Icon(ft.Icons.SHIELD, size=32, color=T.TEAL),
                ft.Text("Command Center", size=28, weight=ft.FontWeight.W_700,
                        color=T.TEXT_PRIMARY),
            ], vertical_alignment=ft.CrossAxisAlignment.CENTER),
            T.card_container(content=ft.Column([
                ft.Icon(ft.Icons.LOCK_OUTLINE, size=30, color=T.WARNING),
                ft.Text("Your account has no Command Center access.", size=14,
                        color=T.TEXT_PRIMARY),
                ft.Text(f"Signed in as {role}. Ask an administrator to grant "
                        "the tabs you need.", size=12, color=T.TEXT_SECONDARY),
            ], spacing=8, horizontal_alignment=ft.CrossAxisAlignment.CENTER)),
        ], spacing=12, expand=True)

    tabs = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        tabs=[ft.Tab(text=label, icon=icon, content=_content(key, builder))
              for key, label, icon, builder in visible],
        expand=1,
    )

    return ft.Column(
        controls=[
            ft.Row([
                ft.Icon(ft.Icons.SHIELD, size=32, color=T.TEAL),
                ft.Text("Command Center", size=28, weight=ft.FontWeight.W_700,
                        color=T.TEXT_PRIMARY),
            ], alignment=ft.MainAxisAlignment.START,
                vertical_alignment=ft.CrossAxisAlignment.CENTER),
            T.spec_tag("OPERATIONS CONSOLE", hot=True),
            ft.Container(height=20),
            tabs,
        ],
        expand=True,
    )
