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
from .command_tabs.live_sales_tab import build_live_sales_tab
from .command_tabs.transfer_intel_tab import build_transfer_intel_tab
from .command_tabs.stock_review_tab import build_stock_review_tab
from .command_tabs.smart_ordering_tab import build_smart_ordering_tab

#: tab → module SKU. Mirrors ops_view.TAB_MODULES and ops_dashboard.TAB_MODULES
#: so all three front doors draw the paywall in the same place. Anything absent
#: is core and stays visible on every licence.
TAB_MODULES = {
    "live_sales": "core",
    "transfers": "network",       # transfer_intelligence
    "stock_review": "core",
    "ordering": "ordering",       # smart_ordering / PO generation
}


def build_command_view(page: ft.Page, project_root: str) -> ft.Column:
    """Main Command Center view."""
    mods = D.allowed_modules()

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

    tabs = ft.Tabs(
        selected_index=0,
        animation_duration=300,
        tabs=[
            ft.Tab(
                text="Live Sales",
                icon=ft.Icons.SHOW_CHART,
                content=_content("live_sales", build_live_sales_tab),
            ),
            ft.Tab(
                text="Transfers",
                icon=ft.Icons.SWAP_HORIZ,
                content=_content("transfers", build_transfer_intel_tab),
            ),
            ft.Tab(
                text="Stock Review",
                icon=ft.Icons.INVENTORY_2,
                content=_content("stock_review", build_stock_review_tab),
            ),
            ft.Tab(
                text="Ordering",
                icon=ft.Icons.SHOPPING_CART,
                content=_content("ordering", build_smart_ordering_tab),
            ),
        ],
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
