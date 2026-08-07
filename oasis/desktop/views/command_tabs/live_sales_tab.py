"""
Live Sales tab for the Flet Command Center.

Presentation only — the feed is read and reduced by
``oasis.desktop.data.live_sales``, which reports line items as line items and
declines to invent a basket value when the feed carries no bill identifier.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def build_live_sales_tab(page: ft.Page, project_root: str) -> ft.Column:
    """Live Sales feed for the Flet app."""

    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores in the active database.",
                                  color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    s = D.live_sales(org, root=project_root)
    if s.get("error"):
        return ft.Column([
            ft.Text(f"Live Sales — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Sales feed unavailable: {s['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
        ], spacing=10, scroll=ft.ScrollMode.AUTO, expand=True)

    if not s["lines"]:
        return ft.Column([
            ft.Text(f"Live Sales — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Text("No sales recorded for this store.", size=12,
                    color=T.TEXT_MUTED),
        ], spacing=10, expand=True)

    # Basket value is shown only when the feed can support it. A dash here means
    # "the POS feed carries no bill identifier", not "zero".
    if s["basket_value"] is None:
        basket_val, basket_sub = "—", "no bill id in feed"
    else:
        basket_val = f"KES {s['basket_value']:,.0f}"
        basket_sub = f"across {s['baskets']:,} baskets"

    rows = [
        ft.DataRow(cells=[
            ft.DataCell(ft.Text(str(i["name"])[:40], size=12, color=T.TEXT_PRIMARY)),
            ft.DataCell(ft.Text(f"{i['units']:,.0f}", size=12, color=T.TEXT_SECONDARY)),
            ft.DataCell(ft.Text(f"KES {i['revenue']:,.0f}", size=12, color=T.SUCCESS)),
        ])
        for i in s["top"][:10]
    ]

    top_movers = ft.DataTable(
        columns=[
            ft.DataColumn(ft.Text("Product", size=12, color=T.TEXT_MUTED)),
            ft.DataColumn(ft.Text("Units", size=12, color=T.TEXT_MUTED), numeric=True),
            ft.DataColumn(ft.Text("Revenue", size=12, color=T.TEXT_MUTED), numeric=True),
        ],
        rows=rows,
        heading_row_color=T.OBSIDIAN_RAISE,
        data_row_color=T.DEEP_OBSIDIAN,
        divider_thickness=1,
        column_spacing=20,
        expand=True,
    ) if rows else ft.Text("No line items on the latest trading day.",
                           color=T.TEXT_MUTED)

    return ft.Column(
        controls=[
            ft.Text(f"Live Sales — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Text(f"Latest trading day: {s['trading_day']}", size=11,
                    color=T.TEXT_MUTED, font_family="JetBrains Mono"),
            ft.Container(height=10),
            ft.Row([
                T.metric_card("Revenue", f"KES {s['revenue']:,.0f}",
                              status="success", sub="latest trading day"),
                T.metric_card("Units Sold", f"{s['units']:,.0f}", status="info",
                              sub=f"{s['lines']:,} line items"),
                T.metric_card("Avg Basket Value", basket_val, status="warning",
                              sub=basket_sub),
                T.metric_card("Active SKUs", f"{s['skus']:,}", status="info",
                              sub="sold that day"),
            ], spacing=12, expand=True),
            ft.Container(height=20),
            T.section_header("Top Movers", "🔥"),
            T.card_container(content=top_movers),
        ],
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
