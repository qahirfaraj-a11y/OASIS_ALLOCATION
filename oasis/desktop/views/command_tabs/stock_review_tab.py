"""
End-of-Day Stock Review tab for the Flet Command Center.

Presentation only — the health classification itself lives in
``oasis.desktop.data.stock_health`` so the Operations view, the Command Center
and the Streamlit console cannot disagree about what "critical" means.
"""
import logging
import flet as ft

from ... import theme as T
from ... import data as D

logger = logging.getLogger("OASIS.Desktop.StockReview")

_HEALTH_COLOR = {
    "STOCKOUT": T.DANGER,
    "CRITICAL": T.DANGER,
    "OVERSTOCK": T.WARNING,
    "HEALTHY": T.SUCCESS,
}


def build_stock_review_tab(page: ft.Page, project_root: str) -> ft.Column:
    """End-of-Day Stock Review tab content."""

    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.", color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    health = D.stock_health(org, project_root)
    if health.get("error"):
        return ft.Column([
            ft.Text(f"Stock Review — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Failed to load products: {health['error']}",
                        size=12, color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
        ], spacing=10, scroll=ft.ScrollMode.AUTO, expand=True)

    counts = health["counts"]
    items = health["items"]

    if not items:
        return ft.Column([
            ft.Text(f"Stock Review — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Text("No product data available.", size=12, color=T.TEXT_MUTED),
        ], spacing=10, expand=True)

    rows = []
    for i in items[:200]:
        color = _HEALTH_COLOR.get(i["health"], T.TEXT_SECONDARY)
        cover = "—" if i["days_cover"] is None else f"{i['days_cover']:.1f}"
        rows.append(ft.DataRow(cells=[
            ft.DataCell(ft.Text(i["health"], size=11, color=color,
                                weight=ft.FontWeight.W_600)),
            ft.DataCell(ft.Text(i["name"][:38], size=11, color=T.TEXT_PRIMARY)),
            ft.DataCell(ft.Text(i["dept"][:20], size=11, color=T.TEXT_SECONDARY)),
            ft.DataCell(ft.Text(str(i["stock"]), size=11, color=T.TEXT_SECONDARY)),
            ft.DataCell(ft.Text(str(i["ads"]), size=11, color=T.TEXT_SECONDARY)),
            ft.DataCell(ft.Text(cover, size=11, color=color)),
        ]))

    return ft.Column(
        controls=[
            ft.Text(f"Stock Review — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            ft.Row([
                T.metric_card("Healthy", f"{counts['HEALTHY']:,}", status="success"),
                T.metric_card("Critical", f"{counts['CRITICAL']:,}",
                              status="danger" if counts["CRITICAL"] else "success",
                              sub="under 1 day cover"),
                T.metric_card("Stockout", f"{counts['STOCKOUT']:,}",
                              status="danger" if counts["STOCKOUT"] else "success",
                              sub="on hand = 0"),
                T.metric_card("Overstock", f"{counts['OVERSTOCK']:,}",
                              status="warning" if counts["OVERSTOCK"] else "success",
                              sub="14d fresh / 30d ambient"),
            ], spacing=12, expand=True),
            ft.Container(height=10),
            T.card_container(content=ft.Column([
                T.section_header("Product Detail (most urgent first)", ""),
                ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                           numeric=num)
                             for h, num in (("Health", False), ("Product", False),
                                            ("Dept", False), ("Stock", True),
                                            ("ADS", True), ("Days Cover", True))],
                    rows=rows,
                    heading_row_color=T.OBSIDIAN_RAISE,
                    data_row_color=T.DEEP_OBSIDIAN,
                    column_spacing=16,
                    expand=True,
                ),
                (ft.Text(f"Showing 200 of {len(items):,} lines.",
                         size=11, color=T.TEXT_MUTED)
                 if len(items) > 200 else ft.Container()),
            ], spacing=8)),
        ],
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
