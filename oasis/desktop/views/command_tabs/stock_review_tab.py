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
    "CRITICAL": T.WARNING,
    "OVERSTOCK": T.TEXT_MUTED,
    "HEALTHY": T.SUCCESS,
}

#: the console plots cover < 100 so a handful of never-selling lines do not
#: flatten the axis for everything else
_SCATTER_COVER_CAP = 100.0


def _stock_vs_demand(items) -> ft.Control:
    """Stock Volume vs Demand — the console's px.scatter, natively.

    Flet has no scatter control, so this is a LineChart with the stroke turned
    off and a circle per point: one invisible series per health band, which is
    also how the colour mapping stays identical to the table above.
    """
    plotted = [i for i in items
               if i["days_cover"] is not None and i["days_cover"] < _SCATTER_COVER_CAP]
    if not plotted:
        return ft.Text("Not enough demand signal to plot stock against sales.",
                       size=12, color=T.TEXT_MUTED)

    series = []
    for health in ("HEALTHY", "CRITICAL", "STOCKOUT", "OVERSTOCK"):
        pts = [i for i in plotted if i["health"] == health]
        if not pts:
            continue
        colour = _HEALTH_COLOR[health]
        series.append(ft.LineChartData(
            data_points=[
                ft.LineChartDataPoint(
                    i["ads"], i["stock"],
                    tooltip=f"{i['name'][:28]}\n{i['stock']:,.0f} on hand · "
                            f"{i['ads']:,.2f} ADS · {i['days_cover']:.1f}d",
                    point=ft.ChartCirclePoint(color=colour, radius=4,
                                              stroke_width=0),
                )
                for i in pts
            ],
            stroke_width=0,          # points only — this is a scatter
            color=colour,
        ))

    max_ads = max(i["ads"] for i in plotted)
    max_stock = max(i["stock"] for i in plotted)
    return ft.Column([
        ft.LineChart(
            data_series=series,
            min_x=0, max_x=max(max_ads * 1.1, 1),
            min_y=0, max_y=max(max_stock * 1.1, 1),
            bgcolor=T.DEEP_OBSIDIAN,
            horizontal_grid_lines=ft.ChartGridLines(
                color=T.OBSIDIAN_BORDER, width=1),
            vertical_grid_lines=ft.ChartGridLines(
                color=T.OBSIDIAN_BORDER, width=1),
            left_axis=ft.ChartAxis(title=ft.Text("On hand", size=11,
                                                 color=T.TEXT_MUTED),
                                   title_size=20, labels_size=44),
            bottom_axis=ft.ChartAxis(title=ft.Text("Average daily sales",
                                                   size=11, color=T.TEXT_MUTED),
                                     title_size=20, labels_size=24),
            interactive=True,
            expand=True,
            height=320,
        ),
        ft.Row([
            ft.Row([ft.Container(width=10, height=10, bgcolor=_HEALTH_COLOR[h],
                                 border_radius=5),
                    ft.Text(h.title(), size=11, color=T.TEXT_SECONDARY)],
                   spacing=6)
            for h in ("HEALTHY", "CRITICAL", "STOCKOUT", "OVERSTOCK")
        ], spacing=18),
        ft.Text(f"Lines with under {_SCATTER_COVER_CAP:.0f} days of cover "
                f"({len(plotted):,} of {len(items):,}).",
                size=11, color=T.TEXT_MUTED),
    ], spacing=10)


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
        # 999 is "no demand signal", not a real cover figure — say so.
        cover = ("no demand" if i["days_cover"] is None
                 else f"{i['days_cover']:.1f}")
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
                T.metric_card("Healthy", f"{counts['HEALTHY']:,}", status="success",
                              sub=f"{D.CRITICAL_COVER_DAYS:g}d+ cover, in stock"),
                T.metric_card("Critical", f"{counts['CRITICAL']:,}",
                              status="warning" if counts["CRITICAL"] else "success",
                              sub=f"under {D.CRITICAL_COVER_DAYS:g} days cover"),
                T.metric_card("Stockout", f"{counts['STOCKOUT']:,}",
                              status="danger" if counts["STOCKOUT"] else "success",
                              sub="nothing on hand"),
                T.metric_card("Overstock", f"{counts['OVERSTOCK']:,}",
                              status="info" if counts["OVERSTOCK"] else "success",
                              sub=f"{D.OVERSTOCK_COVER_DAYS_FRESH:g}d fresh / "
                                  f"{D.OVERSTOCK_COVER_DAYS_AMBIENT:g}d ambient"),
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
            ft.Container(height=10),
            T.card_container(content=ft.Column([
                T.section_header("Stock Volume vs Demand", "📈"),
                _stock_vs_demand(items),
            ], spacing=8)),
        ],
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
