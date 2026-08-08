"""
Live Sales tab for the Flet Command Center.

Parity target: ops_dashboard.py's "📊 Live Sales Feed" tab. Presentation only —
the feed is read and reduced by ``oasis.desktop.data.live_sales``.

One section of the console is deliberately NOT ported. Its "Hourly Revenue
Pattern" assigns each line a random hour (``np.random.normal(14, 3)``) because
the POS schema has no time of day — BILL_DT is a date and there is no
BILL_TIME. Day-over-day trend, which is real, is shown in its place.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _velocity_color(ratio) -> str:
    """The console's colour ladder: >3 hot, >1.5 warm, else normal."""
    if ratio is None:
        return T.TEXT_MUTED
    if ratio > 3:
        return T.DANGER
    if ratio > 1.5:
        return T.WARNING
    return T.SUCCESS


def _trend_chart(trend) -> ft.Control:
    """Day-over-day revenue — the honest replacement for the synthetic hour."""
    pts = trend[-14:]
    if len(pts) < 2:
        return ft.Text("Trend appears once there are two days of sales.",
                       size=12, color=T.TEXT_MUTED)
    top = max(p["revenue"] for p in pts) or 1
    return ft.Column([
        ft.LineChart(
            data_series=[ft.LineChartData(
                data_points=[
                    ft.LineChartDataPoint(
                        i, p["revenue"],
                        tooltip=f"{p['day']}\n{_money(p['revenue'])} · "
                                f"{p['units']:,.0f} units")
                    for i, p in enumerate(pts)
                ],
                stroke_width=2, color=T.TEAL, curved=True,
                below_line_bgcolor=T.TEAL_GLOW,
            )],
            min_x=0, max_x=len(pts) - 1, min_y=0, max_y=top * 1.15,
            bgcolor=T.DEEP_OBSIDIAN,
            horizontal_grid_lines=ft.ChartGridLines(color=T.OBSIDIAN_BORDER,
                                                    width=1),
            left_axis=ft.ChartAxis(labels_size=52),
            bottom_axis=ft.ChartAxis(
                labels=[ft.ChartAxisLabel(
                    value=i,
                    label=ft.Text(str(p["day"])[5:], size=9,
                                  color=T.TEXT_MUTED))
                    for i, p in enumerate(pts)
                    if i % max(1, len(pts) // 7) == 0],
                labels_size=24),
            interactive=True, expand=True, height=200,
        ),
        ft.Text(f"Revenue per trading day · last {len(pts)} days",
                size=11, color=T.TEXT_MUTED),
    ], spacing=8)


def _alerts_panel(alerts) -> ft.Control:
    if not alerts:
        return T.card_container(content=ft.Column([
            T.section_header("Velocity Alerts", "⚠️"),
            ft.Text("All normal", size=18, weight=ft.FontWeight.W_700,
                    color=T.SUCCESS),
            ft.Text("No velocity spikes detected on the latest trading day.",
                    size=12, color=T.TEXT_MUTED),
        ], spacing=6))

    cards = []
    for a in alerts[:5]:
        cards.append(ft.Container(
            content=ft.Column([
                ft.Row([
                    ft.Icon(ft.Icons.WARNING_AMBER, size=16, color=T.WARNING),
                    ft.Text(a.get("type", "VELOCITY_SPIKE"), size=11,
                            weight=ft.FontWeight.W_700, color=T.WARNING,
                            font_family="JetBrains Mono"),
                ], spacing=6),
                ft.Text(str(a.get("product_name", ""))[:34], size=13,
                        weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
                ft.Text(str(a.get("message", "")), size=11,
                        color=T.TEXT_SECONDARY),
                ft.Text(f"💡 {a.get('recommended_action', '')}", size=11,
                        color=T.WARNING),
            ], spacing=4),
            bgcolor=T.OBSIDIAN_RAISE,
            border=ft.border.only(left=ft.BorderSide(3, T.WARNING)),
            border_radius=8, padding=12,
        ))

    return T.card_container(content=ft.Column([
        T.section_header("Velocity Alerts", "⚠️"),
        ft.Text(f"{len(alerts)} line(s) selling above "
                f"{D.VELOCITY_SPIKE_PCT:.0f}% of their daily average.",
                size=12, color=T.TEXT_SECONDARY),
        *cards,
    ], spacing=8))


def build_live_sales_tab(page: ft.Page, project_root: str) -> ft.Column:
    """Live Sales feed for the Flet app."""

    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores in the active database.",
                                  color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    s = D.live_sales(org, root=project_root)
    header = ft.Text(f"Live Sales — {store_name}", size=20,
                     weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY)

    if s.get("error"):
        return ft.Column([
            header,
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Sales feed unavailable: {s['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
        ], spacing=10, scroll=ft.ScrollMode.AUTO, expand=True)

    if not s["lines"]:
        return ft.Column([
            header,
            ft.Text("No sales recorded for this store.", size=12,
                    color=T.TEXT_MUTED),
            ft.Text("OASIS.bat → 9 → 3 seeds sales history into the active "
                    "store.", size=11, color=T.TEXT_MUTED,
                    font_family="JetBrains Mono"),
        ], spacing=10, expand=True)

    # Basket value is real now that the feed carries a bill number; a dash
    # still means "the POS gave us no bill id", never zero.
    if s["basket_value"] is None:
        basket_val, basket_sub = "—", "no bill id in feed"
    else:
        basket_val = _money(s["basket_value"])
        basket_sub = f"across {s['baskets']:,} baskets"

    mover_rows = [
        ft.DataRow(cells=[
            ft.DataCell(ft.Text(str(i["name"])[:34], size=12,
                                color=T.TEXT_PRIMARY)),
            ft.DataCell(ft.Text(f"{i['units']:,.0f}", size=12,
                                color=T.TEXT_SECONDARY)),
            ft.DataCell(ft.Text(_money(i["revenue"]), size=12, color=T.SUCCESS)),
            ft.DataCell(ft.Text(
                "—" if i["velocity_ratio"] is None else f"{i['velocity_ratio']:.1f}x",
                size=12, weight=ft.FontWeight.W_600,
                color=_velocity_color(i["velocity_ratio"]))),
        ])
        for i in s["top"][:15]
    ]

    movers = T.card_container(content=ft.Column([
        T.section_header("Top Movers", "🔥"),
        ft.DataTable(
            columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                   numeric=num)
                     for h, num in (("Product", False), ("Units", True),
                                    ("Revenue", True), ("Velocity", True))],
            rows=mover_rows,
            heading_row_color=T.OBSIDIAN_RAISE,
            data_row_color=T.DEEP_OBSIDIAN,
            divider_thickness=1, column_spacing=18, expand=True,
        ),
        ft.Text("Velocity = units sold that day ÷ the line's average daily "
                "sales.", size=11, color=T.TEXT_MUTED),
    ], spacing=8))

    return ft.Column(
        controls=[
            header,
            ft.Text(f"Latest trading day: {s['trading_day']}", size=11,
                    color=T.TEXT_MUTED, font_family="JetBrains Mono"),
            ft.Container(height=10),
            ft.Row([
                T.metric_card("Revenue", _money(s["revenue"]), status="success",
                              sub="latest trading day"),
                T.metric_card("Units Sold", f"{s['units']:,.0f}", status="info",
                              sub=f"{s['lines']:,} line items"),
                T.metric_card("Avg Basket Value", basket_val, status="warning",
                              sub=basket_sub),
                T.metric_card("Active SKUs", f"{s['skus']:,}", status="info",
                              sub="sold that day"),
            ], spacing=12, expand=True),
            ft.Container(height=16),
            T.card_container(content=ft.Column([
                T.section_header("Multi-Day Trend", "📅"),
                _trend_chart(s["trend"]),
            ], spacing=8)),
            ft.Container(height=16),
            ft.Row([
                ft.Column([movers], expand=3),
                ft.Column([_alerts_panel(s["alerts"])], expand=2),
            ], spacing=16, vertical_alignment=ft.CrossAxisAlignment.START),
        ],
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
