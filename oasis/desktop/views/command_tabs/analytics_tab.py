"""
Analytics for the Flet Command Center.

Parity target: ops_dashboard.py's "📈 Analytics" tab — weekly revenue trend
with week-over-week movement, and the department breakdown.

Weeks come from ``data.weekly_revenue``, the same accessor the Executive ROI
trend uses, so the two tabs cannot report different weeks for one store.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _bars(weeks) -> ft.Control:
    if not weeks:
        return ft.Text("No sales history yet.", size=12, color=T.TEXT_MUTED)
    top = max(w["revenue"] for w in weeks) or 1
    return ft.Column([
        ft.BarChart(
            bar_groups=[
                ft.BarChartGroup(x=i, bar_rods=[ft.BarChartRod(
                    from_y=0, to_y=w["revenue"], width=22,
                    color=T.TEAL, border_radius=3,
                    tooltip=f"{w['week']}\n{_money(w['revenue'])} · "
                            f"{w['units']:,.0f} units")])
                for i, w in enumerate(weeks)
            ],
            bottom_axis=ft.ChartAxis(
                labels=[ft.ChartAxisLabel(
                    value=i, label=ft.Text(w["week"].split("-")[1], size=9,
                                           color=T.TEXT_MUTED))
                    for i, w in enumerate(weeks)],
                labels_size=26),
            left_axis=ft.ChartAxis(labels_size=56),
            horizontal_grid_lines=ft.ChartGridLines(color=T.OBSIDIAN_BORDER,
                                                    width=1),
            bgcolor=T.DEEP_OBSIDIAN, max_y=top * 1.15,
            interactive=True, expand=True, height=240,
        ),
        ft.Text("Revenue per ISO week", size=11, color=T.TEXT_MUTED),
    ], spacing=8)


def build_analytics_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]
    header = ft.Text(f"Analytics — {store_name}", size=20,
                     weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY)

    wk = D.weekly_revenue(org, root=project_root)
    if wk.get("error"):
        return ft.Column([
            header,
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Analytics unavailable: {wk['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
        ], spacing=10, scroll=ft.ScrollMode.AUTO, expand=True)

    weeks = wk["weeks"]
    if not weeks:
        return ft.Column([
            header,
            ft.Text("No sales history for this store yet.", size=12,
                    color=T.TEXT_MUTED),
        ], spacing=10, expand=True)

    latest = wk["latest"]
    wow = wk["wow_pct"]
    wow_txt = "—" if wow is None else f"{wow:+.1f}%"
    wow_status = ("info" if wow is None
                  else "success" if wow >= 0 else "danger")

    # Department mix, from the same store-intelligence aggregation.
    si = D.store_intelligence(org, project_root)
    cats = si.get("categories") or []
    cat_rows = [
        ft.DataRow(cells=[
            ft.DataCell(ft.Text(str(c["Category"])[:26], size=11,
                                color=T.TEXT_PRIMARY)),
            ft.DataCell(ft.Text(_money(c["Revenue"]), size=11, color=T.SUCCESS)),
            ft.DataCell(ft.Text(f"{c['Units']:,.0f}", size=11,
                                color=T.TEXT_SECONDARY)),
        ]) for c in cats
    ]

    return ft.Column(
        controls=[
            header,
            ft.Container(height=8),
            ft.Row([
                T.metric_card("Latest Week Revenue", _money(latest["revenue"]),
                              status="success", sub=latest["week"]),
                T.metric_card("WoW Change", wow_txt, status=wow_status,
                              sub="against the prior week"),
                T.metric_card("Avg Weekly Revenue", _money(wk["avg"]),
                              status="info", sub=f"over {len(weeks)} weeks"),
                T.metric_card("Total Weeks", f"{len(weeks):,}", status="info",
                              sub="with trading"),
            ], spacing=12, expand=True),
            ft.Container(height=16),
            T.card_container(content=ft.Column([
                T.section_header("Weekly Revenue", "📈"),
                _bars(weeks),
            ], spacing=8)),
            ft.Container(height=16),
            T.card_container(content=ft.Column([
                T.section_header("Department Breakdown", "📂"),
                (ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(h, size=11,
                                                   color=T.TEXT_MUTED),
                                           numeric=num)
                             for h, num in (("Department", False),
                                            ("Revenue", True), ("Units", True))],
                    rows=cat_rows, heading_row_color=T.OBSIDIAN_RAISE,
                    data_row_color=T.DEEP_OBSIDIAN, column_spacing=18,
                    expand=True)
                 if cat_rows else
                 ft.Text("No department mix available.", size=12,
                         color=T.TEXT_MUTED)),
            ], spacing=8)),
        ],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
