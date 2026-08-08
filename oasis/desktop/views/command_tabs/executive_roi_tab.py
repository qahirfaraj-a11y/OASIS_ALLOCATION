"""
Executive ROI Overview for the Flet Command Center.

Parity target: ops_dashboard.py's "🏆 Executive ROI Overview" tab. Presentation
only — the dead-stock and stockout rules live in ``data.executive_roi`` and are
the console's exactly (AMIT: ADS < 0.2 and on-hand > 15 is dead capital;
ADS > 0 with under a unit on hand is a stockout).

The console's "Demo Showcase" mode, which overrides the savings figure from
``showcase_roi_savings`` in system config, is NOT ported: it substitutes a
configured headline number for a measured one, which is a sales prop rather
than an operations read.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _trend(weekly: dict) -> ft.Control:
    weeks = weekly.get("weeks") or []
    if len(weeks) < 2:
        return ft.Text("No sales history available for this store yet.",
                       size=12, color=T.TEXT_MUTED)
    top = max(w["revenue"] for w in weeks) or 1
    return ft.Column([
        ft.LineChart(
            data_series=[ft.LineChartData(
                data_points=[
                    ft.LineChartDataPoint(
                        i, w["revenue"],
                        tooltip=f"{w['week']} (from {w['start']})\n"
                                f"{_money(w['revenue'])} · {w['units']:,.0f} units")
                    for i, w in enumerate(weeks)
                ],
                stroke_width=2, color=T.TEAL, curved=True,
                below_line_bgcolor=T.TEAL_GLOW,
            )],
            min_x=0, max_x=len(weeks) - 1, min_y=0, max_y=top * 1.15,
            bgcolor=T.DEEP_OBSIDIAN,
            horizontal_grid_lines=ft.ChartGridLines(color=T.OBSIDIAN_BORDER,
                                                    width=1),
            left_axis=ft.ChartAxis(labels_size=56),
            bottom_axis=ft.ChartAxis(
                labels=[ft.ChartAxisLabel(
                    value=i, label=ft.Text(w["week"].split("-")[1], size=9,
                                           color=T.TEXT_MUTED))
                    for i, w in enumerate(weeks)],
                labels_size=24),
            interactive=True, expand=True, height=220,
        ),
        ft.Text(f"{len(weeks)} weeks of trading", size=11, color=T.TEXT_MUTED),
    ], spacing=8)


def build_executive_roi_tab(page: ft.Page, project_root: str) -> ft.Column:
    """Executive ROI Overview tab content."""
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]
    roi = D.executive_roi(org, project_root)
    header = ft.Text(f"Executive ROI Overview — {store_name}", size=20,
                     weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY)

    if roi.get("error"):
        return ft.Column([
            header,
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Could not read ROI metrics: {roi['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
        ], spacing=10, scroll=ft.ScrollMode.AUTO, expand=True)

    if not roi["total_skus"]:
        return ft.Column([
            header,
            ft.Text("No live stock data for this store yet — connect the ERP "
                    "feed to populate ROI metrics.", size=12, color=T.TEXT_MUTED),
        ], spacing=10, expand=True)

    dead_ok = roi["dead_pct"] < D.ROI_DEAD_STOCK_TARGET_PCT
    so_ok = roi["so_pct"] < D.ROI_STOCKOUT_TARGET_PCT
    verdict = (f"Dead stock {roi['dead_pct']}% · stockout {roi['so_pct']}%. "
               + ("Both within Playbook targets"
                  if dead_ok and so_ok else "Targets")
               + f" (dead <{D.ROI_DEAD_STOCK_TARGET_PCT:g}%, "
                 f"stockout <{D.ROI_STOCKOUT_TARGET_PCT:g}%).")

    weekly = D.weekly_revenue(org, root=project_root)

    return ft.Column(
        controls=[
            header,
            T.spec_tag("MEASURED FROM LIVE STOCK", hot=False),
            ft.Container(height=12),
            ft.Row([
                T.metric_card("Active SKUs", f"{roi['total_skus']:,}",
                              status="info", sub="carried by this store"),
                T.metric_card("Dead Stock", f"{roi['dead_pct']}%",
                              status="success" if dead_ok else "danger",
                              sub=f"target <{D.ROI_DEAD_STOCK_TARGET_PCT:g}%",
                              help_text="AMIT rule: ADS under 0.2 with more "
                                        "than 15 units on hand."),
                T.metric_card("Stockout", f"{roi['so_pct']}%",
                              status="success" if so_ok else "danger",
                              sub=f"{roi['stockout']:,} lines · target "
                                  f"<{D.ROI_STOCKOUT_TARGET_PCT:g}%"),
                T.metric_card("Availability", f"{roi['avail']}%",
                              status="success" if so_ok else "warning",
                              sub="of carried lines sellable"),
            ], spacing=12, expand=True),
            ft.Container(height=10),
            ft.Row([
                T.metric_card("Recoverable Capital", _money(roi["trapped"]),
                              status="warning",
                              sub="tied up in dead stock, at cost"),
            ], spacing=12, expand=True),
            ft.Container(height=10),
            T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.CHECK_CIRCLE if dead_ok and so_ok
                        else ft.Icons.INFO_OUTLINE, size=18,
                        color=T.SUCCESS if dead_ok and so_ok else T.INFO),
                ft.Text(verdict, size=12, color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)),
            ft.Container(height=16),
            T.card_container(content=ft.Column([
                T.section_header("Weekly Revenue Trend (last 90 days)", "💹"),
                (_trend(weekly) if not weekly.get("error")
                 else ft.Text(f"Trend unavailable: {weekly['error']}", size=12,
                              color=T.WARNING)),
            ], spacing=8)),
        ],
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
