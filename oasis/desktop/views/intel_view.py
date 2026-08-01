"""
O.A.S.I.S. Desktop — Intelligence view (Phase 2).

Monitoring, Backtests and Engine Telemetry. Reads through oasis.desktop.data,
the same accessor the Operations view uses, so the two consoles cannot drift
onto different numbers for the same store.

The engine telemetry tab is the one that matters most here: finding S1 was a
whole engine layer sitting dormant and silent on every client install, and the
only reason it stayed invisible was that nothing surfaced which engines were
actually on. This tab is that surface.
"""

import flet as ft

from .. import data as D
from .. import theme as T


def _not_migrated(title: str, detail: str, uses: str) -> ft.Container:
    return T.card_container(
        content=ft.Column([
            ft.Row([
                ft.Icon(ft.Icons.CONSTRUCTION, size=18, color=T.WARNING),
                ft.Text(title, size=15, weight=ft.FontWeight.W_600,
                        color=T.TEXT_PRIMARY),
            ], spacing=8),
            ft.Text(detail, size=12, color=T.TEXT_SECONDARY),
            ft.Text(uses, size=11, color=T.TEXT_MUTED,
                    font_family="JetBrains Mono"),
        ], spacing=6),
    )


def _engine_row(name: str, enabled: bool) -> ft.Row:
    return ft.Row([
        ft.Icon(ft.Icons.CIRCLE, size=10,
                color=T.SUCCESS if enabled else T.TEXT_MUTED),
        ft.Text(name.upper(), size=12, color=T.TEXT_PRIMARY, expand=True,
                font_family="JetBrains Mono"),
        ft.Text("ACTIVE" if enabled else "OFF", size=11,
                color=T.SUCCESS if enabled else T.TEXT_MUTED,
                font_family="JetBrains Mono"),
    ], spacing=10)


def build_intel_view(page: ft.Page, project_root: str) -> ft.Column:
    """Construct the Intelligence view."""
    stores = D.list_stores(project_root)
    prov = D.data_provenance(project_root)
    eng = D.engine_posture(project_root)
    engines = eng.get("engines") or {}
    live = [n for n, on in engines.items() if on]

    # ── Monitoring ───────────────────────────────────────────────────────
    if stores:
        org = stores[0]["org_cd"]
        stock = D.stock_overview(org, project_root)
        cover = "healthy"
        if stock["skus"]:
            out_pct = stock["stockouts"] / stock["skus"] * 100
            cover = ("critical" if out_pct > 10 else
                     "watch" if out_pct > 2 else "healthy")
        monitoring = [
            ft.Row([
                T.metric_card("Stores", str(len(stores)), status="info"),
                T.metric_card("SKUs monitored", f"{stock['skus']:,}", status="info"),
                T.metric_card("Availability", cover.upper(),
                              status={"healthy": "success", "watch": "warning",
                                      "critical": "danger"}[cover],
                              sub=f"{stock['stockouts']:,} out of stock"),
            ], spacing=12, expand=True),
        ]
        if stock.get("error"):
            monitoring.append(ft.Text(f"Stock read failed: {stock['error']}",
                                      size=12, color=T.DANGER))
    else:
        monitoring = [ft.Text("No stores in the active database — complete "
                              "first-run setup.", size=13, color=T.TEXT_SECONDARY)]

    monitoring.append(_not_migrated(
        "Live pulse and velocity alerts not yet in the desktop app",
        "Real-time pulse, velocity alerts, stock review and basket analysis "
        "still run in the Intelligence Console.",
        "OASIS.bat → 3  ·  entrypoint.py --mode intel"))

    # ── Backtests ────────────────────────────────────────────────────────
    backtests = [_not_migrated(
        "Backtesting not yet in the desktop app",
        "Shadow-mode comparison and backtest reporting still run in the "
        "Operations Console's Shadow page and the CLI.",
        "OASIS.bat → 2 (Shadow)  ·  entrypoint.py --mode shadow")]

    # ── Engine telemetry ─────────────────────────────────────────────────
    tier = str(eng.get("tier", "?"))
    telemetry = [
        ft.Row([
            T.metric_card("Engines live", f"{len(live)}/{len(engines)}",
                          status="success" if live else "danger",
                          sub=", ".join(live) if live else "none active"),
            T.metric_card("Config tier", tier.upper(),
                          status={"live": "success", "default": "warning"}.get(tier, "danger"),
                          sub=eng.get("file") or "—",
                          help_text="'default' = shipped methodology defaults, untuned. "
                                    "'none' = no config resolved; every engine is off."),
        ], spacing=12, expand=True),
        ft.Container(height=8),
        T.section_header("Chapter-11 engine layer", "⚙"),
        T.card_container(
            content=ft.Column(
                [_engine_row(n, on) for n, on in engines.items()]
                or [ft.Text("No engine config resolved.", size=12, color=T.DANGER)],
                spacing=8)),
    ]
    if tier == "default":
        telemetry.append(ft.Text(
            "Running the shipped methodology defaults. Tune them in the "
            "Operations Console → Settings → Engine Flags, which writes your "
            "own oasis_engines_config.json.",
            size=11, color=T.TEXT_MUTED))
    if eng.get("error"):
        telemetry.append(ft.Text(f"Engine config unreadable: {eng['error']}",
                                 size=12, color=T.DANGER))

    def _tab(label: str, icon: str, controls) -> ft.Tab:
        return ft.Tab(text=label, icon=icon,
                      content=ft.Container(
                          content=ft.Column(controls, spacing=10,
                                            scroll=ft.ScrollMode.AUTO),
                          padding=ft.padding.only(top=16)))

    header = [T.spec_tag("INTELLIGENCE CONSOLE", hot=True),
              ft.Text("Intelligence", size=28, weight=ft.FontWeight.W_700,
                      color=T.TEXT_PRIMARY)]
    if prov.get("is_sample"):
        header.append(ft.Container(
            content=ft.Text("◆ SAMPLE DATA — the built-in demo store, not your "
                            "live data.", size=11, color=T.WARNING,
                            font_family="JetBrains Mono"),
            bgcolor="#3A2A00", border_radius=8,
            padding=ft.padding.symmetric(horizontal=12, vertical=6)))

    return ft.Column(
        controls=[
            *header,
            ft.Container(height=8),
            ft.Tabs(selected_index=0, animation_duration=200,
                    indicator_color=T.TEAL, label_color=T.TEAL,
                    unselected_label_color=T.TEXT_MUTED,
                    tabs=[
                        _tab("Monitoring", ft.Icons.MONITOR_HEART_OUTLINED, monitoring),
                        _tab("Backtests", ft.Icons.HISTORY, backtests),
                        _tab("Engine telemetry", ft.Icons.SETTINGS_INPUT_COMPONENT,
                             telemetry),
                    ], expand=True),
        ],
        spacing=6, expand=True,
    )
