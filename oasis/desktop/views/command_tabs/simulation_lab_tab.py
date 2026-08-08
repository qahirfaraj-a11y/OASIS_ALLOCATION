"""
Simulation Lab for the Flet Command Center.

Parity target: ops_dashboard.py's "🧪 Simulation Lab" — run the store forward
twice over the same SKUs with the same seed, once on plain heuristic
replenishment and once with the store's risk score fed to the ordering bridge,
and compare.

Built from the store's own enriched products. The simulator used to read an
allocation scorecard holding one retailer's per-SKU revenue and named supplier
terms; that file is not part of any install and must not be.

The run is button-triggered because it takes seconds, and the Command Center
builds every tab when it opens.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _delta_row(label: str, base: float, adj: float, unit: str = "",
               higher_is_better: bool = True) -> ft.DataRow:
    delta = adj - base
    good = (delta >= 0) if higher_is_better else (delta <= 0)
    colour = T.TEXT_MUTED if abs(delta) < 1e-9 else (T.SUCCESS if good
                                                     else T.DANGER)
    fmt = (lambda v: _money(v)) if unit == "KES" else (
        lambda v: f"{v:,.2f}{unit}")
    return ft.DataRow(cells=[
        ft.DataCell(ft.Text(label, size=12, color=T.TEXT_PRIMARY)),
        ft.DataCell(ft.Text(fmt(base), size=12, color=T.TEXT_SECONDARY)),
        ft.DataCell(ft.Text(fmt(adj), size=12, color=T.TEXT_SECONDARY)),
        ft.DataCell(ft.Text(f"{delta:+,.2f}" if unit != "KES"
                            else f"{delta:+,.0f}",
                            size=12, color=colour,
                            weight=ft.FontWeight.W_600)),
    ])


def build_simulation_lab_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])
    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    tiers = D.simulation_tiers()
    days = ft.Slider(min=7, max=60, divisions=53, value=30,
                     label="{value} days", width=280)
    tier_dd = ft.Dropdown(
        label="Store archetype", width=280, dense=True,
        options=[ft.dropdown.Option(key="", text="Auto (from stock value)")]
                + [ft.dropdown.Option(key=t["key"],
                                      text=f"{t['key']} — {t['description']}")
                   for t in tiers.get("tiers", [])],
        value="")
    out = ft.Container()
    run_btn = ft.ElevatedButton("Run comparison simulation",
                                icon=ft.Icons.SCIENCE, bgcolor=T.TEAL,
                                color=T.DEEP_OBSIDIAN)

    def _render(r: dict) -> ft.Control:
        if r.get("error"):
            return T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Simulation failed: {r['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8))

        b, a = r["heuristic"], r["adjusted"]
        return T.card_container(content=ft.Column([
            T.section_header("Side-by-Side Comparison", "📊"),
            ft.Text(f"{r['days']} days · {r['skus']:,} SKUs · tier {r['tier']} "
                    f"· opening stock {_money(r['budget'])} · risk "
                    f"{r['risk']:.3f} (model: {r.get('gnn_status', '—')})",
                    size=11, color=T.TEXT_MUTED, font_family="JetBrains Mono"),
            ft.Container(height=8),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("Measure", False),
                                        ("Heuristic", True),
                                        ("Risk-Adjusted", True),
                                        ("Delta", True))],
                rows=[
                    _delta_row("Fill rate", b["fill_rate"], a["fill_rate"], "%"),
                    _delta_row("Stockout rate", b["stockout_rate"],
                               a["stockout_rate"], "%", higher_is_better=False),
                    _delta_row("Revenue", b["revenue"], a["revenue"], "KES"),
                    _delta_row("Inventory turnover", b["turnover"],
                               a["turnover"], "x"),
                    _delta_row("Capital efficiency", b["capital_efficiency"],
                               a["capital_efficiency"], "%"),
                ],
                heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=20, expand=True,
            ),
            ft.Text("Both runs share a seed and a starting SKU set — the only "
                    "difference is whether the store's risk score reaches the "
                    "ordering bridge. Simulation only; nothing was written.",
                    size=11, color=T.TEXT_MUTED),
        ], spacing=8))

    def _on_run(e):
        run_btn.disabled = True
        run_btn.text = "Running two simulations…"
        if page:
            page.update()
        out.content = _render(D.run_simulation_comparison(
            org, days=int(days.value), tier=tier_dd.value or None,
            root=project_root))
        run_btn.disabled = False
        run_btn.text = "Re-run comparison"
        if page:
            page.update()

    run_btn.on_click = _on_run

    return ft.Column(
        controls=[
            ft.Text(f"Simulation Lab — {store_name}", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            T.card_container(content=ft.Column([
                T.section_header("Replenishment Comparison", "🧪"),
                ft.Text("Run this store forward twice over the same SKUs and "
                        "the same seed — plain heuristic replenishment against "
                        "risk-adjusted — and compare the outcome.",
                        size=12, color=T.TEXT_SECONDARY),
                ft.Row([ft.Text("Duration", size=12, color=T.TEXT_SECONDARY),
                        days], spacing=12,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
                ft.Row([tier_dd, run_btn], spacing=12, wrap=True,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
                (ft.Text(f"Archetypes unavailable: {tiers['error']}", size=11,
                         color=T.WARNING) if tiers.get("error")
                 else ft.Container()),
            ], spacing=10)),
            ft.Container(height=12),
            out,
        ],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
