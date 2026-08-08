"""
Allocation Engine for the Flet Command Center — the greenfield opening buy.

Parity target: ops_dashboard.py's "🧮 Allocation Engine" tab: pick a budget,
run the two-pass allocation with efficiency guards, see the basket.

Sourced differently, and this is the substantive difference. The console reads
``Full_Product_Allocation_Scorecard_*.csv`` — 23,000 rows of one retailer's
per-SKU revenue, margins, GMROI and named supplier terms. That file is not in
any release and must not be: competitors of the retailer it describes are named
in this product's own scenario templates. So the console's tab could only ever
raise FileNotFoundError on a client install.

Here the range is derived from the client's OWN network
(``oasis.logic.scorecard_builder``), which is both shippable and more correct —
a new site should be stocked from this chain's demand, not someone else's.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def build_allocation_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])

    card = D.greenfield_scorecard(root=project_root)
    out = ft.Container()

    budget = ft.TextField(label="Opening budget (KES)", value="2000000",
                          width=220, dense=True,
                          keyboard_type=ft.KeyboardType.NUMBER)
    mode_dd = ft.Dropdown(
        label="Range derived from", width=280, dense=True,
        options=[ft.dropdown.Option(key="network",
                                    text="The whole network (new site)"),
                 ft.dropdown.Option(key="store",
                                    text="One store (re-base an existing site)")],
        value="network")
    run_btn = ft.ElevatedButton("Run allocation", icon=ft.Icons.CALCULATE,
                                bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN)

    def _render(res: dict) -> ft.Control:
        if res.get("error"):
            return T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Allocation failed: {res['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8))

        # Column names are the basket DataFrame's, verified against a real run:
        # Product / Department / Qty / Allocated_Cost / Expected_Revenue / Type.
        rows = res["rows"]
        table_rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(r.get("Product", ""))[:34], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(str(r.get("Department", ""))[:18], size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(f"{float(r.get('Qty') or 0):,.0f}", size=11,
                                    color=T.SUCCESS)),
                ft.DataCell(ft.Text(_money(r.get("Allocated_Cost")), size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(_money(r.get("Expected_Revenue")), size=11,
                                    color=T.SUCCESS)),
                ft.DataCell(ft.Text(str(r.get("Type", ""))[:14], size=11,
                                    color=T.INFO)),
            ]) for r in rows[:200]
        ]

        left = res["budget"] - res["cash_spend"]
        return ft.Column([
            ft.Row([
                T.metric_card("Budget", _money(res["budget"]), status="info"),
                T.metric_card("Committed", _money(res["cash_spend"]),
                              status="success",
                              sub=f"{res.get('utilisation', 0):.1f}% of budget"),
                T.metric_card("Unspent", _money(left),
                              status="warning" if left > 0 else "success",
                              sub="after efficiency guards"),
                T.metric_card("Lines Stocked", f"{res['skus']:,}", status="info",
                              sub="in the opening basket"),
            ], spacing=12, expand=True),
            (ft.Text(f"Consignment value {_money(res['consignment_value'])} is "
                     "carried separately from cash.", size=11,
                     color=T.TEXT_MUTED)
             if res.get("consignment_value") else ft.Container()),
            ft.Container(height=12),
            T.card_container(content=ft.Column([
                T.section_header("Opening Basket", "🧮"),
                ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(h, size=11,
                                                   color=T.TEXT_MUTED),
                                           numeric=num)
                             for h, num in (("Product", False), ("Dept", False),
                                            ("Qty", True), ("Cost", True),
                                            ("Expected revenue", True),
                                            ("Type", False))],
                    rows=table_rows, heading_row_color=T.OBSIDIAN_RAISE,
                    data_row_color=T.DEEP_OBSIDIAN, column_spacing=16,
                    expand=True),
                (ft.Text(f"Showing 200 of {len(rows):,} lines.", size=11,
                         color=T.TEXT_MUTED) if len(rows) > 200
                 else ft.Container()),
            ], spacing=8)),
        ], spacing=8)

    def _on_run(e):
        try:
            b = float(str(budget.value).replace(",", "").strip())
        except (TypeError, ValueError):
            b = 0.0
        if b <= 0:
            out.content = ft.Text("Enter an opening budget above zero.",
                                  size=12, color=T.WARNING)
            if page:
                page.update()
            return
        run_btn.disabled = True
        run_btn.text = "Allocating…"
        if page:
            page.update()
        out.content = _render(D.run_greenfield(b, mode=mode_dd.value,
                                               org_cd=stores[0]["org_cd"],
                                               root=project_root))
        run_btn.disabled = False
        run_btn.text = "Re-run allocation"
        if page:
            page.update()

    run_btn.on_click = _on_run

    s = card.get("summary") or {}
    range_line = (
        f"{s.get('skus', 0):,} lines · {s.get('staples', 0):,} staples · "
        f"{s.get('departments', 0)} departments · {s.get('suppliers', 0)} suppliers "
        f"· {_money(s.get('daily_revenue'))}/day at "
        f"{s.get('avg_margin_pct')}% average margin"
        if s.get("skus") else (card.get("error") or "No range available."))

    return ft.Column(
        controls=[
            ft.Text("Allocation Engine", size=20, weight=ft.FontWeight.W_600,
                    color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            T.card_container(content=ft.Column([
                T.section_header("Greenfield Opening Buy", "🧮"),
                ft.Text("Budget-constrained two-pass allocation with the "
                        "engine's efficiency guards. The recommended range is "
                        "derived from your own network's demand — no external "
                        "scorecard.", size=12, color=T.TEXT_SECONDARY),
                ft.Text(range_line, size=11, color=T.TEXT_MUTED,
                        font_family="JetBrains Mono"),
                ft.Row([budget, mode_dd, run_btn], spacing=12, wrap=True,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
            ], spacing=10)),
            ft.Container(height=12),
            out,
        ],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
