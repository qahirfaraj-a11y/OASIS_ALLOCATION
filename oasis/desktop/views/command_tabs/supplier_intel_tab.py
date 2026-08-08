"""
Supplier Intelligence for the Flet Command Center.

Parity target: ops_dashboard.py's "🔬 Supplier Intelligence" tab — HHI
concentration, top suppliers by share, and the failure-impact simulator.

Sourced differently on purpose. The console answers from
``supplier_analytics.load_scorecard_data``, which reads a scorecard CSV that is
not on the release whitelist, so on a client install that tab could only raise
FileNotFoundError. This reads the catalogue the store actually carries, which
every install has, and reports which basis it used.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


_SEV_COLOR = {"CRITICAL": T.DANGER, "HIGH": T.DANGER,
              "MEDIUM": T.WARNING, "LOW": T.SUCCESS}


def build_supplier_intel_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])
    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    header = ft.Text(f"Supplier Intelligence — {store_name}", size=20,
                     weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY)
    body = ft.Container()

    def _render(dept):
        conc = D.supplier_concentration(org, dept, project_root)
        if conc.get("error"):
            return ft.Text(f"Could not read suppliers: {conc['error']}",
                           size=12, color=T.DANGER)
        sups = conc["suppliers"]
        if not sups:
            return ft.Text("No supplier information on the carried catalogue.",
                           size=12, color=T.TEXT_MUTED)

        hhi_status = ("danger" if conc["hhi"] > D.HHI_HIGH
                      else "warning" if conc["hhi"] > D.HHI_MODERATE
                      else "success")

        rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(s["supplier"][:30], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(f"{s['skus']:,}", size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(_money(s["revenue_potential"]), size=11,
                                    color=T.SUCCESS)),
                ft.DataCell(ft.Text(f"{s['share_pct']:.1f}%", size=11,
                                    color=T.WARNING if s["share_pct"] >= 25
                                    else T.TEXT_SECONDARY,
                                    weight=ft.FontWeight.W_600)),
            ]) for s in sups[:10]
        ]

        # ── failure impact simulator ──
        picker = ft.Dropdown(
            label="Supplier", width=280, dense=True,
            options=[ft.dropdown.Option(s["supplier"]) for s in sups[:10]],
            value=sups[0]["supplier"])
        out = ft.Container()

        def _simulate(e):
            imp = D.supplier_failure_impact(org, picker.value, dept,
                                            root=project_root)
            if imp.get("error"):
                out.content = ft.Text(imp["error"], size=12, color=T.WARNING)
            else:
                out.content = ft.Row([
                    T.metric_card("Severity", imp["severity"],
                                  status=("danger" if imp["severity"] in
                                          ("CRITICAL", "HIGH")
                                          else "warning" if imp["severity"] == "MEDIUM"
                                          else "success"),
                                  sub=f"{imp['share_pct']:.1f}% of this scope"),
                    T.metric_card("Affected SKUs", f"{imp['affected_skus']:,}",
                                  status="info"),
                    T.metric_card("Revenue at Risk",
                                  _money(imp["revenue_at_risk"]),
                                  status="warning", sub="on-hand at sell price"),
                ], spacing=12, expand=True)
            if page:
                page.update()

        return ft.Column([
            ft.Row([
                T.metric_card("HHI Score", f"{conc['hhi']:,.0f}",
                              status=hhi_status, sub=conc["band"],
                              help_text=f"Above {D.HHI_HIGH:,.0f} is highly "
                                        f"concentrated; below "
                                        f"{D.HHI_MODERATE:,.0f} is healthy."),
                T.metric_card("Top Supplier Share",
                              f"{sups[0]['share_pct']:.1f}%",
                              status="warning" if sups[0]["share_pct"] >= 25
                              else "success",
                              sub=sups[0]["supplier"][:22]),
                T.metric_card("Tracked Suppliers", f"{len(sups):,}",
                              status="info", sub=f"by {conc['basis']}"),
            ], spacing=12, expand=True),
            ft.Container(height=12),
            T.section_header("Top Suppliers by Share", "🏭"),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("Supplier", False), ("SKUs", True),
                                        ("Revenue Potential", True),
                                        ("Share", True))],
                rows=rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=16, expand=True,
            ),
            ft.Container(height=16),
            T.section_header("Supplier Failure Impact Simulator", "⚠️"),
            ft.Text("What this store loses if a supplier stops delivering.",
                    size=12, color=T.TEXT_SECONDARY),
            ft.Row([picker,
                    ft.ElevatedButton("Simulate failure",
                                      icon=ft.Icons.REPORT_PROBLEM,
                                      on_click=_simulate)],
                   spacing=12, vertical_alignment=ft.CrossAxisAlignment.CENTER),
            out,
        ], spacing=8)

    all_depts = D.supplier_concentration(org, None, project_root).get(
        "departments", [])
    dept_dd = ft.Dropdown(
        label="Department", width=260, dense=True,
        options=[ft.dropdown.Option("All departments")]
                + [ft.dropdown.Option(d) for d in all_depts],
        value="All departments")

    def _on_dept(e):
        body.content = _render(None if dept_dd.value == "All departments"
                               else dept_dd.value)
        if page:
            page.update()

    dept_dd.on_change = _on_dept
    body.content = _render(None)

    return ft.Column(
        controls=[header, ft.Container(height=8), dept_dd,
                  ft.Container(height=12),
                  T.card_container(content=body)],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
