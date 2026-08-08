"""
Transfer Intelligence tab for the Flet Command Center.

Presentation only — store risk, item-level stockout risk and transfer execution
all come from ``oasis.desktop.data``. The risk figures are inventory-led by
design: ``gnn_service.store_risk`` keeps the unvalidated model out of the number
until it beats baseline on a real-outcome backtest.

Licensing is applied by ``command_view`` (module ``network``).
"""
import logging
import flet as ft

from ... import theme as T
from ... import data as D

logger = logging.getLogger("OASIS.Desktop.TransferIntel")

_SEV_COLOR = {
    "DEPLETED": T.DANGER, "CRITICAL": T.DANGER,
    "URGENT": T.WARNING, "LOW": T.INFO,
}
_SEV_LABEL = {
    "DEPLETED": "DEPLETED", "CRITICAL": "CRITICAL (<½d)",
    "URGENT": "URGENT (<1d)", "LOW": "LOW (<3d)",
}


def _warn_card(title: str, msg: str) -> ft.Container:
    return T.card_container(content=ft.Column([
        T.section_header(title, ""),
        ft.Text(msg, size=12, color=T.WARNING),
    ], spacing=8))


def _cell(text, color=None, bold=False) -> ft.DataCell:
    return ft.DataCell(ft.Text(str(text), size=11,
                               color=color or T.TEXT_SECONDARY,
                               weight=ft.FontWeight.W_600 if bold else None))


def build_transfer_intel_tab(page: ft.Page, project_root: str) -> ft.Column:
    """Transfer Intelligence tab content."""

    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.", color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]

    controls = [
        ft.Text("Transfer Intelligence — Intra-Day Stockout Prevention",
                size=20, weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
        ft.Container(height=8),
    ]

    # ── Store risk ───────────────────────────────────────────────────────
    risk = D.network_risk(project_root)
    if risk.get("error"):
        controls.append(_warn_card("Store Risk Scores",
                                   f"Risk scores unavailable: {risk['error']}"))
    else:
        # Fixed width, not expand: this row WRAPS (a demo network is 14 stores)
        # and an expanding child inside a Wrap renders as a grey void.
        cards = [
            T.metric_card(
                s["name"][:20], f"{s['risk']:.2f}",
                status=("danger" if s["risk"] > 0.5
                        else "warning" if s["risk"] > 0.25 else "success"),
                sub="Risk Score", width=200)
            for s in risk["stores"]
        ]
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Store Risk Scores", ""),
            ft.Text(f"Signal: inventory-led · GNN status: {risk['status']}",
                    size=11, color=T.TEXT_MUTED, font_family="JetBrains Mono"),
            ft.Row(cards, spacing=12, wrap=True, run_spacing=12),
        ], spacing=8)))

    # ── Item-level stockout risk ─────────────────────────────────────────
    scan = D.network_stockout_risk(project_root)
    if scan.get("error"):
        controls.append(_warn_card("Item-Level Stockout Risk",
                                   f"Stockout scan failed: {scan['error']}"))
    else:
        c = scan["counts"]
        n_crit = c.get("DEPLETED", 0) + c.get("CRITICAL", 0)
        total = len(scan["items"])
        rows = [
            ft.DataRow(cells=[
                _cell(_SEV_LABEL.get(i["severity"], i["severity"]),
                      _SEV_COLOR.get(i["severity"], T.TEXT_SECONDARY), bold=True),
                _cell(i["name"][:35], T.TEXT_PRIMARY),
                _cell(i["store"][:20]),
                _cell(i["stock"]),
                _cell(i["ads"]),
                _cell(i["days_cover"], _SEV_COLOR.get(i["severity"])),
            ])
            for i in scan["items"][:50]
        ]
        table = ft.DataTable(
            columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                   numeric=num)
                     for h, num in (("Severity", False), ("Product", False),
                                    ("Store", False), ("Stock", True),
                                    ("ADS", True), ("Days Cover", True))],
            rows=rows,
            heading_row_color=T.OBSIDIAN_RAISE,
            data_row_color=T.DEEP_OBSIDIAN,
            column_spacing=16,
            expand=True,
        )
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Item-Level Stockout Risk (ADS Heuristic)", ""),
            ft.Text("Items with under 3 days of unit-based cover, all stores.",
                    size=12, color=T.TEXT_MUTED),
            ft.Row([
                T.metric_card("Depleted/Critical", str(n_crit), status="danger"),
                T.metric_card("Urgent (<1 day)", str(c.get("URGENT", 0)),
                              status="warning"),
                T.metric_card("Low (<3 days)", str(c.get("LOW", 0)), status="info"),
                T.metric_card("Total At-Risk", str(total),
                              status="danger" if total > 20 else "warning"),
            ], spacing=12, expand=True),
            ft.Container(height=8),
            table if rows else ft.Text("No unit-level stockouts projected.",
                                       size=12, color=T.SUCCESS),
            (ft.Text(f"Showing the 50 most urgent of {total}.",
                     size=11, color=T.TEXT_MUTED)
             if total > 50 else ft.Container()),
        ], spacing=8)))

    # ── Transfer execution ───────────────────────────────────────────────
    xfer = D.transfer_status(org, project_root)
    if xfer.get("error"):
        controls.append(_warn_card("Transfer Execution & Status",
                                   f"Could not read transfers: {xfer['error']}"))
    elif not xfer["rows"]:
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Transfer Execution & Status", ""),
            ft.Text("No transfer records found.", size=12, color=T.TEXT_MUTED),
        ], spacing=8)))
    else:
        status_colors = {"REQUESTED": T.WARNING, "IN_TRANSIT": T.TEAL,
                         "RECEIVED": T.SUCCESS}
        rows = []
        for r in xfer["rows"][:30]:
            sv = str(r.get("STATUS", ""))
            rows.append(ft.DataRow(cells=[
                _cell(r.get("ID", "")),
                _cell(str(r.get("PRODUCT_NAME", ""))[:30], T.TEXT_PRIMARY),
                _cell(r.get("FROM_ORG", "")),
                _cell(r.get("TO_ORG", "")),
                _cell(r.get("QTY", "")),
                _cell(sv, status_colors.get(sv, T.TEXT_MUTED), bold=True),
            ]))
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Transfer Execution & Status", ""),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("ID", False), ("Product", False),
                                        ("From", False), ("To", False),
                                        ("Qty", True), ("Status", False))],
                rows=rows,
                heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN,
                column_spacing=16,
                expand=True,
            ),
        ], spacing=8)))

    return ft.Column(
        controls=controls,
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
