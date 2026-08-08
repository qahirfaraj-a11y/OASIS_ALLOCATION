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


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _network_opportunities(page, project_root: str, org: str,
                           stores) -> ft.Control:
    """The console's Live Network Transfer Opportunities section.

    The scan is behind a button rather than run on render. It loads every
    store's enriched catalogue and the transfer service, which costs seconds;
    the Command Center builds all four tabs when it opens, so running it eagerly
    would make every visit to Live Sales pay for a network scan nobody asked
    for. The console can afford it — Streamlit renders one tab at a time.
    """
    body = ft.Container()

    if len(stores) < 2:
        return T.card_container(content=ft.Column([
            T.section_header("Live Network Transfer Opportunities", "🌐"),
            ft.Text("This install has a single store, so there is nothing to "
                    "transfer between.", size=12, color=T.TEXT_SECONDARY),
        ], spacing=8))

    def _render(scan: dict) -> ft.Control:
        if scan.get("error"):
            return _warn_card("Network scan", scan["error"])
        t = scan["totals"]
        opps = scan["opportunities"]

        health_rows = [
            ft.DataRow(cells=[
                _cell(h["store"][:24], T.TEXT_PRIMARY),
                _cell(h["total_skus"]),
                _cell(h["overstock"]),
                _cell(h["deficits"], T.WARNING if h["deficits"] else None),
                _cell(h["push_from"]),
                _cell(f"{h['risk']:.3f}"),
                _cell(h["status"],
                      T.DANGER if h["status"] == "High Risk"
                      else T.WARNING if h["status"] == "Moderate" else T.SUCCESS,
                      bold=True),
            ]) for h in scan["store_health"]
        ]

        opp_rows = [
            ft.DataRow(cells=[
                _cell(o["type"] + (" · manual" if o["manual_only"] else ""),
                      T.WARNING if o["type"] == "PULL" else T.INFO, bold=True),
                _cell(o["product"][:32], T.TEXT_PRIMARY),
                _cell(o["from"][:18]),
                _cell(o["to"][:18]),
                _cell(f"{o['qty']:,.0f}", T.SUCCESS),
                _cell(f"{o['donor_cover']:.1f}"),
                _cell(f"{o['recipient_cover']:.1f}",
                      T.DANGER if o["recipient_cover"] <= 1 else None),
                _cell(_money(o["value"])),
                _cell(str(o["department"])[:16]),
            ]) for o in opps[:100]
        ]

        limit = ft.TextField(label="Max to queue", value=str(min(50, len(opps))),
                             width=130, dense=True, keyboard_type=ft.KeyboardType.NUMBER)
        queue_btn = ft.ElevatedButton(
            "Queue transfers to database", icon=ft.Icons.PLAYLIST_ADD_CHECK,
            bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN)
        result = ft.Text("", size=12, color=T.TEXT_SECONDARY)

        def _on_queue(e):
            try:
                n = int(limit.value or 0)
            except (TypeError, ValueError):
                n = 0
            if n <= 0:
                result.value = "Enter how many transfers to queue."
                result.color = T.WARNING
                if page: page.update()
                return
            queue_btn.disabled = True
            queue_btn.text = "Queuing…"
            if page: page.update()
            try:
                username = page.session.get("username") or "system" if page else "system"
            except Exception:
                username = "system"
            res = D.queue_transfers(opps, username, org, limit=n,
                                    root=project_root)
            if res["queued"]:
                result.value = (f"Queued {res['queued']} transfer(s)"
                                + (f"; skipped {res['skipped']} fresh/failed."
                                   if res["skipped"] else "."))
                result.color = T.SUCCESS
                queue_btn.icon = ft.Icons.CHECK
            else:
                result.value = res.get("error") or "Nothing queued."
                result.color = T.DANGER
            queue_btn.disabled = False
            queue_btn.text = "Queue transfers to database"
            if page: page.update()

        queue_btn.on_click = _on_queue

        return ft.Column([
            ft.Row([
                T.metric_card("Stores Scanned", f"{t['stores']:,}", status="info"),
                T.metric_card("Overstock SKUs", f"{t['overstock_skus']:,}",
                              status="info", sub="network-wide excess"),
                T.metric_card("Deficit SKUs", f"{t['deficit_skus']:,}",
                              status="warning" if t["deficit_skus"] else "success",
                              sub="pull triggers (<7d)"),
                T.metric_card("Push Opportunities", f"{t['push_opps']:,}",
                              status="info", sub="cold → hot"),
            ], spacing=12, expand=True),
            (ft.Text(f"{t['pending_outbound_units']:,.0f} units already committed "
                     "in REQUESTED/IN_TRANSIT transfers are excluded from donor "
                     "stock.", size=11, color=T.TEXT_MUTED)
             if t.get("pending_outbound_units") else ft.Container()),
            ft.Container(height=12),
            T.section_header("Store-Level Inventory Health", "📊"),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("Store", False), ("SKUs", True),
                                        ("Overstock", True), ("Deficits", True),
                                        ("Push from", True), ("Risk", True),
                                        ("Status", False))],
                rows=health_rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=14, expand=True,
            ),
            ft.Container(height=14),
            T.section_header("Recommended Item-Level Transfers", "🔄"),
            ft.Row([
                T.metric_card("Total Transfer Value", _money(t["total_value"]),
                              status="success"),
                T.metric_card("Unique SKUs", f"{t['unique_skus']:,}", status="info"),
                T.metric_card("Store Pairs", f"{t['store_pairs']:,}", status="info"),
            ], spacing=12, expand=True),
            ft.Container(height=8),
            (ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("Type", False), ("Product", False),
                                        ("From", False), ("To", False),
                                        ("Qty", True), ("Donor cover", True),
                                        ("Rcpt cover", True), ("Value", True),
                                        ("Dept", False))],
                rows=opp_rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=14, expand=True,
             ) if opp_rows else
             ft.Text("No profitable transfer opportunities right now — either "
                     "every store is well stocked, or no donor has excess above "
                     "its safety stock.", size=12, color=T.TEXT_MUTED)),
            (ft.Text(f"Showing {len(opp_rows)} of {len(opps):,} opportunities. "
                     f"{t['manual_only']} fresh line(s) are shown but never "
                     "auto-queued.", size=11, color=T.TEXT_MUTED)
             if opps else ft.Container()),
            ft.Container(height=10),
            (ft.Row([limit, queue_btn, result], spacing=12,
                    vertical_alignment=ft.CrossAxisAlignment.CENTER)
             if opps else ft.Container()),
        ], spacing=8)

    scan_btn = ft.ElevatedButton("Scan network for transfers",
                                 icon=ft.Icons.TRAVEL_EXPLORE)

    def _on_scan(e):
        scan_btn.disabled = True
        scan_btn.text = "Scanning…"
        if page:
            page.update()
        body.content = _render(D.network_transfer_scan(project_root))
        scan_btn.disabled = False
        scan_btn.text = "Re-scan network"
        if page:
            page.update()

    scan_btn.on_click = _on_scan

    return T.card_container(content=ft.Column([
        T.section_header("Live Network Transfer Opportunities", "🌐"),
        ft.Text("Cross-store analysis: finds lines overstocked at one store "
                "that can plug a gap at another. Minimum-order failures become "
                "pull triggers; stock already in transit is excluded.",
                size=12, color=T.TEXT_SECONDARY),
        scan_btn,
        ft.Container(height=10),
        body,
    ], spacing=8))


def _status_controls(page, project_root: str, org: str) -> ft.Control:
    """Advance a queued transfer — the console's Update Status box."""
    tid = ft.TextField(label="Transfer ID", width=130, dense=True,
                       keyboard_type=ft.KeyboardType.NUMBER)
    status = ft.Dropdown(
        label="Status", width=170, dense=True,
        options=[ft.dropdown.Option(s) for s in D.TRANSFER_STATUSES],
        value=D.TRANSFER_STATUSES[0])
    out = ft.Text("", size=12, color=T.TEXT_SECONDARY)
    btn = ft.ElevatedButton("Update status", icon=ft.Icons.LOCAL_SHIPPING)

    def _on_update(e):
        try:
            n = int(tid.value)
        except (TypeError, ValueError):
            out.value = "Enter a transfer ID."
            out.color = T.WARNING
            if page: page.update()
            return
        try:
            username = page.session.get("username") or "system" if page else "system"
        except Exception:
            username = "system"
        res = D.set_transfer_status(n, status.value, username, org,
                                    root=project_root)
        out.value = (f"Transfer {n} marked {status.value}."
                     if res["success"] else res["error"])
        out.color = T.SUCCESS if res["success"] else T.DANGER
        if page: page.update()

    btn.on_click = _on_update
    return ft.Row([tid, status, btn, out], spacing=12,
                  vertical_alignment=ft.CrossAxisAlignment.CENTER)


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

    # ── Live network transfer opportunities ──────────────────────────────
    controls.append(_network_opportunities(page, project_root, org, stores))

    # ── Transfer execution ───────────────────────────────────────────────
    # Network-wide, not this store's: a transfer has two ends, and the console
    # passes org_filter=None for anyone who can view all stores. Filtering to
    # the first store hid every movement between the other four.
    xfer = D.transfer_status(None, project_root)
    if xfer.get("error"):
        controls.append(_warn_card("Transfer Execution & Status",
                                   f"Could not read transfers: {xfer['error']}"))
    elif not xfer["rows"]:
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Transfer Execution & Status", ""),
            ft.Text("No transfer records found.", size=12, color=T.TEXT_MUTED),
        ], spacing=8)))
    else:
        # Column names come from INTEGRATION_TRANSFER_ORDERS as the adapter
        # returns it. An earlier draft read ID / FROM_ORG / TO_ORG / QTY —
        # none of which exist — so every cell in this table rendered blank and
        # the operator had no transfer id to type into Update Status.
        status_colors = {"REQUESTED": T.WARNING, "IN_TRANSIT": T.TEAL,
                         "RECEIVED": T.SUCCESS}
        rows = []
        for r in xfer["rows"][:30]:
            sv = str(r.get("STATUS", ""))
            rows.append(ft.DataRow(cells=[
                _cell(r.get("TRANSFER_ID", ""), T.TEAL, bold=True),
                _cell(str(r.get("PRODUCT_NAME", ""))[:30], T.TEXT_PRIMARY),
                _cell(r.get("FROM_ORG_CD", "")),
                _cell(r.get("TO_ORG_CD", "")),
                _cell(f"{float(r.get('QUANTITY') or 0):,.0f}"),
                _cell(_money(r.get("VALUE_KES"))),
                _cell(r.get("URGENCY", "")),
                _cell(sv, status_colors.get(sv, T.TEXT_MUTED), bold=True),
                _cell(r.get("COMPLETED_DT") or "—"),
            ]))
        controls.append(T.card_container(content=ft.Column([
            T.section_header("Transfer Execution & Status", "🚚"),
            _status_controls(page, project_root, org),
            ft.Container(height=8),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("ID", True), ("Product", False),
                                        ("From", False), ("To", False),
                                        ("Qty", True), ("Value", True),
                                        ("Urgency", False), ("Status", False),
                                        ("Completed", False))],
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
