"""
Smart Ordering tab for the Flet Command Center.

The native Command Center's ordering surface: generate → review → push →
approve. Every read and every write goes through ``oasis.desktop.data``, which
wraps the one verified adapter and writes to the SAME tables the Streamlit
console writes to. This tab must never re-derive the pipeline itself — an
earlier draft imported ``simulation_bridge`` and ``gnn_service`` directly and
became a second source of truth for what a purchase order is.

Licensing is applied by ``command_view`` (module ``ordering``) before this
builder is ever called.
"""
import logging
import flet as ft

from ... import theme as T
from ... import data as D

logger = logging.getLogger("OASIS.Desktop.SmartOrdering")


def _money(v) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _qty(r) -> float:
    try:
        return float(r.get("recommended_quantity", 0) or 0)
    except (TypeError, ValueError):
        return 0.0


def _error_card(title: str, msg: str) -> ft.Container:
    return T.card_container(content=ft.Column([
        T.section_header(title, ""),
        ft.Row([
            ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
            ft.Text(msg, size=12, color=T.TEXT_SECONDARY, expand=True),
        ], spacing=8),
    ], spacing=8))


def build_smart_ordering_tab(page: ft.Page, project_root: str) -> ft.Column:
    """Smart Ordering tab content."""

    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.", color=T.TEXT_SECONDARY)])

    org = stores[0]["org_cd"]
    store_name = stores[0]["name"]

    def _username() -> str:
        try:
            return page.session.get("username") or "system"
        except Exception:          # headless (tests) — page may be None
            return "system"

    controls = [
        ft.Text(f"Smart Ordering — {store_name}",
                size=20, weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
        ft.Container(height=8),
    ]

    # ── Generate → review → push ─────────────────────────────────────────
    gen_result = ft.Container()

    def _render_recs(res: dict) -> ft.Control:
        po_recs = [r for r in res.get("po_recs", []) if _qty(r) > 0]
        blocked = [r for r in res.get("po_recs", []) if _qty(r) <= 0]
        dropped = res.get("dropped_recs", []) or []

        if not po_recs:
            return ft.Column([
                ft.Text("No orders recommended at this time.",
                        size=12, color=T.TEXT_MUTED),
                ft.Text(f"{len(blocked)} items evaluated and not ordered; "
                        f"{len(dropped)} dropped by the minimum-order gate.",
                        size=11, color=T.TEXT_MUTED),
            ], spacing=6)

        total_qty = sum(_qty(r) for r in po_recs)
        total_cost = sum(float(r.get("cost_est", 0) or 0) for r in po_recs)

        # Supplier grouping — the Command Center's reason to exist over /ops:
        # a buyer places orders one supplier at a time.
        by_supplier: dict = {}
        for r in po_recs:
            key = (r.get("supplier_name") or r.get("SUPPLIER_NAME")
                   or r.get("supplier_cd") or "Unassigned")
            by_supplier.setdefault(key, []).append(r)

        sup_rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(str(name)[:32], size=11, color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(str(len(items)), size=11, color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(f"{sum(_qty(i) for i in items):,.0f}",
                                    size=11, color=T.SUCCESS)),
                ft.DataCell(ft.Text(_money(sum(float(i.get("cost_est", 0) or 0)
                                               for i in items)),
                                    size=11, color=T.TEXT_SECONDARY)),
            ])
            for name, items in sorted(by_supplier.items(),
                                      key=lambda kv: -sum(_qty(i) for i in kv[1]))
        ]

        push_btn = ft.ElevatedButton(
            "Push to PENDING Approvals", icon=ft.Icons.CLOUD_UPLOAD,
            bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN)

        def _on_push(ev):
            push_btn.disabled = True
            push_btn.text = "Pushing…"
            _update()
            res_push = D.push_purchase_order(org, _username(), po_recs,
                                             project_root)
            if res_push.get("success"):
                push_btn.text = f"Pushed {res_push['pushed_count']} items"
                push_btn.icon = ft.Icons.CHECK
            else:
                push_btn.text = f"Push failed: {res_push.get('error') or 'unknown'}"
                push_btn.icon = ft.Icons.ERROR_OUTLINE
                push_btn.bgcolor = T.DANGER
            _update()

        push_btn.on_click = _on_push

        return ft.Column([
            ft.Row([
                T.metric_card("Items to Order", f"{len(po_recs):,}", status="success"),
                T.metric_card("Total Qty", f"{total_qty:,.0f}", status="info"),
                T.metric_card("Est. Cost", _money(total_cost), status="warning"),
                T.metric_card("Suppliers", f"{len(by_supplier):,}", status="info"),
            ], spacing=12, expand=True),
            ft.Container(height=8),
            ft.Text(f"Generated at {res.get('generated_at', '—')}",
                    size=11, color=T.TEXT_MUTED, font_family="JetBrains Mono"),
            ft.DataTable(
                columns=[
                    ft.DataColumn(ft.Text("Supplier", size=11, color=T.TEXT_MUTED)),
                    ft.DataColumn(ft.Text("Lines", size=11, color=T.TEXT_MUTED), numeric=True),
                    ft.DataColumn(ft.Text("Qty", size=11, color=T.TEXT_MUTED), numeric=True),
                    ft.DataColumn(ft.Text("Est. Cost", size=11, color=T.TEXT_MUTED), numeric=True),
                ],
                rows=sup_rows,
                heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN,
                column_spacing=16,
                expand=True,
            ),
            ft.Text(f"{len(blocked)} items not recommended; {len(dropped)} "
                    "dropped by the minimum-order gate.",
                    size=11, color=T.TEXT_MUTED),
            ft.Container(height=10),
            push_btn,
        ], spacing=8)

    def _update():
        if page is not None:
            page.update()

    gen_btn = ft.ElevatedButton("Generate Orders", icon=ft.Icons.AUTO_AWESOME)

    def _on_generate(e):
        gen_btn.disabled = True
        gen_btn.text = "Generating…"
        _update()
        res = D.generate_smart_orders(org, root=project_root)
        if res.get("error"):
            gen_result.content = _error_card("Ordering Pipeline",
                                             f"Generation failed: {res['error']}")
        else:
            gen_result.content = _render_recs(res)
        gen_btn.disabled = False
        gen_btn.text = "Regenerate Orders"
        _update()

    gen_btn.on_click = _on_generate

    controls.append(T.card_container(content=ft.Column([
        T.section_header("Smart Ordering (PO Generation)", "🚀"),
        ft.Text("Engine → Network Intelligence → MOQ Gate. Nothing is written "
                "until you push the result to approvals.",
                size=12, color=T.TEXT_SECONDARY),
        gen_btn,
        ft.Container(height=10),
        gen_result,
    ], spacing=8)))

    # ── Pending approvals — approve / reject ─────────────────────────────
    pend = D.pending_orders(org, project_root)
    approvals = ft.Column(spacing=4)

    if pend.get("error"):
        controls.append(_error_card("Pending Approvals",
                                    f"Could not read orders: {pend['error']}"))
    else:
        def _decide(po_id, status, row):
            res = D.update_po_status(po_id, status, _username(), org,
                                     root=project_root)
            if res.get("success"):
                row.controls[-2].disabled = True
                row.controls[-1].disabled = True
                row.controls[0].value = f"{row.controls[0].value}  →  {status}"
                row.controls[0].color = (T.SUCCESS if status == "APPROVED"
                                         else T.TEXT_MUTED)
            else:
                row.controls[0].value = (f"{row.controls[0].value}  →  failed: "
                                         f"{res.get('error') or 'unknown'}")
                row.controls[0].color = T.DANGER
            _update()

        for p_row in pend.get("rows", []):
            pid = p_row.get("PO_ID") or p_row.get("po_id")
            label = ft.Text(
                f"PO #{pid} · {p_row.get('SUPPLIER_CD', '—')} · "
                f"{str(p_row.get('PRODUCT_NAME', ''))[:34]} · "
                f"Qty {p_row.get('QUANTITY', 0)}",
                size=12, color=T.TEXT_PRIMARY, expand=True)
            row = ft.Row([label])
            row.controls.append(ft.IconButton(
                ft.Icons.CHECK_CIRCLE, icon_color=T.SUCCESS, tooltip="Approve",
                on_click=lambda e, i=pid, r=row: _decide(i, "APPROVED", r)))
            row.controls.append(ft.IconButton(
                ft.Icons.CANCEL, icon_color=T.DANGER, tooltip="Reject",
                on_click=lambda e, i=pid, r=row: _decide(i, "REJECTED", r)))
            approvals.controls.append(row)

        if not approvals.controls:
            approvals.controls.append(
                ft.Text("No purchase orders awaiting approval.",
                        size=12, color=T.TEXT_MUTED))

        controls.append(T.card_container(content=ft.Column([
            T.section_header("Pending Approvals", "✅"),
            ft.Text(f"{pend['count']} awaiting approval.", size=12,
                    color=T.WARNING if pend["count"] else T.TEXT_MUTED),
            approvals,
        ], spacing=8)))

    return ft.Column(
        controls=controls,
        spacing=10,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
