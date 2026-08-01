"""
O.A.S.I.S. Desktop — Operations view (Phase 2).

Ordering, Transfers, Suppliers and Allocation for the active store, reading the
SAME data the Streamlit Operations Console reads (oasis.desktop.data wraps the
one verified PosErpAdapter — no parallel queries, no invented API).

Where a capability has not been migrated yet, this view says so plainly instead
of drawing an empty panel that reads as "you have no data". A store with zero
pending orders and a console that cannot yet generate them must not look alike.
"""

import flet as ft

from .. import data as D
from .. import theme as T


def _money(v: float) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"


def _pending_state(pend: dict) -> tuple:
    """(value, status, sub) for the pending-PO card — distinguishes 0 from error."""
    if pend.get("error"):
        return "—", "danger", "could not read orders"
    return str(pend["count"]), ("info" if pend["count"] else "success"), \
        ("awaiting approval" if pend["count"] else "queue clear")


def _not_migrated(title: str, detail: str, uses: str) -> ft.Container:
    """Honest placeholder: says what is missing and where it still lives."""
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


def _error_row(msg: str) -> ft.Container:
    return T.card_container(
        content=ft.Row([
            ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
            ft.Text(msg, size=12, color=T.TEXT_SECONDARY, expand=True),
        ], spacing=8),
    )


def _table(headers, rows, empty_msg: str) -> ft.Control:
    if not rows:
        return ft.Text(empty_msg, size=12, color=T.TEXT_MUTED)
    return ft.Column([
        ft.Row([ft.Text(h, size=11, color=T.TEXT_MUTED, weight=ft.FontWeight.W_600,
                        expand=True, font_family="JetBrains Mono")
                for h in headers], spacing=8),
        ft.Divider(height=1, color=T.OBSIDIAN_BORDER),
        *[ft.Row([ft.Text(str(c), size=12, color=T.TEXT_SECONDARY, expand=True)
                  for c in r], spacing=8) for r in rows],
    ], spacing=6)


def build_ops_view(page: ft.Page, project_root: str) -> ft.Column:
    """Construct the Operations view for the active store."""
    stores = D.list_stores(project_root)
    prov = D.data_provenance(project_root)

    if not stores:
        return ft.Column([
            T.spec_tag("OPERATIONS CONSOLE", hot=True),
            ft.Container(height=16),
            _error_row("No stores found in the active database. Complete "
                       "first-run setup, or check the data source in Settings."),
        ], expand=True, scroll=ft.ScrollMode.AUTO)

    org = stores[0]["org_cd"]
    stock = D.stock_overview(org, project_root)
    pend = D.pending_orders(org, project_root)
    sup = D.supplier_overview(org, project_root)
    multi = len(stores) > 1

    # ── Ordering ─────────────────────────────────────────────────────────
    pend_val, pend_status, pend_sub = _pending_state(pend)
    ordering = [
        ft.Row([
            T.metric_card("SKUs", f"{stock['skus']:,}", status="info",
                          help_text="Catalogue lines carried by this store."),
            T.metric_card("Stockouts", f"{stock['stockouts']:,}",
                          status="danger" if stock["stockouts"] else "success",
                          sub="on hand = 0"),
            T.metric_card("Low stock", f"{stock['low_stock']:,}",
                          status="warning" if stock["low_stock"] else "success",
                          sub="fewer than 5 units"),
            T.metric_card("Stock value", _money(stock["stock_value"]),
                          status="info", sub="at weighted average cost"),
        ], spacing=12, expand=True),
        ft.Container(height=8),
        ft.Row([T.metric_card("Pending POs", pend_val, status=pend_status,
                              sub=pend_sub)], spacing=12, expand=True),
    ]
    if stock.get("error"):
        ordering.append(_error_row(f"Stock read failed: {stock['error']}"))
    ordering.append(_not_migrated(
        "PO generation not yet in the desktop app",
        "Generating a purchase order (engine → network → MOQ gate) and pushing "
        "it to approvals still runs in the Operations Console.",
        "OASIS.bat → 2  ·  entrypoint.py --mode shell"))

    # ── Transfers ────────────────────────────────────────────────────────
    if multi:
        transfers = [
            ft.Text(f"{len(stores)} stores in this network.", size=13,
                    color=T.TEXT_SECONDARY),
            _table(["Store", "Code"],
                   [(s["name"], s["org_cd"]) for s in stores],
                   "No stores."),
            _not_migrated(
                "Transfer proposals not yet in the desktop app",
                "Inter-store transfer generation and the consolidated transfer "
                "service still run in the Operations Console.",
                "OASIS.bat → 2  ·  entrypoint.py --mode shell"),
        ]
    else:
        transfers = [_not_migrated(
            "Transfers need more than one store",
            "This install has a single store, so there is nothing to transfer "
            "between. Multi-store installs get donor/receiver proposals here.",
            "Home → first-run setup → multi-store demo network")]

    # ── Suppliers ────────────────────────────────────────────────────────
    suppliers = [
        ft.Row([
            T.metric_card("Suppliers", f"{sup['suppliers']:,}", status="info",
                          help_text="Distinct vendors across the carried catalogue."),
        ], spacing=12, expand=True),
        ft.Container(height=8),
        T.section_header("Largest by SKU count", "◇"),
        _table(["Supplier", "SKUs"],
               [(s["name"], s["skus"]) for s in sup["top"]],
               "No supplier data in the catalogue."),
    ]
    if sup.get("error"):
        suppliers.append(_error_row(f"Supplier read failed: {sup['error']}"))

    # ── Allocation ───────────────────────────────────────────────────────
    eng = D.engine_posture(project_root)
    live = [n for n, on in (eng.get("engines") or {}).items() if on]
    allocation = [
        ft.Row([
            T.metric_card("Engines live", f"{len(live)}/{len(eng.get('engines') or {})}",
                          status="success" if live else "danger",
                          sub=", ".join(live) if live else "none active",
                          help_text="Chapter-11 engine layer (AMIT/LATA/DHARAM/MANDE)."),
            T.metric_card("Config tier", str(eng.get("tier", "?")).upper(),
                          status="info" if eng.get("tier") == "live" else "warning",
                          sub=eng.get("file") or "—",
                          help_text="'default' means shipped methodology defaults, untuned."),
        ], spacing=12, expand=True),
        _not_migrated(
            "Budget-constrained allocation not yet in the desktop app",
            "Two-pass allocation with efficiency guards still runs in the "
            "Operations Console and the Command Center.",
            "OASIS.bat → 2 / 4"),
    ]
    if eng.get("error"):
        allocation.append(_error_row(f"Engine config unreadable: {eng['error']}"))

    # ── Shell ────────────────────────────────────────────────────────────
    def _tab(label: str, icon: str, controls) -> ft.Tab:
        return ft.Tab(text=label, icon=icon,
                      content=ft.Container(
                          content=ft.Column(controls, spacing=10,
                                            scroll=ft.ScrollMode.AUTO),
                          padding=ft.padding.only(top=16)))

    header = [T.spec_tag("OPERATIONS CONSOLE", hot=True),
              ft.Text("Operations", size=28, weight=ft.FontWeight.W_700,
                      color=T.TEXT_PRIMARY)]
    if prov.get("is_sample"):
        header.append(ft.Container(
            content=ft.Text("◆ SAMPLE DATA — the built-in demo store, not your "
                            "live data.", size=11, color=T.WARNING,
                            font_family="JetBrains Mono"),
            bgcolor="#3A2A00", border_radius=8,
            padding=ft.padding.symmetric(horizontal=12, vertical=6)))
    else:
        header.append(ft.Text(
            f"▸ {prov.get('store_name') or stores[0]['name']}  ·  {prov.get('db','')}",
            size=11, color=T.TEXT_MUTED, font_family="JetBrains Mono"))

    return ft.Column(
        controls=[
            *header,
            ft.Container(height=8),
            ft.Tabs(selected_index=0, animation_duration=200,
                    indicator_color=T.TEAL, label_color=T.TEAL,
                    unselected_label_color=T.TEXT_MUTED,
                    tabs=[
                        _tab("Ordering", ft.Icons.INVENTORY_2_OUTLINED, ordering),
                        _tab("Transfers", ft.Icons.SWAP_HORIZ, transfers),
                        _tab("Suppliers", ft.Icons.LOCAL_SHIPPING_OUTLINED, suppliers),
                        _tab("Allocation", ft.Icons.GRID_VIEW, allocation),
                    ], expand=True),
        ],
        spacing=6, expand=True,
    )
