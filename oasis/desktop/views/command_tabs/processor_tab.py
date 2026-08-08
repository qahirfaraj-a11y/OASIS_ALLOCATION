"""
Batch Inventory Processor for the Flet Command Center.

Parity target: ops_dashboard.py's "🚀 OASIS Processor" tab — upload picking
lists or GRN files, run them through parse → enrich → decision engine, get an
Excel report per file.

Native difference, on purpose: the console uploads into the browser and hands
back a download button. A desktop app has a filesystem, so this picks files
where they already are and writes the reports to a folder the operator can
open. Same pipeline, one less round trip.
"""
import os

import flet as ft

from ... import theme as T
from ... import data as D


def build_processor_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    org = stores[0]["org_cd"] if stores else "ORG001"

    chosen: list = []
    picked_text = ft.Text("No files selected.", size=12, color=T.TEXT_MUTED)
    results = ft.Container()
    run_btn = ft.ElevatedButton("Process all files", icon=ft.Icons.PLAY_ARROW,
                                bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN,
                                disabled=True)

    def _username() -> str:
        try:
            return page.session.get("username") or "system" if page else "system"
        except Exception:
            return "system"

    def _on_pick(e: ft.FilePickerResultEvent):
        chosen.clear()
        chosen.extend(f.path for f in (e.files or []) if f.path)
        if chosen:
            picked_text.value = f"{len(chosen)} file(s) selected: " + ", ".join(
                os.path.basename(p) for p in chosen[:4]) + (
                " …" if len(chosen) > 4 else "")
            picked_text.color = T.TEXT_SECONDARY
            run_btn.disabled = False
        else:
            picked_text.value = "No files selected."
            picked_text.color = T.TEXT_MUTED
            run_btn.disabled = True
        if page:
            page.update()

    picker = ft.FilePicker(on_result=_on_pick)
    # The picker must be in the page overlay to open; when it cannot be
    # attached (headless tests) the tab still builds and simply cannot browse.
    if page is not None:
        try:
            page.overlay.append(picker)
            page.update()
        except Exception:
            pass

    def _browse(e):
        try:
            picker.pick_files(allow_multiple=True,
                              allowed_extensions=list(D.PROCESSOR_EXTENSIONS))
        except Exception as ex:
            picked_text.value = f"File picker unavailable: {ex}"
            picked_text.color = T.DANGER
            if page:
                page.update()

    def _render(res: dict) -> ft.Control:
        if res.get("error"):
            return ft.Row([
                ft.Icon(ft.Icons.ERROR_OUTLINE, size=18, color=T.DANGER),
                ft.Text(f"Processor unavailable: {res['error']}", size=12,
                        color=T.TEXT_SECONDARY, expand=True),
            ], spacing=8)

        rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Icon(
                    ft.Icons.CHECK_CIRCLE if not r["error"]
                    else ft.Icons.ERROR_OUTLINE, size=16,
                    color=T.SUCCESS if not r["error"] else T.DANGER)),
                ft.DataCell(ft.Text(r["file"][:36], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(f"{r['products']:,}", size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(f"{r['recommendations']:,}", size=11,
                                    color=T.SUCCESS if r["recommendations"]
                                    else T.TEXT_MUTED)),
                ft.DataCell(ft.Text(r["error"] or os.path.basename(
                    r["output"] or ""), size=11,
                    color=T.DANGER if r["error"] else T.TEXT_SECONDARY)),
            ]) for r in res["results"]
        ]

        return ft.Column([
            ft.Row([
                T.metric_card("Processed", f"{res['processed']:,}",
                              status="success"),
                T.metric_card("Failed", f"{res['failed']:,}",
                              status="danger" if res["failed"] else "success"),
                T.metric_card("Recommendations",
                              f"{sum(r['recommendations'] for r in res['results']):,}",
                              status="info"),
            ], spacing=12, expand=True),
            ft.Container(height=10),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=num)
                         for h, num in (("", False), ("File", False),
                                        ("Products", True),
                                        ("Recommendations", True),
                                        ("Report / error", False))],
                rows=rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=16, expand=True,
            ),
            ft.Text(f"Reports written to {res['out_dir']}", size=11,
                    color=T.TEXT_MUTED, font_family="JetBrains Mono",
                    selectable=True),
        ], spacing=8)

    def _on_run(e):
        if not chosen:
            return
        run_btn.disabled = True
        run_btn.text = "Processing…"
        if page:
            page.update()
        res = D.process_inventory_files(list(chosen), _username(), org,
                                        root=project_root)
        results.content = _render(res)
        run_btn.disabled = False
        run_btn.text = "Process all files"
        if page:
            page.update()

    run_btn.on_click = _on_run

    return ft.Column(
        controls=[
            ft.Text("Batch Inventory Processor", size=20,
                    weight=ft.FontWeight.W_600, color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            T.card_container(content=ft.Column([
                T.section_header("Picking lists & GRN files", "🚀"),
                ft.Text("Parse → enrich with intelligence → decision engine → "
                        "an Excel report per file. "
                        f"Accepts {', '.join(D.PROCESSOR_EXTENSIONS)}.",
                        size=12, color=T.TEXT_SECONDARY),
                ft.Row([
                    ft.ElevatedButton("Choose files…", icon=ft.Icons.FOLDER_OPEN,
                                      on_click=_browse),
                    run_btn,
                ], spacing=12),
                picked_text,
            ], spacing=8)),
            ft.Container(height=12),
            results,
        ],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
