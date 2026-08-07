"""
O.A.S.I.S. Desktop — the licensing surface (Phase 3, P3.0).

The four Streamlit consoles have called ``console_gate`` since Wave 2a; the
native window never did. It read ``license_posture()`` for a status chip and
then rendered store data regardless — so an expired trial locked every browser
console while ``--mode desktop``, the front door OASIS.bat calls RECOMMENDED,
opened straight into the store (Phase 3 finding R-2).

This module is the desktop half of the gate. It renders the decision made by
``license_manager.gate_status`` — it does not make its own — so the two front
doors cannot drift into disagreeing about what a client may do.

The lock screen keeps the same three doors the console lock screen has always
had (audit E1/E3): activate a key, take your data, see what a license buys. A
locked client is never a trapped client, and their records are never hostage.
"""

import os

import flet as ft

from .. import theme as T


# ── shared pieces ────────────────────────────────────────────────────────
_NOTICE_COLOR = {"error": T.DANGER, "warning": T.WARNING, "info": T.TEXT_MUTED}
_NOTICE_BG = {"error": "#3A0A16", "warning": "#3A2A00", "info": T.OBSIDIAN_RAISE}


def build_notice(status: dict) -> ft.Container:
    """The evaluation banner / renewal warning / quiet licensed caption.

    Returns an empty (zero-height) container for the quiet case rather than
    None, so callers can place it unconditionally.
    """
    notice = (status or {}).get("notice")
    if not notice:
        return ft.Container(height=0)
    level, text = notice
    if level == "info":
        return ft.Container(
            content=ft.Text(text, size=10, color=T.TEXT_MUTED,
                            font_family="JetBrains Mono"),
            padding=ft.padding.only(bottom=6))
    return ft.Container(
        content=ft.Row([
            ft.Icon(ft.Icons.WARNING_AMBER_ROUNDED, size=16,
                    color=_NOTICE_COLOR.get(level, T.WARNING)),
            ft.Text(text, size=12, color=_NOTICE_COLOR.get(level, T.WARNING),
                    font_family="JetBrains Mono", expand=True),
        ], spacing=8),
        bgcolor=_NOTICE_BG.get(level, T.OBSIDIAN_RAISE),
        border_radius=8,
        padding=ft.padding.symmetric(horizontal=12, vertical=8),
        margin=ft.margin.only(bottom=10))


def build_upsell(module: str) -> ft.Container:
    """The locked-feature stub — a sales surface, not a dead end.

    The native twin of ``license_manager.render_upsell``: same promise, same
    reassurance that the data is already being collected.
    """
    from oasis.logic.license_manager import MODULE_LABELS
    label = MODULE_LABELS.get(module, module.title())
    return T.card_container(
        content=ft.Column([
            ft.Icon(ft.Icons.LOCK_OUTLINE, size=34, color=T.WARNING),
            ft.Text(f"{label} module", size=19, weight=ft.FontWeight.W_700,
                    color=T.TEXT_PRIMARY, text_align=ft.TextAlign.CENTER),
            ft.Text(f"This capability is part of the {label} module, which is "
                    "not included in your current license. Your data is "
                    "already being collected — activation is immediate once "
                    "licensed.",
                    size=12, color=T.TEXT_SECONDARY,
                    text_align=ft.TextAlign.CENTER),
            ft.Text(f"Contact iLink to activate {label}.",
                    size=12, weight=ft.FontWeight.W_600, color=T.TEAL,
                    text_align=ft.TextAlign.CENTER),
        ], spacing=8, horizontal_alignment=ft.CrossAxisAlignment.CENTER))


# ── the three doors ──────────────────────────────────────────────────────
def build_activation_panel(page, on_activated=None) -> ft.Column:
    """Paste a key and unlock in place — no file surgery, no restart (audit E2).

    Used by the lock screen AND by Settings, so a client who buys on day 9 can
    activate without waiting to be locked out first.
    """
    paste = ft.TextField(
        label="Paste the contents of oasis_license.key",
        multiline=True, min_lines=5, max_lines=8, width=560,
        border_color=T.OBSIDIAN_BORDER, focused_border_color=T.TEAL,
        color=T.TEXT_PRIMARY, text_size=12,
        label_style=ft.TextStyle(color=T.TEXT_SECONDARY),
        hint_text='{"tenant_id": "...", "authorized_modules": {...}}')
    result = ft.Text("", size=12, color=T.DANGER)

    def _load_from_file(path: str):
        try:
            with open(path, "r", encoding="utf-8", errors="replace") as f:
                paste.value = f.read()
            result.value = f"Loaded {os.path.basename(path)} — review, then activate."
            result.color = T.TEXT_SECONDARY
        except OSError as e:
            result.value = f"Could not read that file: {e}"
            result.color = T.DANGER
        if page:
            page.update()

    picker = ft.FilePicker(
        on_result=lambda e: (_load_from_file(e.files[0].path)
                             if getattr(e, "files", None) else None))
    if page is not None:
        try:
            page.overlay.append(picker)
            page.update()
        except Exception:      # no window yet (tests build the tree headless)
            pass

    def _browse(e):
        try:
            picker.pick_files(allow_multiple=False,
                              allowed_extensions=["key", "json"])
        except Exception as ex:
            result.value = f"File dialog unavailable: {ex}. Paste the key instead."
            result.color = T.WARNING
            if page:
                page.update()

    def _activate(e):
        from oasis.logic.license_manager import activate_key
        ok, detail = activate_key(paste.value or "")
        result.value = (f"✓ License activated. {detail}" if ok else detail)
        result.color = T.SUCCESS if ok else T.DANGER
        if page:
            page.update()
        if ok and on_activated:
            on_activated()

    return ft.Column([
        ft.Text("Already purchased? Activate here — no file copying needed.",
                size=12, color=T.TEXT_SECONDARY),
        ft.Row([
            ft.OutlinedButton("Choose key file…", icon=ft.Icons.FOLDER_OPEN,
                              on_click=_browse),
        ], spacing=8),
        paste,
        ft.ElevatedButton("Activate license", on_click=_activate,
                          style=ft.ButtonStyle(bgcolor=T.TEAL,
                                               color=T.DEEP_OBSIDIAN)),
        result,
    ], spacing=10, scroll=ft.ScrollMode.AUTO)


def _export_door(page, project_root: str) -> ft.Column:
    """Your data is yours, licensed or not (audit E1/E3).

    A native window has no browser download, so this copies the files where the
    operator asks. When no folder can be chosen it still writes them, under
    ``exports/`` beside the install, and says exactly where they went.
    """
    from oasis.logic.license_manager import copy_exports, lock_screen_exports

    exports = lock_screen_exports(project_root)
    result = ft.Text("", size=12, color=T.TEXT_SECONDARY)

    def _do_copy(dest: str):
        try:
            written = copy_exports(dest, project_root)
        except OSError as e:
            result.value = f"Copy failed: {e}"
            result.color = T.DANGER
        else:
            result.value = (("Copied:\n" + "\n".join(written))
                            if written else "Nothing to export yet.")
            result.color = T.SUCCESS if written else T.TEXT_MUTED
        if page:
            page.update()

    picker = ft.FilePicker(
        on_result=lambda e: (_do_copy(e.path) if getattr(e, "path", None)
                             else None))
    if page is not None:
        try:
            page.overlay.append(picker)
            page.update()
        except Exception:
            pass

    def _choose(e):
        try:
            picker.get_directory_path(dialog_title="Where should OASIS put your data?")
        except Exception:
            _do_copy(os.path.join(project_root, "exports"))

    rows = []
    for label, key in (("Store database", "db"), ("Latest Value Report", "report")):
        path = exports.get(key)
        rows.append(ft.Row([
            ft.Icon(ft.Icons.CHECK_CIRCLE_OUTLINE if path
                    else ft.Icons.REMOVE_CIRCLE_OUTLINE,
                    size=16, color=T.SUCCESS if path else T.TEXT_MUTED),
            ft.Text(label, size=12, color=T.TEXT_SECONDARY, width=160),
            ft.Text(os.path.basename(path) if path else "not present yet",
                    size=11, color=T.TEXT_MUTED, font_family="JetBrains Mono",
                    expand=True),
        ], spacing=8))

    return ft.Column([
        ft.Text("Your data is yours, licensed or not. Take a full copy any time.",
                size=12, color=T.TEXT_SECONDARY),
        *rows,
        ft.ElevatedButton("Copy my data to a folder…", icon=ft.Icons.DOWNLOAD,
                          on_click=_choose,
                          style=ft.ButtonStyle(bgcolor=T.TEAL,
                                               color=T.DEEP_OBSIDIAN)),
        result,
    ], spacing=10, scroll=ft.ScrollMode.AUTO)


def _includes_door() -> ft.Column:
    from oasis.logic.license_manager import BUNDLES, KNOWN_MODULES, MODULE_LABELS
    lines = [
        ft.Row([
            ft.Text("◆", size=12, color=T.TEAL),
            ft.Text(MODULE_LABELS.get(m, m)
                    + (" — mandatory base" if m == "core" else ""),
                    size=13, color=T.TEXT_SECONDARY),
        ], spacing=8)
        for m in KNOWN_MODULES
    ]
    bundles = " · ".join(f"{n} ({len(ms)} modules)" for n, ms in BUNDLES.items())
    return ft.Column([
        T.section_header("Modules", "◇"),
        *lines,
        ft.Container(height=8),
        T.section_header("Bundles", "◇"),
        ft.Text(bundles, size=12, color=T.TEXT_SECONDARY,
                font_family="JetBrains Mono"),
        ft.Container(height=8),
        ft.Text("Contact iLink for a key — activation is immediate, and the "
                "data OASIS collected during your trial lights up in full the "
                "moment it lands.",
                size=12, color=T.TEXT_MUTED),
    ], spacing=8, scroll=ft.ScrollMode.AUTO)


def build_lock_view(page, project_root: str, status: dict,
                    on_activated=None) -> ft.Column:
    """The day-15 (or expiry) surface for the native window.

    Never a dead end: activate, export, or read what a license buys.
    """
    reason = (status or {}).get("reason") or "evaluation period ended"
    return ft.Column(
        controls=[
            ft.Container(height=24),
            ft.Column([
                T.spec_tag("LICENSE REQUIRED", hot=True),
                ft.Container(height=12),
                ft.Icon(ft.Icons.LOCK, size=44, color=T.DANGER),
                ft.Text("O.A.S.I.S. is locked", size=28,
                        weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY,
                        text_align=ft.TextAlign.CENTER),
                ft.Text(reason, size=13, color=T.TEXT_SECONDARY,
                        font_family="JetBrains Mono",
                        text_align=ft.TextAlign.CENTER),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=4),
            ft.Container(height=16),
            ft.Tabs(selected_index=0, animation_duration=200,
                    indicator_color=T.TEAL, label_color=T.TEAL,
                    unselected_label_color=T.TEXT_MUTED,
                    tabs=[
                        ft.Tab(text="Activate", icon=ft.Icons.KEY,
                               content=ft.Container(
                                   content=build_activation_panel(page, on_activated),
                                   padding=ft.padding.only(top=16))),
                        ft.Tab(text="Export my data", icon=ft.Icons.INVENTORY_2_OUTLINED,
                               content=ft.Container(
                                   content=_export_door(page, project_root),
                                   padding=ft.padding.only(top=16))),
                        ft.Tab(text="What a license includes",
                               icon=ft.Icons.RECEIPT_LONG_OUTLINED,
                               content=ft.Container(
                                   content=_includes_door(),
                                   padding=ft.padding.only(top=16))),
                    ], expand=True),
        ],
        expand=True, spacing=4,
    )
