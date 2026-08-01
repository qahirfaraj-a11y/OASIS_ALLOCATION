"""
O.A.S.I.S. Desktop Application — Single Native Window.

One process. One window. Left-rail navigation. Single auth gate.
No browser, no localhost, no port numbers.

Launch:
    python -m oasis.desktop.app
    python entrypoint.py --mode desktop
"""

import os
import sys

# Ensure project root is on sys.path for oasis.logic imports
_PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__))))
if _PROJECT_ROOT not in sys.path:
    sys.path.insert(0, _PROJECT_ROOT)

import flet as ft  # noqa: E402  (imports follow the sys.path bootstrap above)

from . import theme as T  # noqa: E402
from .views.home_view import build_home_view  # noqa: E402
from .views.settings_view import build_settings_view  # noqa: E402


# ── Navigation Destinations ──────────────────────────────────────────────
NAV_ITEMS = [
    {"icon": ft.Icons.HOME_OUTLINED,      "selected": ft.Icons.HOME,
     "label": "Home",      "route": "/"},
    {"icon": ft.Icons.DASHBOARD_OUTLINED, "selected": ft.Icons.DASHBOARD,
     "label": "Operations", "route": "/ops"},
    {"icon": ft.Icons.SHIELD_OUTLINED,    "selected": ft.Icons.SHIELD,
     "label": "Command",    "route": "/command"},
    {"icon": ft.Icons.INSIGHTS_OUTLINED,  "selected": ft.Icons.INSIGHTS,
     "label": "Intel",      "route": "/intel"},
    {"icon": ft.Icons.TRENDING_UP_OUTLINED, "selected": ft.Icons.TRENDING_UP,
     "label": "Market",     "route": "/market"},
    {"icon": ft.Icons.SETTINGS_OUTLINED,  "selected": ft.Icons.SETTINGS,
     "label": "Settings",   "route": "/settings"},
]


def _placeholder_view(title: str, icon: str) -> ft.Column:
    """Temporary placeholder for views not yet migrated."""
    return ft.Column(
        controls=[
            T.spec_tag(f"{title.upper()} CONSOLE", hot=True),
            ft.Container(height=40),
            ft.Column([
                ft.Icon(ft.Icons.CONSTRUCTION, size=64, color=T.TEAL),
                ft.Text(f"{icon} {title}", size=28,
                        weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY),
                ft.Text("This console is being migrated from Streamlit to native desktop.",
                        size=14, color=T.TEXT_SECONDARY),
                ft.Text("Phase 2 of the desktop migration will bring this view to life.",
                        size=12, color=T.TEXT_MUTED),
                ft.Container(height=20),
                ft.Row([
                    T.metric_card("Status", "IN PROGRESS", status="warning"),
                    T.metric_card("Phase", "2", status="info"),
                    T.metric_card("Engine Layer", "ACTIVE ✓", status="success",
                                  help_text="The logic layer is fully operational. Only the UI surface is pending migration."),
                ], spacing=12, expand=True),
            ], horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=12),
        ],
        expand=True,
        scroll=ft.ScrollMode.AUTO,
    )


def _build_login_view(page: ft.Page, on_login) -> ft.Column:
    """Login gate — shown before any console access."""
    username_field = ft.TextField(
        label="Username", autofocus=True,
        border_color=T.OBSIDIAN_BORDER,
        focused_border_color=T.TEAL,
        color=T.TEXT_PRIMARY,
        label_style=ft.TextStyle(color=T.TEXT_SECONDARY),
        width=320,
    )
    password_field = ft.TextField(
        label="Password", password=True, can_reveal_password=True,
        border_color=T.OBSIDIAN_BORDER,
        focused_border_color=T.TEAL,
        color=T.TEXT_PRIMARY,
        label_style=ft.TextStyle(color=T.TEXT_SECONDARY),
        width=320,
        on_submit=lambda e: _do_login(e),
    )
    error_text = ft.Text("", color=T.DANGER, size=12)

    def _do_login(e):
        try:
            from oasis.logic.auth_manager import authenticate
            from oasis.logic.onboarding import resolved_db_path
            db = resolved_db_path(_PROJECT_ROOT)
            user = authenticate(username_field.value.strip(),
                                password_field.value, db)
            if user:
                page.session.set("username", user["username"])
                page.session.set("role", user.get("role", "ops_admin"))
                page.session.set("authenticated", True)
                on_login()
            else:
                error_text.value = "Invalid username or password."
                page.update()
        except Exception as ex:
            error_text.value = f"Auth error: {ex}"
            page.update()

    return ft.Column(
        controls=[
            ft.Container(height=80),
            ft.Column([
                T.spec_tag("AUDITED LOGIC_ENGINE_V2.3", hot=True),
                ft.Container(height=16),
                ft.Text("O . A . S . I . S .", size=32,
                        weight=ft.FontWeight.W_800, color=T.TEAL,
                        text_align=ft.TextAlign.CENTER),
                ft.Text("Algorithmic Retail Systems & Logic",
                        size=13, color=T.TEXT_MUTED,
                        font_family="JetBrains Mono",
                        style=ft.TextStyle(letter_spacing=2.0),
                        text_align=ft.TextAlign.CENTER),
                ft.Container(height=32),
                username_field,
                password_field,
                ft.Container(height=8),
                ft.ElevatedButton(
                    "Sign In", width=320,
                    on_click=_do_login,
                    style=ft.ButtonStyle(
                        bgcolor=T.TEAL,
                        color=T.DEEP_OBSIDIAN,
                    ),
                ),
                error_text,
            ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER,
                spacing=8,
            ),
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        expand=True,
    )


def _build_set_password_view(page: ft.Page, on_done) -> ft.Column:
    """First-run: the operator chooses the administrator password.

    A packaged desktop .exe has no console, so the seeder's one-time password
    (logged, never persisted) is invisible to a real client. Rather than bake a
    known credential into source to work around that, first run asks for the
    password and seeds the accounts with it — the standard on-prem pattern.
    """
    pw1 = ft.TextField(label="Administrator password", password=True,
                       can_reveal_password=True, autofocus=True, width=320,
                       border_color=T.OBSIDIAN_BORDER, focused_border_color=T.TEAL,
                       color=T.TEXT_PRIMARY,
                       label_style=ft.TextStyle(color=T.TEXT_SECONDARY))
    pw2 = ft.TextField(label="Confirm password", password=True,
                       can_reveal_password=True, width=320,
                       border_color=T.OBSIDIAN_BORDER, focused_border_color=T.TEAL,
                       color=T.TEXT_PRIMARY,
                       label_style=ft.TextStyle(color=T.TEXT_SECONDARY))
    msg = ft.Text("", color=T.DANGER, size=12)

    def _save(e):
        if (pw1.value or "") != (pw2.value or ""):
            msg.value = "Passwords do not match."
            page.update()
            return
        if len(pw1.value or "") < 8:
            msg.value = "Password must be at least 8 characters."
            page.update()
            return
        try:
            from oasis.logic.auth_manager import (has_accounts, seed_users,
                                                  set_password)
            from oasis.logic.db_connector import ensure_oasis_tables
            from oasis.logic.onboarding import (mark_admin_password_set,
                                                resolved_db_path)
            db = resolved_db_path(_PROJECT_ROOT)
            # ensure_oasis_tables() also SEEDS the default accounts (with random
            # one-time passwords), so by this point they usually already exist —
            # rotate ops_admin rather than assuming we get to seed it.
            ensure_oasis_tables(db)
            if has_accounts(db):
                set_password(db, "ops_admin", pw1.value)
            else:
                seed_users(db, password=pw1.value)
            mark_admin_password_set(_PROJECT_ROOT)   # don't ask again
            page.session.set("username", "ops_admin")
            page.session.set("role", "ops_admin")
            page.session.set("authenticated", True)
            on_done()
        except Exception as ex:
            msg.value = f"Could not set the password: {ex}"
            page.update()

    pw2.on_submit = _save

    return ft.Column(
        controls=[
            ft.Container(height=80),
            ft.Column([
                T.spec_tag("FIRST-RUN SECURITY", hot=True),
                ft.Container(height=16),
                ft.Text("Set your administrator password", size=26,
                        weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY,
                        text_align=ft.TextAlign.CENTER),
                ft.Text("This creates the 'ops_admin' account for this install. "
                        "Nothing is shipped with a default password.",
                        size=12, color=T.TEXT_MUTED,
                        text_align=ft.TextAlign.CENTER),
                ft.Container(height=24),
                pw1, pw2,
                ft.Container(height=8),
                ft.ElevatedButton("Create account", width=320, on_click=_save,
                                  style=ft.ButtonStyle(bgcolor=T.TEAL,
                                                       color=T.DEEP_OBSIDIAN)),
                msg,
            ],
                horizontal_alignment=ft.CrossAxisAlignment.CENTER, spacing=8),
        ],
        horizontal_alignment=ft.CrossAxisAlignment.CENTER,
        expand=True,
    )


def main(page: ft.Page):
    """Main Flet entry point."""
    page.title = "O.A.S.I.S. — Algorithmic Retail Platform"
    page.theme_mode = ft.ThemeMode.DARK
    page.theme = T.build_theme()
    page.bgcolor = T.DEEP_OBSIDIAN
    page.padding = 0
    page.window_width = 1400
    page.window_height = 900
    page.window_min_width = 1000
    page.window_min_height = 700
    page.fonts = {
        "Inter": "https://fonts.googleapis.com/css2?family=Inter:wght@400;500;600;700;800&display=swap",
        "JetBrains Mono": "https://fonts.googleapis.com/css2?family=JetBrains+Mono:wght@400;500;700&display=swap",
    }

    # ── State ────────────────────────────────────────────────────────────
    content_area = ft.Container(expand=True, padding=24)
    nav_rail = ft.NavigationRail(
        selected_index=0,
        label_type=ft.NavigationRailLabelType.ALL,
        min_width=80,
        min_extended_width=200,
        bgcolor=T.OBSIDIAN_RAISE,
        indicator_color=T.TEAL,
        destinations=[
            ft.NavigationRailDestination(
                icon_content=ft.Icon(n["icon"], color=T.TEXT_MUTED),
                selected_icon_content=ft.Icon(n["selected"], color=T.TEAL),
                label_content=ft.Text(n["label"], size=10, color=T.TEXT_SECONDARY),
            )
            for n in NAV_ITEMS
        ],
    )

    # ── Auth Gate ────────────────────────────────────────────────────────
    def _needs_auth() -> bool:
        """Is a login required before this window shows any console?

        Mirrors the posture home_app._gate() settled on (finding S5): gate once
        there are accounts to sign in with, stay open before that so the
        first-run wizard is reachable on a store that has no users yet.

        Fails CLOSED. An unreadable store or a broken auth table used to return
        False here, which the boot sequence then treated as "first run" and
        handed the caller a synthetic ops_admin — an error path that granted
        full access. If we cannot prove no login is needed, we require one.
        """
        try:
            from oasis.logic.onboarding import is_onboarded
            if not is_onboarded(_PROJECT_ROOT):
                return False          # genuinely first run — no store, no users
        except Exception:
            return True               # cannot tell → demand credentials
        try:
            from oasis.logic.onboarding import resolved_db_path
            from oasis.ui.shell import ensure_seeded
            db = resolved_db_path(_PROJECT_ROOT)
            ensure_seeded(db)         # same first-run seed the consoles do
            from oasis.logic.auth_manager import get_all_users
            return bool(get_all_users(db))
        except Exception:
            return True               # onboarded but unreadable → fail closed

    def _render_view(route: str):
        """Build the view for the given route."""
        if route == "/" or route == "":
            return build_home_view(page, _PROJECT_ROOT)
        elif route == "/settings":
            return build_settings_view(page, _PROJECT_ROOT)
        elif route == "/ops":
            return _placeholder_view("Operations", "◎")
        elif route == "/command":
            return _placeholder_view("Command Center", "🔮")
        elif route == "/intel":
            return _placeholder_view("Intelligence", "⚡")
        elif route == "/market":
            return _placeholder_view("Market Intelligence", "📈")
        else:
            return _placeholder_view("Unknown", "?")

    def _show_app():
        """Render the full app shell with nav rail + content."""
        def on_nav_change(e):
            idx = e.control.selected_index
            route = NAV_ITEMS[idx]["route"]
            content_area.content = _render_view(route)
            page.update()

        nav_rail.on_change = on_nav_change
        content_area.content = _render_view("/")

        # Status bar at bottom
        status_bar = ft.Container(
            content=ft.Row([
                ft.Text("O.A.S.I.S. Platform", size=10, color=T.TEXT_MUTED,
                        font_family="JetBrains Mono"),
                ft.Text("·", size=10, color=T.OBSIDIAN_BORDER),
                ft.Text(f"Signed in as: {page.session.get('username', 'system')}",
                        size=10, color=T.TEXT_MUTED,
                        font_family="JetBrains Mono"),
            ], spacing=8),
            bgcolor=T.DEEP_OBSIDIAN,
            padding=ft.padding.symmetric(horizontal=20, vertical=6),
            border=ft.border.only(top=ft.BorderSide(1, T.OBSIDIAN_BORDER)),
        )

        page.clean()
        page.add(
            ft.Row(
                controls=[
                    nav_rail,
                    ft.VerticalDivider(width=1, color=T.OBSIDIAN_BORDER),
                    content_area,
                ],
                expand=True,
            ),
            status_bar,
        )
        page.update()

    def _needs_initial_password() -> bool:
        """Real store whose accounts hold passwords the operator never saw."""
        try:
            from oasis.logic.onboarding import needs_admin_password
            return needs_admin_password(_PROJECT_ROOT)
        except Exception:
            return False                  # can't tell → _needs_auth gates instead

    # ── Boot Sequence ────────────────────────────────────────────────────
    if page.session.contains_key("authenticated"):
        _show_app()
    elif _needs_initial_password():
        # Ordered BEFORE the login gate on purpose. A real store always has
        # accounts (ensure_oasis_tables seeds them), but their passwords are
        # random and were only logged — so showing a login form here would be
        # an unanswerable prompt. Ask for the password instead, then rotate.
        page.clean()
        page.add(_build_set_password_view(page, _show_app))
    elif _needs_auth():
        page.clean()
        page.add(_build_login_view(page, _show_app))
    else:
        # Genuine first run: no store at all, so there is nothing to sign in
        # to and the wizard must be reachable. The session is NOT authenticated,
        # so it must not carry an operator role — "ops_admin" here would hand an
        # unauthenticated window top privilege, and Settings resolves its
        # password-change target from this same username.
        if not page.session.contains_key("username"):
            page.session.set("username", "setup")
            page.session.set("role", "setup")
        _show_app()


if __name__ == "__main__":
    ft.app(target=main)
