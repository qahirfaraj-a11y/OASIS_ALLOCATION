"""
O.A.S.I.S. Desktop — Settings View.

Auth management, license posture, branding, and data source reset.
"""

import flet as ft

from .. import theme as T


def build_settings_view(page: ft.Page, project_root: str) -> ft.Column:
    """Construct the Settings view."""

    # ── License Section ──────────────────────────────────────────────────
    def _license_info():
        try:
            from oasis.logic.license_manager import license_posture
            p = license_posture()
            return p
        except Exception:
            return {"status": "unknown"}

    lic = _license_info()

    license_card = T.card_container(
        content=ft.Column([
            T.section_header("License", "🔑"),
            ft.Row([
                T.metric_card("Status", lic.get("status", "unknown").upper(),
                              status="success" if lic.get("status") == "licensed" else "warning"),
                T.metric_card("Days Remaining",
                              str(lic.get("days_remaining", "∞")),
                              status="success" if lic.get("days_remaining", 0) > 7 else "danger"),
            ], spacing=12, expand=True),
            ft.Text("Drop your license key as 'oasis_license.key' beside the application.",
                    size=12, color=T.TEXT_MUTED),
        ], spacing=12),
    )

    # ── Data Source Reset ────────────────────────────────────────────────
    def _current_source():
        try:
            from oasis.logic.onboarding import load_onboarding
            ob = load_onboarding()
            return ob.get("source", "none"), ob.get("store_name", "")
        except Exception:
            return "none", ""

    src, store = _current_source()

    def _reset_onboarding(e):
        try:
            from oasis.logic.onboarding import reset_onboarding
            reset_onboarding()
            page.snack_bar = ft.SnackBar(
                ft.Text("✅ Onboarding reset. Restart to re-run setup.", color=T.TEAL),
                bgcolor=T.OBSIDIAN_RAISE)
            page.snack_bar.open = True
            page.update()
        except Exception as ex:
            page.snack_bar = ft.SnackBar(
                ft.Text(f"❌ Reset failed: {ex}", color=T.DANGER),
                bgcolor=T.OBSIDIAN_RAISE)
            page.snack_bar.open = True
            page.update()

    data_card = T.card_container(
        content=ft.Column([
            T.section_header("Data Source", "🔌"),
            ft.Text(f"Current source: {src}" + (f"  ·  {store}" if store else ""),
                    size=14, color=T.TEXT_SECONDARY),
            ft.ElevatedButton("Re-run First-Run Setup", on_click=_reset_onboarding,
                               icon=ft.Icons.REFRESH,
                               style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                                    color=T.WARNING)),
        ], spacing=12),
    )

    # ── Password Change ──────────────────────────────────────────────────
    current_pw = ft.TextField(label="Current Password", password=True,
                               can_reveal_password=True,
                               border_color=T.OBSIDIAN_BORDER,
                               focused_border_color=T.TEAL,
                               color=T.TEXT_PRIMARY,
                               label_style=ft.TextStyle(color=T.TEXT_SECONDARY))
    new_pw = ft.TextField(label="New Password", password=True,
                           can_reveal_password=True,
                           border_color=T.OBSIDIAN_BORDER,
                           focused_border_color=T.TEAL,
                           color=T.TEXT_PRIMARY,
                           label_style=ft.TextStyle(color=T.TEXT_SECONDARY))
    pw_status = ft.Text("", size=12, color=T.TEAL)

    def _change_password(e):
        try:
            from oasis.logic.auth_manager import verify_password, hash_password, get_auth_db_conn
            from oasis.logic.onboarding import resolved_db_path
            # Change the SIGNED-IN account's password, never a guessed default.
            # This used to fall back to "ops_admin", so an unauthenticated
            # first-run window pointed the form at the admin account.
            username = page.session.get("username")
            if not username or username == "setup":
                pw_status.value = "❌ Sign in before changing a password."
                pw_status.color = T.DANGER
                page.update()
                return
            db = resolved_db_path(project_root)
            conn = get_auth_db_conn(db)
            row = conn.execute(
                "SELECT PASSWORD_HASH FROM OASIS_USERS WHERE USERNAME=?",
                (username,)).fetchone()
            if row and verify_password(current_pw.value, row[0]):
                new_hash = hash_password(new_pw.value)
                conn.execute("UPDATE OASIS_USERS SET PASSWORD_HASH=? WHERE USERNAME=?",
                             (new_hash, username))
                conn.commit()
                pw_status.value = "✅ Password changed successfully."
                pw_status.color = T.TEAL
            else:
                pw_status.value = "❌ Current password is incorrect."
                pw_status.color = T.DANGER
        except Exception as ex:
            pw_status.value = f"❌ Error: {ex}"
            pw_status.color = T.DANGER
        page.update()

    auth_card = T.card_container(
        content=ft.Column([
            T.section_header("Change Password", "🔐"),
            current_pw,
            new_pw,
            ft.ElevatedButton("Update Password", on_click=_change_password,
                               icon=ft.Icons.LOCK,
                               style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                                    color=T.TEAL)),
            pw_status,
        ], spacing=12),
    )

    return ft.Column(
        controls=[
            T.spec_tag("SYSTEM CONFIGURATION", hot=True),
            ft.Text("Settings", size=28, weight=ft.FontWeight.W_700,
                    color=T.TEXT_PRIMARY),
            ft.Container(height=12),
            license_card,
            data_card,
            auth_card,
        ],
        spacing=8,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
