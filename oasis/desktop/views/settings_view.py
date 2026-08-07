"""
O.A.S.I.S. Desktop — Settings View.

Auth management, license posture, branding, and data source reset.
"""

import flet as ft

from .. import data as D
from .. import theme as T
from .license_view import build_activation_panel


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
    mods = sorted(D.allowed_modules())

    license_card = T.card_container(
        content=ft.Column([
            T.section_header("License", "🔑"),
            ft.Row([
                T.metric_card("Status", lic.get("status", "unknown").upper(),
                              status="success" if lic.get("status") == "licensed" else "warning"),
                T.metric_card("Days Remaining",
                              str(lic.get("days_remaining", "∞")),
                              status="success" if lic.get("days_remaining", 0) > 7 else "danger"),
                T.metric_card("Modules", str(len(mods)), status="info",
                              sub=", ".join(mods) if mods else "none",
                              help_text="What this install may use right now. "
                                        "During evaluation every module is open."),
            ], spacing=12, expand=True),
            # Audit E2: activation happens IN the product. Telling a client to
            # drop a file beside the application was the workaround this replaces.
            build_activation_panel(page),
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

    # ── System Configuration (from Command Center) ───────────────────────
    config_card_content = ft.Column([
        T.section_header("System Configuration", "⚙️"),
        ft.Text("Requires ops_admin privileges.", size=12, color=T.TEXT_MUTED),
    ], spacing=12)
    
    config_card = T.card_container(content=config_card_content)
    
    def _load_configs():
        try:
            from oasis.logic.db_connector import load_system_config_full
            from oasis.logic.onboarding import resolved_db_path
            db = resolved_db_path(project_root)
            return load_system_config_full(db) or []
        except Exception as e:
            return []

    configs = _load_configs()
    config_inputs = {}

    if configs:
        groups = {}
        for cfg in configs:
            g = cfg.get('CONFIG_GROUP', 'general')
            groups.setdefault(g, []).append(cfg)
        
        for group_name, items in groups.items():
            config_card_content.controls.append(ft.Text(f"{group_name.title()}", weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY))
            for cfg in items:
                key = cfg['CONFIG_KEY']
                desc = cfg.get('DESCRIPTION', key)
                val = cfg['CONFIG_VALUE']
                
                tf = ft.TextField(
                    label=desc,
                    value=val,
                    border_color=T.OBSIDIAN_BORDER,
                    focused_border_color=T.TEAL,
                    color=T.TEXT_PRIMARY,
                    label_style=ft.TextStyle(color=T.TEXT_SECONDARY),
                    tooltip=f"Key: {key}"
                )
                config_inputs[key] = (tf, val)
                config_card_content.controls.append(tf)
                
        def _save_configs(e):
            username = page.session.get("username")
            if not username or username == "setup":
                page.snack_bar = ft.SnackBar(ft.Text("❌ Sign in as ops_admin to save settings.", color=T.DANGER), bgcolor=T.OBSIDIAN_RAISE)
                page.snack_bar.open = True
                page.update()
                return
                
            try:
                from oasis.logic.db_connector import save_system_config
                from oasis.logic.onboarding import resolved_db_path
                db = resolved_db_path(project_root)
                changes = 0
                for key, (tf, old_val) in config_inputs.items():
                    new_val = tf.value
                    if new_val != old_val:
                        save_system_config(db, key, new_val, username)
                        config_inputs[key] = (tf, new_val)
                        changes += 1
                
                msg = f"✅ Saved {changes} config change(s)." if changes > 0 else "No changes detected."
                page.snack_bar = ft.SnackBar(ft.Text(msg, color=T.TEAL), bgcolor=T.OBSIDIAN_RAISE)
                page.snack_bar.open = True
            except Exception as ex:
                page.snack_bar = ft.SnackBar(ft.Text(f"❌ Save failed: {ex}", color=T.DANGER), bgcolor=T.OBSIDIAN_RAISE)
                page.snack_bar.open = True
            page.update()

        config_card_content.controls.append(
            ft.ElevatedButton("Save Settings", on_click=_save_configs, icon=ft.Icons.SAVE, style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE, color=T.TEAL))
        )
    else:
        config_card_content.controls.append(ft.Text("No configuration entries found.", color=T.WARNING))

    return ft.Column(
        controls=[
            T.spec_tag("SYSTEM CONFIGURATION", hot=True),
            ft.Text("Settings", size=28, weight=ft.FontWeight.W_700,
                    color=T.TEXT_PRIMARY),
            ft.Container(height=12),
            license_card,
            data_card,
            auth_card,
            config_card,
        ],
        spacing=8,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
