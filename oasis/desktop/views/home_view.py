"""
O.A.S.I.S. Desktop — Home View.

The first screen a client sees: system status, onboarding wizard,
action buttons, and console navigation (now in-app, not browser URLs).
"""

import os
import time

import flet as ft

from .. import theme as T


def build_home_view(page: ft.Page, project_root: str) -> ft.Column:
    """Construct the Home view controls."""

    # ── Helpers ──────────────────────────────────────────────────────────
    def _load_version() -> str:
        try:
            with open(os.path.join(project_root, "VERSION"), "r") as f:
                return f.read().strip()
        except Exception:
            return "?"

    def _load_onboarding():
        try:
            from oasis.logic.onboarding import load_onboarding, is_onboarded
            return load_onboarding(), is_onboarded()
        except Exception:
            return {}, False

    def _load_branding():
        try:
            from oasis.logic.branding import load_branding
            b = load_branding()
            return b.tenant_name or "OASIS", b.product_name or "OASIS"
        except Exception:
            return "OASIS", "OASIS"

    def _license_status() -> str:
        try:
            from oasis.logic.license_manager import license_posture
            p = license_posture()
            return p.get("status", "evaluation")
        except Exception:
            return "evaluation"

    version = _load_version()
    ob_record, is_onboarded = _load_onboarding()
    tenant_name, product_name = _load_branding()
    source = ob_record.get("source", "none")
    store_name = ob_record.get("store_name", tenant_name)
    lic_status = _license_status()

    # ── Source badge text ────────────────────────────────────────────────
    source_labels = {
        "demo": "🧪 Sample Store",
        "empty": "📭 Your Own Store",
        "connect": "🔌 Connected POS",
        "init": "🏢 Catalogue Build",
        "multi_demo": "🏬 Multi-Store Demo Network",
    }
    source_label = source_labels.get(source, "⚠ Not Onboarded")

    # ── System Status Ticker ─────────────────────────────────────────────
    ticker = ft.Row(
        controls=[
            T.status_dot("SYSTEM READINESS", "PEAK" if is_onboarded else "SETUP REQUIRED"),
            T.status_dot("ENGINE VERSION", f"v{version}"),
            T.status_dot("LICENSE", lic_status.upper()),
            T.status_dot("DATA SOURCE", source_label),
        ],
        wrap=True,
        spacing=24,
    )

    # ── Onboarding Wizard (shown when not onboarded) ─────────────────────
    def _build_onboarding_section() -> ft.Column:
        store_name_field = ft.TextField(
            label="Store / Company Name",
            value=tenant_name if tenant_name != "OASIS" else "",
            hint_text="e.g. Rhapta Superstore",
            border_color=T.OBSIDIAN_BORDER,
            focused_border_color=T.TEAL,
            color=T.TEXT_PRIMARY,
            label_style=ft.TextStyle(color=T.TEXT_SECONDARY),
            expand=True,
        )

        progress_text = ft.Text("", size=12, color=T.TEAL, visible=False)

        def _run_setup(setup_fn_name: str, e):
            progress_text.visible = True
            progress_text.value = "🔌 Step 1/3: Initializing database schema..."
            page.update()
            time.sleep(0.3)
            progress_text.value = "⚡ Step 2/3: Seeding authentication & engine parameters..."
            page.update()
            time.sleep(0.3)
            progress_text.value = "🔮 Step 3/3: Activating Chapter-11 engines (AMIT, LATA, DHARAM, MANDE)..."
            page.update()

            try:
                from oasis.logic import onboarding as OB
                name = store_name_field.value.strip() or "OASIS Store"
                if setup_fn_name == "demo":
                    OB.apply_demo(store_name=name)
                elif setup_fn_name == "empty":
                    OB.apply_empty(store_name=name)
                elif setup_fn_name == "multi_demo":
                    OB.apply_multi_demo()

                # Save branding
                try:
                    from oasis.logic.branding import save_branding
                    save_branding({"tenant_name": name, "product_name": "OASIS"})
                except Exception:
                    pass

                progress_text.value = "✅ Setup complete! Reloading..."
                page.update()
                time.sleep(0.5)
                # Reload the home view
                page.go("/")
            except Exception as ex:
                progress_text.value = f"❌ Error: {ex}"
                page.update()

        return ft.Column([
            T.section_header("First-Run Setup", "🚀"),
            ft.Text("Configure your store identity and data source to get started.",
                     size=14, color=T.TEXT_SECONDARY),
            ft.Row([store_name_field], expand=True),
            ft.Divider(color=T.OBSIDIAN_BORDER),
            ft.Row([
                ft.ElevatedButton(
                    "🧪 Sample Store",
                    tooltip="Load a realistic demo store (~35 SKUs, 7 departments)",
                    on_click=lambda e: _run_setup("demo", e),
                    style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                         color=T.TEAL),
                ),
                ft.ElevatedButton(
                    "📭 Start Fresh",
                    tooltip="Create an empty store with the full OASIS schema",
                    on_click=lambda e: _run_setup("empty", e),
                    style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                         color=T.TEXT_PRIMARY),
                ),
                ft.ElevatedButton(
                    "🏬 Multi-Store Network",
                    tooltip="Build a 5-outlet demo network (Rhapta, Westgate, Kilimani, Lavington, Karen)",
                    on_click=lambda e: _run_setup("multi_demo", e),
                    style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                         color=T.TEXT_PRIMARY),
                ),
            ], spacing=12, wrap=True),
            progress_text,
        ], spacing=12)

    # ── Console Cards (quick navigation) ─────────────────────────────────
    def _nav_card(icon: str, title: str, desc: str, route: str) -> ft.Container:
        return ft.Container(
            content=ft.Column([
                ft.Text(icon, size=32),
                ft.Text(title, size=16, weight=ft.FontWeight.W_600,
                        color=T.TEXT_PRIMARY),
                ft.Text(desc, size=12, color=T.TEXT_SECONDARY),
            ], spacing=6, horizontal_alignment=ft.CrossAxisAlignment.CENTER),
            bgcolor=T.OBSIDIAN_RAISE,
            border=ft.border.all(1, T.OBSIDIAN_BORDER),
            border_radius=12,
            padding=20,
            expand=True,
            on_click=lambda e: page.go(route),
            on_hover=lambda e: None,  # Flet handles hover visually
            ink=True,
        )

    console_cards = ft.Row([
        _nav_card("◎", "Operations", "Ordering, Transfers,\nSuppliers, Allocation", "/ops"),
        _nav_card("🔮", "Command Center", "Multi-store oversight,\nNetwork health", "/command"),
        _nav_card("⚡", "Intelligence", "Monitoring, Backtests,\nEngine telemetry", "/intel"),
        _nav_card("📈", "Market Intel", "ST-GAT, Competitor\nMapping, Expansion", "/market"),
    ], spacing=16, expand=True)

    # ── Action Buttons ───────────────────────────────────────────────────
    def _run_backup(e):
        try:
            from oasis.logic.backup_util import run_backup
            from oasis.logic.onboarding import resolved_db_path
            db = resolved_db_path(project_root)
            result = run_backup(db)
            page.snack_bar = ft.SnackBar(
                ft.Text(f"✅ Backup complete: {result}", color=T.TEAL),
                bgcolor=T.OBSIDIAN_RAISE)
            page.snack_bar.open = True
            page.update()
        except Exception as ex:
            page.snack_bar = ft.SnackBar(
                ft.Text(f"❌ Backup failed: {ex}", color=T.DANGER),
                bgcolor=T.OBSIDIAN_RAISE)
            page.snack_bar.open = True
            page.update()

    action_buttons = ft.Row([
        ft.ElevatedButton("💾 Run Backup", on_click=_run_backup,
                           style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                                color=T.TEAL)),
        ft.ElevatedButton("📊 Value Report",
                           on_click=lambda e: None,  # TODO: wire value report
                           style=ft.ButtonStyle(bgcolor=T.OBSIDIAN_RAISE,
                                                color=T.TEXT_PRIMARY)),
    ], spacing=12)

    # ── Assemble ─────────────────────────────────────────────────────────
    controls = [
        T.spec_tag(f"AUDITED LOGIC_ENGINE_V{version}", hot=True),
        ft.Text(f"Welcome to {product_name}", size=28,
                weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY),
        ft.Text(f"{store_name} · {source_label}", size=14,
                color=T.TEXT_SECONDARY),
        ft.Container(height=8),
        ticker,
        ft.Divider(color=T.OBSIDIAN_BORDER, height=32),
    ]

    if not is_onboarded:
        controls.append(_build_onboarding_section())
        controls.append(ft.Divider(color=T.OBSIDIAN_BORDER, height=32))

    controls.extend([
        T.section_header("Consoles", "🖥"),
        console_cards,
        ft.Container(height=16),
        T.section_header("System Actions", "⚙"),
        action_buttons,
    ])

    return ft.Column(
        controls=controls,
        spacing=8,
        scroll=ft.ScrollMode.AUTO,
        expand=True,
    )
