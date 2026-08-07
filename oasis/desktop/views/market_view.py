"""
O.A.S.I.S. Desktop — Market Intelligence View (Phase 3).

Store Intelligence (top movers, revenue drivers) and Cluster Analysis.
The ST-GAT map and neural ecosystem render in the browser.
"""

import flet as ft
import subprocess
import os

from .. import data as D
from .. import theme as T
from .license_view import build_upsell

TAB_MODULES = {
    "store_intelligence": "network",
    "cluster_analysis": "network",
    "st_gat_map": "network",
}

def _money(v: float) -> str:
    try:
        return f"KES {float(v):,.0f}"
    except (TypeError, ValueError):
        return "—"

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

def build_market_view(page: ft.Page, project_root: str) -> ft.Column:
    """Construct the Market Intelligence view."""
    stores = D.list_stores(project_root)
    prov = D.data_provenance(project_root)
    mods = D.allowed_modules()

    def _ok(tab_key: str) -> bool:
        return TAB_MODULES.get(tab_key, "core") in mods

    if not stores:
        return ft.Column([
            T.spec_tag("MARKET INTELLIGENCE", hot=True),
            ft.Container(height=16),
            _error_row("No stores found in the active database. Complete "
                       "first-run setup, or check the data source in Settings."),
        ], expand=True, scroll=ft.ScrollMode.AUTO)

    org = "ALL" if len(stores) > 1 else stores[0]["org_cd"]

    # ── Store Intelligence ───────────────────────────────────────────────
    if not _ok("store_intelligence"):
        intel = [build_upsell(TAB_MODULES["store_intelligence"])]
    else:
        intel_data = D.store_intelligence(org, project_root)
        if intel_data.get("error"):
            intel = [_error_row(f"Failed to read store intelligence: {intel_data['error']}")]
        else:
            top_qty = intel_data.get("top_qty", [])
            top_rev = intel_data.get("top_rev", [])
            cat_stats = intel_data.get("categories", [])
            
            qty_rows = [[r["Product"], r["Category"], f"{r['Units']:,.0f}", r["Stockouts"]] for r in top_qty]
            rev_rows = [[r["Product"], r["Category"], _money(r["Revenue"])] for r in top_rev]
            cat_rows = [[r["Category"], _money(r["Revenue"]), f"{r['Units']:,.0f}"] for r in cat_stats]
            
            intel = [
                T.section_header(f"Store Intelligence: {org}", "🧠"),
                ft.Row([
                    ft.Column([
                        T.section_header("Top Movers (Velocity)", "🔥"),
                        _table(["Product", "Category", "Units", "Stockouts"], qty_rows, "No velocity data.")
                    ], expand=True),
                    ft.Column([
                        T.section_header("Top Revenue Drivers", "💰"),
                        _table(["Product", "Category", "Revenue"], rev_rows, "No revenue data.")
                    ], expand=True),
                ], spacing=20, expand=True, vertical_alignment=ft.CrossAxisAlignment.START),
                ft.Container(height=20),
                T.section_header("Sales by Category", "📂"),
                _table(["Category", "Revenue", "Units"], cat_rows, "No category data.")
            ]

    # ── Cluster Analysis ─────────────────────────────────────────────────
    if not _ok("cluster_analysis"):
        cluster = [build_upsell(TAB_MODULES["cluster_analysis"])]
    else:
        cluster_data = D.cluster_analysis(project_root)
        if cluster_data.get("error"):
            cluster = [_error_row(f"Cluster analysis failed: {cluster_data['error']}")]
        else:
            c_rows = [[r["Store"], r["Region"], r["Cluster"], str(r["Risk"])] for r in cluster_data.get("clusters", [])]
            cluster = [
                T.section_header("Store Similarity Map (PCA)", "🔗"),
                _table(["Store", "Region", "Cluster", "Risk"], c_rows, "No cluster data available.")
            ]

    # ── Map & Neural Ecosystem (Launcher) ────────────────────────────────
    def _launch_map(e):
        e.control.disabled = True
        e.control.text = "Launching ST-GAT Map..."
        page.update()
        
        # Launch browser console on port 8503
        try:
            env = os.environ.copy()
            env["OASIS_DB_PATH"] = D.store_db_path(project_root)
            script_path = os.path.join(project_root, "st_gat_dashboard.py")
            subprocess.Popen([
                sys.executable if 'sys' in globals() else 'python', "-m", "streamlit", "run", script_path, "--server.port=8503", "--browser.serverAddress=localhost"
            ], env=env, cwd=project_root)
            e.control.text = "ST-GAT Map launched in browser (Port 8503)"
            e.control.icon = ft.Icons.CHECK
        except Exception as ex:
            e.control.text = f"Launch failed: {ex}"
            
        page.update()

    import sys
    
    map_launcher = [
        T.card_container(content=ft.Column([
            ft.Row([
                ft.Icon(ft.Icons.MAP, size=32, color=T.TEAL),
                ft.Column([
                    ft.Text("Live Network Map & Neural Ecosystem", size=18, weight=ft.FontWeight.W_700, color=T.TEXT_PRIMARY),
                    ft.Text("The ST-GAT map, expansion grid, and neural ecosystem graphs render in the browser.", size=12, color=T.TEXT_SECONDARY)
                ], expand=True)
            ]),
            ft.Container(height=16),
            ft.ElevatedButton("Launch Web Console", icon=ft.Icons.OPEN_IN_BROWSER, on_click=_launch_map, bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN)
        ]))
    ]

    def _tab(label: str, icon: str, controls) -> ft.Tab:
        return ft.Tab(text=label, icon=icon,
                      content=ft.Container(
                          content=ft.Column(controls, spacing=10,
                                            scroll=ft.ScrollMode.AUTO),
                          padding=ft.padding.only(top=16)))

    header = [T.spec_tag("MARKET INTELLIGENCE", hot=True),
              ft.Text("Market Intelligence", size=28, weight=ft.FontWeight.W_700,
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
                        _tab("Store Intelligence", ft.Icons.INSIGHTS, intel),
                        _tab("Cluster Analysis", ft.Icons.BUBBLE_CHART, cluster),
                        _tab("Network Map", ft.Icons.MAP, map_launcher),
                    ], expand=True),
        ],
        spacing=6, expand=True,
    )
