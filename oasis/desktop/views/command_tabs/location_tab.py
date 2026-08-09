"""
Site Selection for the Flet Command Center — the greenfield location pillar.

Three things a chain needs before opening a site: where its own stores are, who
else is already there, and what share a new store could realistically take.

Nothing here ships with data. The estate is the client's own (entered once, per
install), the competitor set is fetched for THEIR region from OpenStreetMap,
and the scoring is interpretable geography rather than a model. See
``oasis.logic.site_scoring`` for what it deliberately cannot tell you.
"""
import flet as ft

from ... import theme as T
from ... import data as D


def _num(field, default=0.0) -> float:
    try:
        return float(str(field.value).strip())
    except (TypeError, ValueError):
        return default


def build_location_tab(page: ft.Page, project_root: str) -> ft.Column:
    stores = D.list_stores(project_root)
    if not stores:
        return ft.Column([ft.Text("No stores configured.",
                                  color=T.TEXT_SECONDARY)])

    body = ft.Container()
    placed_panel = ft.Container()

    def _refresh_estate():
        m = D.store_map(project_root)
        rows = [
            ft.DataRow(cells=[
                ft.DataCell(ft.Text(s["name"][:26], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(s["org_cd"], size=11, color=T.TEXT_MUTED)),
                ft.DataCell(ft.Text(f"{s['lat']:.4f}", size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(f"{s['lon']:.4f}", size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(f"{s['size_sqft']:,.0f}", size=11,
                                    color=T.TEXT_SECONDARY)),
            ]) for s in m["located"]
        ]
        controls = [
            ft.Row([
                T.metric_card("Stores placed", f"{len(m['located']):,}",
                              status="success" if m["located"] else "warning"),
                T.metric_card("Awaiting a location", f"{len(m['missing']):,}",
                              status="warning" if m["missing"] else "success",
                              sub="cannot be scored against"),
            ], spacing=12, expand=True),
        ]
        if rows:
            controls.append(ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=n)
                         for h, n in (("Store", False), ("Org", False),
                                      ("Lat", True), ("Lon", True),
                                      ("Sq ft", True))],
                rows=rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=16, expand=True))
        if m["missing"]:
            controls.append(ft.Text(
                "Not yet placed: " + ", ".join(s["name"] for s in m["missing"][:6])
                + ("…" if len(m["missing"]) > 6 else ""),
                size=11, color=T.WARNING))
        placed_panel.content = ft.Column(controls, spacing=8)
        if page:
            page.update()

    # ── place a store ────────────────────────────────────────────────────
    org_dd = ft.Dropdown(label="Store", width=250, dense=True,
                         options=[ft.dropdown.Option(key=s["org_cd"],
                                                     text=s["name"][:30])
                                  for s in stores],
                         value=stores[0]["org_cd"])
    p_lat = ft.TextField(label="Latitude", width=140, dense=True)
    p_lon = ft.TextField(label="Longitude", width=140, dense=True)
    p_size = ft.TextField(label="Sq ft", width=110, dense=True, value="10000")
    p_msg = ft.Text("", size=12, color=T.TEXT_SECONDARY)

    def _on_place(e):
        res = D.set_store_location(org_dd.value, _num(p_lat), _num(p_lon),
                                   _num(p_size, 10_000.0), root=project_root)
        p_msg.value = ("Saved." if res["saved"] else res["error"])
        p_msg.color = T.SUCCESS if res["saved"] else T.DANGER
        _refresh_estate()

    # ── score a candidate ────────────────────────────────────────────────
    c_name = ft.TextField(label="Candidate name", width=220, dense=True)
    c_lat = ft.TextField(label="Latitude", width=140, dense=True)
    c_lon = ft.TextField(label="Longitude", width=140, dense=True)
    c_size = ft.TextField(label="Sq ft", width=110, dense=True, value="10000")
    candidates: list = []

    def _render(res: dict) -> ft.Control:
        if res.get("error"):
            return T.card_container(content=ft.Row([
                ft.Icon(ft.Icons.INFO_OUTLINE, size=18, color=T.WARNING),
                ft.Text(res["error"], size=12, color=T.TEXT_SECONDARY,
                        expand=True),
            ], spacing=8))

        rows = []
        for s in res["sites"]:
            cap = s["adjusted_capture_pct"]
            colour = (T.SUCCESS if cap >= 30 else
                      T.WARNING if cap >= 10 else T.DANGER)
            rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(s["name"][:24], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(f"{cap:.1f}%", size=11, color=colour,
                                    weight=ft.FontWeight.W_600)),
                ft.DataCell(ft.Text(f"{s['cannibalisation_pct']:.0f}%", size=11,
                                    color=T.WARNING
                                    if s["cannibalisation_pct"] > 50
                                    else T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text("—" if s["nearest_own_km"] is None
                                    else f"{s['nearest_own_km']:.1f}", size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text("—" if s["nearest_competitor_km"] is None
                                    else f"{s['nearest_competitor_km']:.1f}",
                                    size=11, color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(str(s["competitors_within_2km"]), size=11,
                                    color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(s["format"][:24], size=11, color=T.INFO)),
            ]))

        verdicts = [ft.Text(f"· {s['name']}: {s['verdict']}", size=11,
                            color=T.TEXT_SECONDARY) for s in res["sites"]]

        return ft.Column([
            T.card_container(content=ft.Column([
                T.section_header("Ranked Candidates", "📍"),
                ft.DataTable(
                    columns=[ft.DataColumn(ft.Text(h, size=11,
                                                   color=T.TEXT_MUTED),
                                           numeric=n)
                             for h, n in (("Site", False), ("Capture", True),
                                          ("From own", True),
                                          ("Nearest own km", True),
                                          ("Nearest rival km", True),
                                          ("Rivals <2km", True),
                                          ("Format", False))],
                    rows=rows, heading_row_color=T.OBSIDIAN_RAISE,
                    data_row_color=T.DEEP_OBSIDIAN, column_spacing=14,
                    expand=True),
                ft.Container(height=6),
                *verdicts,
                ft.Container(height=6),
                ft.Text(f"Scored against {res['own_stores']} of your stores and "
                        f"{res['competitors']} competitor sites. Capture is a "
                        "Huff share of the surrounding catchment — it measures "
                        "how CONTESTED an area is, not how many people live "
                        "there.", size=11, color=T.TEXT_MUTED),
                (ft.Text(res["attribution"], size=10, color=T.TEXT_MUTED,
                         font_family="JetBrains Mono")
                 if res.get("attribution") else ft.Container()),
                (ft.Text(res["competitor_error"], size=11, color=T.WARNING)
                 if res.get("competitor_error") else ft.Container()),
            ], spacing=8)),
        ], spacing=8)

    def _on_add(e):
        lat, lon = _num(c_lat, 999), _num(c_lon, 999)
        if abs(lat) > 90 or abs(lon) > 180:
            body.content = ft.Text("Enter a valid latitude and longitude.",
                                   size=12, color=T.WARNING)
            if page:
                page.update()
            return
        candidates.append({"name": (c_name.value or "").strip()
                           or f"{lat:.4f}, {lon:.4f}",
                           "lat": lat, "lon": lon,
                           "size_sqft": _num(c_size, 10_000.0)})
        body.content = _render(D.score_sites(candidates, root=project_root))
        if page:
            page.update()

    def _on_clear(e):
        candidates.clear()
        body.content = ft.Container()
        if page:
            page.update()

    _refresh_estate()

    return ft.Column(
        controls=[
            ft.Text("Site Selection", size=20, weight=ft.FontWeight.W_600,
                    color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            T.card_container(content=ft.Column([
                T.section_header("Your Estate", "🏬"),
                ft.Text("Your POS knows your stores but not where they are. "
                        "Place them once — every candidate is scored by "
                        "distance from these points.",
                        size=12, color=T.TEXT_SECONDARY),
                ft.Row([org_dd, p_lat, p_lon, p_size,
                        ft.ElevatedButton("Place store", icon=ft.Icons.PLACE,
                                          on_click=_on_place)],
                       spacing=10, wrap=True,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
                p_msg,
                ft.Container(height=8),
                placed_panel,
            ], spacing=8)),
            ft.Container(height=14),
            T.card_container(content=ft.Column([
                T.section_header("Score a Candidate Site", "🧭"),
                ft.Text("Add one or more locations to rank them against your "
                        "estate and the competition already there.",
                        size=12, color=T.TEXT_SECONDARY),
                ft.Row([c_name, c_lat, c_lon, c_size,
                        ft.ElevatedButton("Score site", icon=ft.Icons.TRAVEL_EXPLORE,
                                          bgcolor=T.TEAL, color=T.DEEP_OBSIDIAN,
                                          on_click=_on_add),
                        ft.TextButton("Clear", on_click=_on_clear)],
                       spacing=10, wrap=True,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
            ], spacing=8)),
            ft.Container(height=12),
            body,
        ],
        spacing=10, scroll=ft.ScrollMode.AUTO, expand=True,
    )
