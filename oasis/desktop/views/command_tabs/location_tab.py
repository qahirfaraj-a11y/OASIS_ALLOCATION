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
                # The size the ESTATE's productivity supports, not the size the
                # operator typed in. site_scoring.recommend_format reads the
                # format off a capture computed from that very size, so it can
                # only restate the question; s["format"] is kept in the payload
                # for compatibility but is not what a buyer is shown.
                ft.DataCell(ft.Text(
                    str((s.get("size_recommendation") or {}).get("format")
                        or "—")[:24], size=11, color=T.INFO)),
            ]))

        verdicts = [ft.Text(f"· {s['name']}: {s['verdict']}", size=11,
                            color=T.TEXT_SECONDARY) for s in res["sites"]]

        # ── proposed capital, and the basis it rests on ──────────────────
        # A share is not a budget. The capital panel is deliberately separate
        # from the ranking table so the operator can see WHICH of the three
        # bases produced the number before spending against it.
        cal = res.get("calibration") or {}
        val = res.get("validation") or {}
        cap_rows = []
        for s in res["sites"]:
            c = s.get("capital") or {}
            money = ("—" if c.get("opening_capital") is None
                     else f"{c['opening_capital']:,.0f}")
            band = ("—" if c.get("capital_low") is None
                    else f"{c['capital_low']:,.0f} – {c['capital_high']:,.0f}")
            basis = c.get("basis", "—")
            cap_rows.append(ft.DataRow(cells=[
                ft.DataCell(ft.Text(s["name"][:24], size=11,
                                    color=T.TEXT_PRIMARY)),
                ft.DataCell(ft.Text(money, size=11,
                                    color=T.TEXT_PRIMARY if
                                    c.get("opening_capital") else T.TEXT_MUTED,
                                    weight=ft.FontWeight.W_600)),
                ft.DataCell(ft.Text(band, size=11, color=T.TEXT_SECONDARY)),
                ft.DataCell(ft.Text(basis, size=11,
                                    color=T.SUCCESS if basis == "estate-calibrated"
                                    else T.WARNING if basis == "estate-productivity"
                                    else T.TEXT_MUTED)),
            ]))

        if val.get("validated"):
            gate_txt = (f"Validated: on your {val['n']} located stores the "
                        f"geography predicts revenue better "
                        f"({val['mape_capture']:.0%} median error) than floor "
                        f"area alone ({val['mape_sqft_only']:.0%}) or your "
                        f"estate median ({val['mape_estate_median']:.0%}). "
                        "Capital below is calibrated on your own trading.")
            gate_col = T.SUCCESS
        else:
            gate_txt = ("Not validated — " + (val.get("reason") or "") +
                        " Capital below is your own revenue per square foot, "
                        "and the site does not enter it.")
            gate_col = T.WARNING

        capital_card = T.card_container(content=ft.Column([
            T.section_header("Proposed Opening Capital", "💰"),
            ft.Text(gate_txt, size=11, color=gate_col),
            ft.DataTable(
                columns=[ft.DataColumn(ft.Text(h, size=11, color=T.TEXT_MUTED),
                                       numeric=n)
                         for h, n in (("Site", False), ("Capital", True),
                                      ("Range", True), ("Basis", False))],
                rows=cap_rows, heading_row_color=T.OBSIDIAN_RAISE,
                data_row_color=T.DEEP_OBSIDIAN, column_spacing=16, expand=True),
            ft.Container(height=4),
            *[ft.Text(f"· {s['name']}: {(s.get('capital') or {}).get('note', '')}",
                      size=10, color=T.TEXT_MUTED) for s in res["sites"]],
            ft.Container(height=4),
            ft.Text(
                (f"Calibrated on {res.get('calibrated_on', 0)} of your stores · "
                 f"catchments differ {cal.get('demand_spread_ratio', 0):.1f}x · "
                 f"stock-to-sales {cal.get('cover_ratio', 0):.2f}"
                 + ("" if cal.get("cover_measured")
                    else " (stock values missing — revenue only)"))
                if cal.get("usable") else
                (cal.get("reason") or "No estate calibration available."),
                size=10, color=T.TEXT_MUTED),
        ], spacing=8))

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
                                          ("Supported size", False))],
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
            ft.Container(height=12),
            capital_card,
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
