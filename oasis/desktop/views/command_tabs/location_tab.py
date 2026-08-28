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
from ....logic.geo_sources import DEFAULT_COMPETITOR_SQFT as GS_DEFAULT_SQFT


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
                # The share turned into people. Sites are ranked on THIS when
                # a grid is loaded, because a large share of an empty valley
                # must not outrank a small share of a dense suburb.
                ft.DataCell(ft.Text(
                    "—" if s.get("captured_population") is None
                    else f"{s['captured_population']:,.0f}",
                    size=11, color=T.TEXT_PRIMARY
                    if s.get("captured_population") else T.TEXT_MUTED)),
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

        if val.get("basis") == "affluence":
            gate_txt = (f"Validated on catchment spend: across your {val['n']} "
                        f"located stores, modelling spend per person from the "
                        f"catchment predicts revenue to "
                        f"{val['mape_affluence']:.0%} median error, against "
                        f"{val['mape_sqft_only']:.0%} for floor area alone.")
            gate_col = T.SUCCESS
        elif val.get("basis") == "population":
            gate_txt = (f"Validated on population: across your {val['n']} "
                        f"located stores, people captured x your own spend per "
                        f"person predicts revenue to {val['mape_population']:.0%} "
                        f"median error, against {val['mape_sqft_only']:.0%} for "
                        f"floor area alone. Capital below is a headcount, not "
                        "a share.")
            gate_col = T.SUCCESS
        elif val.get("validated"):
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

        # Population status is shown next to the money, not buried: whether a
        # number rests on a headcount or on a share is the first thing a buyer
        # should be able to see.
        pop = res.get("population") or {}
        if pop.get("error"):
            pop_line = ft.Text("No population data loaded — scores measure how "
                               "CONTESTED a catchment is, not how many people "
                               "live in it. Load a grid to price a site on "
                               "people.", size=11, color=T.WARNING)
        else:
            pop_line = ft.Text(
                f"Population: {pop.get('rows', 0):,} cells, "
                f"{pop.get('people', 0):,.0f} people · {pop.get('source')}",
                size=11, color=T.TEXT_SECONDARY)

        capital_card = T.card_container(content=ft.Column([
            T.section_header("Proposed Opening Capital", "💰"),
            pop_line,
            ft.Text(gate_txt, size=11, color=gate_col),
            (ft.Text(val["population_note"], size=10, color=T.TEXT_MUTED)
             if val.get("population_note") else ft.Container()),
            (ft.Text(pop["attribution"], size=10, color=T.TEXT_MUTED,
                     font_family="JetBrains Mono")
             if pop.get("attribution") else ft.Container()),
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
                                          ("People", True),
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

    # ── region data: fetch what the scoring runs on ──────────────────────
    # All three fetchers existed and NOTHING called them, so a retailer could
    # not acquire competitor, population or amenity data through the product
    # at all. The worst of the three is silent: with no competitor file every
    # site scores as uncontested rather than as unknown.
    r_south = ft.TextField(label="South", width=110, dense=True)
    r_west = ft.TextField(label="West", width=110, dense=True)
    r_north = ft.TextField(label="North", width=110, dense=True)
    r_east = ft.TextField(label="East", width=110, dense=True)
    r_brands = ft.TextField(label="Competitor chains (comma separated)",
                            width=420, dense=True,
                            value=", ".join(D.DEFAULT_COMPETITOR_BRANDS))
    r_iso = ft.TextField(label="Country (ISO3)", width=130, dense=True,
                         value="KEN")
    r_msg = ft.Text("", size=12, color=T.TEXT_SECONDARY)
    status_panel = ft.Container()

    def _refresh_status():
        s = D.region_data_status(project_root)
        def _row(label, n, err, unit):
            ok = n and not err
            return ft.Row([
                ft.Icon(ft.Icons.CHECK_CIRCLE if ok else ft.Icons.ERROR_OUTLINE,
                        size=15, color=T.SUCCESS if ok else T.WARNING),
                ft.Text(f"{label}: {n:,} {unit}" if ok else f"{label}: none yet",
                        size=11,
                        color=T.TEXT_SECONDARY if ok else T.WARNING, expand=True),
            ], spacing=6)

        controls = [
            _row("Stores placed", s["stores_placed"], None, "of your estate"),
            _row("Competitors", s["competitors"], s["competitor_error"], "sites"),
            _row("Population", s["population_cells"], s["population_error"],
                 f"cells ({s['population_people']:,.0f} people)"),
            _row("Amenities", s["amenities"], s["amenity_error"], "POIs"),
        ]
        if s["stores_missing"]:
            controls.append(ft.Text(
                f"{s['stores_missing']} store(s) still need a location — they "
                "cannot be scored against.", size=11, color=T.WARNING))
        if s.get("stores_orphaned") and not s["stores_placed"]:
            controls.append(ft.Text(
                f"{s['stores_orphaned']} location(s) are on file but match no "
                "store in your POS. Check the connection, or that the store "
                "codes match — this is not a placement problem.",
                size=11, color=T.DANGER))
        if s["competitor_legacy_path"]:
            controls.append(ft.Text(
                "Competitors are being read from the old console's location. "
                "Re-fetch to move them into oasis/data.",
                size=11, color=T.WARNING))
        if s["competitors"] and s["competitors_unsized"]:
            controls.append(ft.Text(
                f"{s['competitors_unsized']:,} competitor sites have no floor "
                f"area and are assumed {int(GS_DEFAULT_SQFT):,} sq ft. Huff pull "
                "is proportional to floor area, so a kiosk currently competes "
                "as hard as a hypermarket.", size=11, color=T.WARNING))
        status_panel.content = ft.Column(controls, spacing=4)
        if page:
            page.update()

    def _on_fetch(e):
        bbox = (_num(r_south, 999), _num(r_west, 999),
                _num(r_north, 999), _num(r_east, 999))
        if any(abs(v) > 180 for v in bbox):
            r_msg.value = "Enter all four edges of the region."
            r_msg.color = T.WARNING
            if page:
                page.update()
            return
        r_msg.value = "Fetching — this can take a minute."
        r_msg.color = T.TEXT_SECONDARY
        if page:
            page.update()
        brands = [b.strip() for b in (r_brands.value or "").split(",")
                  if b.strip()]
        res = D.fetch_region_data(bbox, brands=brands,
                                  iso3=(r_iso.value or "KEN").strip().upper(),
                                  root=project_root)
        if res.get("error"):
            r_msg.value = res["error"]
            r_msg.color = T.DANGER
        else:
            parts, failed = [], []
            for name, r in (res.get("results") or {}).items():
                if r.get("error"):
                    failed.append(f"{name}: {r['error'][:70]}")
                else:
                    parts.append(f"{name} {r.get('written', 0):,}")
            r_msg.value = ("Fetched " + ", ".join(parts) if parts else "") + \
                          (("  |  FAILED — " + "; ".join(failed)) if failed else "")
            r_msg.color = T.WARNING if failed else T.SUCCESS
        _refresh_status()
        _refresh_estate()

    # ── bulk placement ───────────────────────────────────────────────────
    bulk = ft.TextField(label="Paste org_cd,lat,lon,size_sqft — one store per line",
                        multiline=True, min_lines=3, max_lines=8, width=560,
                        dense=True)
    bulk_msg = ft.Text("", size=12, color=T.TEXT_SECONDARY)

    def _on_template(e):
        bulk.value = D.store_location_template(project_root)
        bulk_msg.value = ("Filled with your stores. Add coordinates and press "
                          "Import.")
        bulk_msg.color = T.TEXT_SECONDARY
        if page:
            page.update()

    def _on_import(e):
        res = D.import_store_locations(bulk.value or "", root=project_root)
        if res.get("error"):
            bulk_msg.value = res["error"]
            bulk_msg.color = T.DANGER
        else:
            note = f"Placed {res['saved']} store(s)."
            if res.get("errors"):
                shown = "; ".join(
                    f"{x.get('org_cd') or 'line ' + str(x.get('line'))}: "
                    f"{x['reason']}" for x in res["errors"][:4])
                note += f"  Rejected {len(res['errors'])}: {shown}"
            bulk_msg.value = note
            bulk_msg.color = T.WARNING if res.get("errors") else T.SUCCESS
        _refresh_estate()
        _refresh_status()

    # ── competitor floor areas ───────────────────────────────────────────
    size_fields: dict = {}
    sizes_panel = ft.Container()
    sizes_msg = ft.Text("", size=12, color=T.TEXT_SECONDARY)

    def _refresh_sizes():
        known = D.competitor_sizes(project_root)
        chains = (D.competitor_set(project_root).get("chains") or [])
        size_fields.clear()
        rows_ = []
        for chain in chains[:12]:
            f = ft.TextField(label=chain[:18], width=150, dense=True,
                             value=(f"{known[chain.lower()]:.0f}"
                                    if chain.lower() in known else ""),
                             hint_text=f"{int(GS_DEFAULT_SQFT):,}")
            size_fields[chain] = f
            rows_.append(f)
        sizes_panel.content = (
            ft.Row(rows_, spacing=8, wrap=True) if rows_
            else ft.Text("No competitor chains loaded yet.", size=11,
                         color=T.TEXT_MUTED))
        if page:
            page.update()

    def _on_save_sizes(e):
        vals = {c: _num(f) for c, f in size_fields.items() if (f.value or "").strip()}
        if not vals:
            sizes_msg.value = "Enter a floor area for at least one chain."
            sizes_msg.color = T.WARNING
            if page:
                page.update()
            return
        res = D.set_competitor_sizes(vals, root=project_root)
        sizes_msg.value = (f"Saved {res.get('chains', 0)} chain size(s). "
                           "Re-score to see the effect."
                           if res.get("saved") else res.get("error", ""))
        sizes_msg.color = T.SUCCESS if res.get("saved") else T.DANGER
        _refresh_status()

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
    _refresh_status()
    _refresh_sizes()

    return ft.Column(
        controls=[
            ft.Text("Site Selection", size=20, weight=ft.FontWeight.W_600,
                    color=T.TEXT_PRIMARY),
            ft.Container(height=8),
            T.card_container(content=ft.Column([
                T.section_header("Region Data", "🌍"),
                ft.Text("Site scoring reads four things: your estate, the "
                        "competition, the people, and the local amenity mix. "
                        "The last three are public data fetched once for your "
                        "region.", size=12, color=T.TEXT_SECONDARY),
                ft.Container(height=4),
                status_panel,
                ft.Container(height=8),
                ft.Row([r_south, r_west, r_north, r_east, r_iso], spacing=10,
                       wrap=True),
                ft.Row([r_brands,
                        ft.ElevatedButton("Fetch region data",
                                          icon=ft.Icons.CLOUD_DOWNLOAD,
                                          on_click=_on_fetch)],
                       spacing=10, wrap=True,
                       vertical_alignment=ft.CrossAxisAlignment.CENTER),
                r_msg,
                ft.Container(height=8),
                ft.Text("Competitor floor areas — Huff pull is proportional to "
                        "them, so leaving these blank makes every rival the "
                        "same size.", size=11, color=T.TEXT_SECONDARY),
                sizes_panel,
                ft.Row([ft.ElevatedButton("Save chain sizes",
                                          icon=ft.Icons.STRAIGHTEN,
                                          on_click=_on_save_sizes)], spacing=10),
                sizes_msg,
            ], spacing=8)),
            ft.Container(height=14),
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
                # Typing one store at a time is fine for five and unusable for
                # thirty — and thirty is roughly where the estate becomes big
                # enough to validate anything.
                ft.Text("Or place them all at once:", size=12,
                        color=T.TEXT_SECONDARY),
                bulk,
                ft.Row([
                    ft.TextButton("Fill from my stores", icon=ft.Icons.DOWNLOAD,
                                  on_click=_on_template),
                    ft.ElevatedButton("Import placements",
                                      icon=ft.Icons.UPLOAD_FILE,
                                      on_click=_on_import),
                ], spacing=10, wrap=True),
                bulk_msg,
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
