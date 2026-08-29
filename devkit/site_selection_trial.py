"""Site Selection, in a browser, for a hands-on trial.

DEV TOOLING — never ships (devkit/ is excluded from the release).

Mounts the real ``build_location_tab`` from the Command Center on its own Flet
web server, so the site-selection methodology can be driven end to end without
signing into the full console. The tab, the data layer and the scoring are the
shipping ones; only the shell around them is a harness.

    python devkit/site_selection_trial.py [--port 8551]

It points at the multi-store demo database and ignores whatever POS/ERP the
developer has configured, for the same reason the test suite does: a persistent
OASIS_POS_DB_URL left over from a port silently repoints the estate at a SQL
Server that is not running, and the tab then reports an empty estate rather
than a connection fault.

NOTE it reads and writes the REAL oasis/data — placements, chain sizes and any
fetch land in the developer's install, which is the point of a trial but worth
knowing before pressing Import.
"""

from __future__ import annotations

import argparse
import os
import sys

_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
sys.path.insert(0, _ROOT)


def _prepare_environment() -> str:
    """Point the harness at the demo estate. Returns the database in use."""
    # A developer's leftover POS/ERP outranks OASIS_DB_PATH — clear it first.
    for var in ("OASIS_POS_DB_URL", "OASIS_DB_URL", "OASIS_ERP"):
        os.environ.pop(var, None)

    db = os.environ.get("OASIS_DB_PATH")
    if not db:
        demo = os.path.join(_ROOT, "oasis", "data", "rhapta_multi_store.db")
        if os.path.exists(demo):
            os.environ["OASIS_DB_PATH"] = demo
            db = demo
    # Serve rather than launch the developer's browser; the caller opens it.
    os.environ.setdefault("FLET_FORCE_WEB_SERVER", "true")
    return db or "(none configured)"


def main(port: int = 8551) -> None:
    db = _prepare_environment()

    import flet as ft

    from oasis.desktop import data as D
    from oasis.desktop import theme as T
    from oasis.desktop.views.command_tabs.location_tab import build_location_tab

    D.reset_adapter()
    stores = D.list_stores(_ROOT)
    status = D.region_data_status(_ROOT)

    print(f"  database   {db}")
    print(f"  stores     {len(stores)} from the POS, "
          f"{status['stores_placed']} placed")
    print(f"  rivals     {status['competitors']:,} "
          f"({status['competitors_sized']:,} sized)")
    print(f"  population {status['population_cells']:,} cells, "
          f"{status['population_people']:,.0f} people")
    print(f"  amenities  {status['amenities']:,} POIs")
    print(f"  READY      {status['ready']}")
    print(f"\n  http://localhost:{port}\n")

    def _page(page: ft.Page) -> None:
        page.title = "O.A.S.I.S. — Site Selection (trial)"
        page.theme_mode = ft.ThemeMode.DARK
        page.theme = T.build_theme()
        page.bgcolor = T.DEEP_OBSIDIAN
        page.padding = 18
        page.scroll = ft.ScrollMode.AUTO
        page.add(
            ft.Container(
                content=ft.Row([
                    ft.Icon(ft.Icons.SCIENCE, size=16, color=T.TEAL),
                    ft.Text("Trial harness — the Site Selection tab from the "
                            "Command Center, mounted without the login shell. "
                            "Placements and fetches write to this install.",
                            size=11, color=T.TEXT_MUTED, expand=True),
                ], spacing=8),
                padding=ft.padding.only(bottom=10)),
            build_location_tab(page, _ROOT),
        )

    ft.app(target=_page, view=ft.AppView.WEB_BROWSER, port=port)


if __name__ == "__main__":
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--port", type=int, default=8551)
    main(ap.parse_args().port)
