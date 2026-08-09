# Third-Party Notices

Data and libraries O.A.S.I.S. relies on, and the obligations that come with
them. Reviewed 2026-08-09.

**This is an engineering record, not legal advice.** Items marked ⚠ need
sign-off from someone qualified before a commercial release.

---

## OpenStreetMap — competitor locations (Site Selection)

**Licence:** Open Database License 1.0 (ODbL) · https://opendatacommons.org/licenses/odbl/1-0/
**Source:** Overpass API (`https://overpass-api.de/api/interpreter`)
**Used by:** `oasis/logic/geo_sources.py`, `oasis/logic/site_scoring.py`

### What ODbL distinguishes

| | What it is | Obligation |
|---|---|---|
| **Derivative Database** | An extract, a filtered copy — e.g. a `competitor_network.csv` of supermarket locations | Redistributing one means licensing **that database** under ODbL, plus attribution |
| **Produced Work** | Something computed *from* the data — a map image, a site score, a ranked shortlist | May be licensed however you like. **Attribution still required.** |

### How OASIS is arranged to stay on the right side of that

1. **No extract is redistributed.** `competitor_network.csv` is *not* in the
   release (asserted by `test_no_openstreetmap_extract_is_redistributed`). The
   client fetches their own region on their own machine via
   `geo_sources.fetch_competitors()`, and the cache is `.gitignore`d.
   OASIS therefore never distributes an OSM-derived database at all.

2. **What OASIS ships is the scoring** — `site_scoring.py` — which contains no
   OSM data, only arithmetic.

3. **Site scores are Produced Works.** A capture percentage and a ranked
   shortlist are computed *from* the data, so they carry no share-alike
   obligation — but they must be attributed.

4. **Attribution is centralised** in `geo_sources.OSM_ATTRIBUTION`:

   > Competitor locations © OpenStreetMap contributors (ODbL)

   It is returned alongside every competitor read and rendered in the Site
   Selection tab. `test_attribution_exists_and_names_osm` pins it.

### ⚠ Open questions for legal sign-off

* **Attribution placement.** ODbL §4.3 requires the notice be "reasonably
  calculated to make any Person that usesible" aware of it. Today it appears in
  the Site Selection tab. Confirm whether it must also appear in exported
  reports, printed output, and any customer-facing deck built from site scores.
* **Overpass API usage policy.** Separate from the ODbL: the public endpoint is
  a shared volunteer resource with a fair-use policy. OASIS fetches once and
  caches, and does not query per page view. Confirm acceptable volume for a
  commercial product, or budget for a self-hosted/commercial Overpass instance.
* **If the arrangement ever changes** — if OASIS starts shipping a bundled
  competitor set, or a hosted service serves OSM-derived data to many clients —
  the Derivative Database obligations reattach and this section must be redone.

---

## Python dependencies

Pinned in `requirements.txt`. Notable licences:

| Package | Licence | Note |
|---|---|---|
| `flet`, `flet-desktop` | Apache-2.0 | The desktop shell |
| `streamlit` | Apache-2.0 | The browser consoles |
| `pandas`, `numpy` | BSD-3-Clause | |
| `scikit-learn` | BSD-3-Clause | Used by cluster analysis, not by site scoring |
| `torch` | BSD-3-Clause | Optional; the GNN path degrades without it |
| `bcrypt` | Apache-2.0 | Password hashing |
| `SQLAlchemy` | MIT | |
| `openpyxl` | MIT | Excel import/export |

⚠ Not independently audited. Run a licence scanner (`pip-licenses` or similar)
before a commercial release and record the output here.

---

## What is NOT third-party, and must never ship

Not a licence matter — a confidentiality one, recorded here because it is the
same review.

* `Full_Product_Allocation_Scorecard_*.csv` — one retailer's per-SKU revenue,
  margin, gross profit, GMROI and named supplier terms.
* `oasis_vault/` — the same data in Obsidian form.
* `store_coords.json` — a client's estate map.
* `oasis/data/store_locations.json`, `oasis/data/competitor_network.csv` — the
  *client's own* copies, written per install.

`test_no_client_trading_data_ships` and the whitewash guards in
`tests/test_command_center_parity.py` enforce this.
