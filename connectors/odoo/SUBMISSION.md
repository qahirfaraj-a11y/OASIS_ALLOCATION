# Odoo Apps Store submission — OASIS Retail Intelligence Connector

Listing tier decided: **Free, LGPL-3** (see the memory note / conversation for
the reasoning — the connector has no proprietary IP; OASIS's revenue lives in
the Cloud Hub's module licensing, not the connector, so a free listing
maximizes install volume into the trial funnel).

## What's done (this repo, verified)

| Item | Status | Where |
|---|---|---|
| Manifest complete (name/version/summary/description/author/website/category/license/depends/data) | ✅ | `oasis_connector/__manifest__.py` |
| License set to LGPL-3 | ✅ | manifest `"license"` key |
| LICENSE file (canonical GNU LGPL-3.0 text) | ✅ | `oasis_connector/LICENSE` |
| App icon, exactly 128×128 (Odoo's required size) | ✅ | `static/description/icon.png` |
| Store banner, 560×280, OASIS SYSTEMS v1.0 palette | ✅ | `static/description/banner.png` |
| Long-form listing page (`static/description/index.html`) | ✅ | covers the connector AND the OASIS-inside-Odoo consoles |
| `application: True` (so OASIS gets a navbar/Apps-grid tile) | ✅ | manifest |
| All bundled XML well-formed | ✅ | verified via `xml.etree.ElementTree` |
| `ir.model.access.csv` grants correct (not header-only) | ✅ | fixed 2026-07-16 — was a latent bug that silently hid the OASIS menu |
| No `eval`/`exec`/`pickle`, no hardcoded secrets, no bare `except` | ✅ | scanned |
| Zero extra pip dependencies (stdlib-only `push_client.py`) | ✅ | a customer's Odoo server needs nothing new installed |
| Unit + end-to-end test coverage (15 tests) | ✅ | `tests/test_odoo_connector.py` |
| Verified installing + running live against real Odoo 16 in Docker | ✅ | see `INTEGRATION.md` — L4 proof, including the in-Odoo console embed |
| Version bumped to reflect this submission pass | ✅ | `16.0.1.2.0` |

## What's still a placeholder (fix before the actual upload)

- **`website` in the manifest** (`https://www.oasis-systems.example`) — apps.odoo.com
  requires a real, reachable publisher URL. Replace with iLink's actual
  domain once decided.
- **Support contact** — Odoo's submission form asks for a support email;
  none is baked into the manifest (Odoo doesn't have a dedicated manifest key
  for it — it's entered directly in the Apps portal form at submission time).
- **Screenshots for the listing** — `static/description/index.html` describes
  the product in text; apps.odoo.com's submission form also accepts uploaded
  screenshot images shown in a gallery. Real screenshots of the running addon
  (Settings page, the OASIS app tile, an embedded console) were not captured
  in this pass because the local Docker stack was down at the time — see
  "Next step" below.
- **Author/publisher identity** — currently `"iLink"`. Confirm this matches
  the name you want on the public listing (apps.odoo.com ties listings to the
  submitting odoo.com account's registered name).

## The procedure — what happens on apps.odoo.com (human-only)

I cannot do these steps: creating accounts, accepting third-party publisher
terms, and submitting to an external marketplace are outside what I do
autonomously. This is the exact sequence for whoever submits:

1. **Create (or use) an odoo.com account.** Go to https://www.odoo.com and
   sign up / log in. This is the identity the listing will be published under.
2. **Open the Apps developer area.** From https://apps.odoo.com, there's a
   "Submit an App" / "Become a Publisher" flow (under your account menu once
   logged in) that opens the module submission form.
3. **Upload the module.** Zip the `oasis_connector/` directory (the addon
   folder itself, not `connectors/odoo/` — the zip's top level should be
   `oasis_connector/`) and upload it, or connect a Git repository if the form
   offers that option.
4. **Fill out the listing form**: category (Inventory), short summary, the
   support email, pricing (select **Free**), and confirm the license
   (**LGPL-3**, matching the manifest — Odoo cross-checks this).
5. **Accept the publisher agreement.** Free-tier apps still require accepting
   Odoo's standard publisher terms (not a revenue-share contract — that's only
   for paid listings). Read it; only you can accept it.
6. **Submit for review.** Odoo runs the free-tier apps through an automated
   technical check (manifest validity, `pylint-odoo`-style linting, security
   scan for obvious issues) plus a lighter manual pass. Turnaround for free
   apps is typically faster than paid listings, but Odoo doesn't publish a
   guaranteed SLA — expect it to take from a few days up to a couple of weeks.
7. **Respond to reviewer feedback, if any.** Odoo may come back with requested
   changes (e.g., "add a support email," "clarify data usage in the
   description"). Everything in this repo has been pre-checked against the
   common technical rejection reasons, so feedback at this stage should be
   listing-copy level, not code level.
8. **Once approved, it's live** on apps.odoo.com, searchable and installable
   by any Odoo user. Future updates: bump the manifest `version`, re-upload —
   Odoo re-reviews each version bump, usually faster than the first submission.

## Next step (technical, I can do this)

Capture real screenshots for the listing gallery: with the Docker stack up
(`docker compose -f docker-compose.odoo.yml up -d` from this directory), log
into Odoo, and grab: (1) Settings → OASIS Connector, (2) the OASIS app tile in
Odoo's Apps grid / home menu, (3) the Intelligence console embedded live. Ask
for this once Docker is back up, or I can walk you through capturing them
yourself if you'd rather not wait on the container bring-up.
