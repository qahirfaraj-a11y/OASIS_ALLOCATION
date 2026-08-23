# Transfers as an Odoo Module — 2026-08

The session that took the transfer engine from *works on our depot* to
*installable at a customer, measurable in a pilot*.

Blow-by-blow narrative and the commit list live in the repo:
`OASIS_Session_Handover_2026-08-23.md`. This note keeps what should outlive it.

Related: [[OASIS_ERP_Agnostic_2026-08]] · [[Log]] · [[OASIS_Port_Method]]

---

## The decisions

**σ is R.** The safety floor a store keeps *is* its relief horizon — the time
until its next realistic delivery. Previously `default_safety_days = 14`, and
the seed said 10; neither came from anything. Precedence is now: explicit
per-store policy → derived relief → 14 only where there is no evidence at all.
This is the same rule as everywhere else in OASIS — **derive, do not declare**.
Four constants remain in the transfer path (ρ=0.5, κ=500, Θ=90, R_max=45); a
fifth appearing is a defect.

**Cadence is per-store where the store has earned it.** A site with ≥6 of its
own goods receipts gets its own rhythm rather than the supplier's network
rhythm. A store served weekly should not carry a monthly store's floor.

**Three modules, not one.** `oasis_connector` (base, depends on `base` alone),
`oasis_transfers`, `oasis_telemetry`. A client can take any combination. The
guarantee is enforced by tests that assert the dependency set exactly and run
the whole approve path with telemetry absent — a client moving stock between
their own stores is never made to stream anything to the Hub.

**The review IS the product.** Not a step on the way to automation. Automation
is revisited only after a live run where suggestions are approved
consistently — not before.

**PULL and PUSH rows stay separate**, even though they double-count headline
value, because merging them would lose the ability to filter the queue by
direction. Diagnosis beats a tidier number.

**The consoles stay outside Odoo.** The embed action was removed, not hidden —
removing a menu hides an entrance, not the door; it was still callable by RPC.

**We do not geocode through a third party.** Sending a customer's site
addresses to an external service is their data-protection decision, not ours.
Coordinates are supplied from their own records or set by hand.

---

## What is now measured rather than believed

| invariant | result on live depot data |
|---|---|
| order independence | 0.0000% — 3,567 / 3,567 identical |
| donor protection | 0 breaches of 1,113 pairs |
| self-transfers | 0 |
| 999 cover sentinel | 0 (was 1,431) |
| pass-through (arrives then leaves) | 0 (was 17) |

Suite: 1,360 local; 1,340 passed / 47 skipped on a **clean clone** in
`python:3.10-slim`. Addon: 59 tests in each POS configuration. e2e: 12/0.

---

## Traps paid for once

**Access rules had never been in git.** `*.csv` in `.gitignore` excluded every
`ir.model.access.csv` since the connector was written, so a clean clone
installed the modules with **no access rules**. The most consequential finding
of the session, and it was only visible from a fresh checkout.

**Reproduce CI with a clone, never the working tree** — the tree carries
untracked files CI never sees, and reports a false green.

**`os.environ.setdefault` returns the existing value.** CI set its own
`OASIS_SEED_PASSWORD`, so the DB was seeded with one password and the tests
signed in with another. Related: [[oasis-seed-password-trap]].

**Warehouses can share one address partner.** 16 of 17 did, so per-store
coordinates overwrote the same record 14 times and collapsed the chain onto a
single point.

**Never parse an identifier out of a display label.** A warehouse code
recovered from a label already truncated to 28 characters
(`"Meridian Fresh Northgate (NGATE-009)"` → `"NG"`) missed the site lookup, fell
back to company-wide stock, and raised a donor alarm that was not real. A false
alarm on that line is worse than no line.

**Customer names never enter `oasis/`.** A parity test scans the package for
the tokens in `demo_identity.py` and fails the build — it caught the comment
that explained the bug above, which had been written with the reference
customer's real chain and branch. The fictional identity exists for exactly
this; the allowlist is not the way out.

**An autouse fixture can blind a test.** A trial-clock fixture patched
`_first_run` and silently disabled four tests that establish posture by
*writing* state — one had been passing for the wrong reason.

**A migration silenced the application.** `fileConfig` in `migrations/env.py`
disabled every existing logger; it needs `disable_existing_loggers=False`.

---

## The pilot, in one paragraph

A pilot that cannot be measured is a demo. `--mode odoo-pilot` reports
acceptance (of lines somebody actually decided — a queue nobody has read is
not 0%), decision latency, where the engine is overruled broken down by kind /
perishability / store / category, what moved, and **whether any donor ended up
below its own safety floor**. That last line is the one number that would make
a chain stop trusting the queue; it is why the release cap exists.

Read the breakdown before touching thresholds: a uniform rejection rate is a
threshold problem, a concentrated one is a data problem in that store or
category, and those have completely different fixes.
