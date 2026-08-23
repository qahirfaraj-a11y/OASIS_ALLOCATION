# Session Handover — Transfers into Odoo, 2026-08-23

> **READ THIS FIRST** if you are picking up the transfer work with no context.
> 23 commits, `ecd6b6d5` … `f4fe4214`, branch `pre-mosaic-backup`.
> **Everything is pushed.** The blocker described in the 2026-08-20 handover
> is gone — see §0.
>
> Previous instalment: [`OASIS_Session_Handover_2026-08-20.md`](OASIS_Session_Handover_2026-08-20.md)

---

## Where it started and where it ended

**Started:** 16 commits of transfer work sitting on a local branch that could
not be pushed, an Odoo addon that worked on a synthetic depot, and an engine
whose safety floor was still a declared constant.

**Ended:** the branch is on origin, the addon is three independently
installable modules, the safety floor is measured from the customer's own
goods receipts, and there is a rehearsed six-step runbook that takes a real
multi-store Odoo from cold to a reviewed queue — plus a scorecard that says
whether the pilot is working.

---

## 0. The push blocker — cleared

`.git` was **2.26 GB**; five files were over GitHub's hard 100 MB per-file
limit, the worst a 2 GB zip under `oasis_vault/System_Backups/`.

`git filter-repo --invert-paths` removed those paths from history.
**`.git` 2.26 GB → 132 MB**, and 216 commits went to origin.

**`main` was deliberately left alone** at `93d8d151`. The rewrite changed
commit hashes, so local `main` and `origin/main` are one rewritten commit
apart. That divergence is known and accepted — do not "fix" it with a force
push without asking.

---

## 1. The engine — σ is no longer a constant

The safety floor was `default_safety_days = 14`. The seed said 10. Neither
number came from anything.

**σ is now R** — the safety floor a store keeps *is* the relief horizon, the
time until its next realistic delivery. One idea, not two.

`_safety_days(org_cd, product)` resolves in this precedence:

1. an explicit per-store policy, if the customer has set one
2. the derived relief horizon `_relief_days(...)` — measured
3. `default_safety_days` (14) — only when there is no evidence at all

`_relief_days` prefers **that store's own rhythm** over the supplier's network
rhythm, gated at `MIN_STORE_RECEIPTS_FOR_LOCAL_RHYTHM = 6`. A store that gets
a delivery weekly does not carry the same floor as one served monthly, and now
it does not have to.

Cadence comes from `supplier_patterns_by_store.json`, written by
`--mode odoo-rhythm` (§3).

### Two logic flaws fixed

**F1 — the 999 sentinel.** A donor with no sales had "cover" of 999 days, and
that number leaked into reports and comparisons as if it were a measurement.
`donor_days_cover` / `recipient_days_cover` are now `Optional[float]`;
`_reported_cover()` returns `None` rather than `_COVER_SENTINEL`. On the depot
the sentinel count went **1,431 → 0**.

**F2 — pass-through.** A store could appear as a recipient in one pass and a
donor in another, so stock arrived and left again. `_drop_pushes_into_net_donors`
makes the resolution one-directional: a net donor does not receive.
Pass-through went **17 → 0**.

**F3 was withdrawn on purpose.** Merging the duplicate PULL/PUSH rows would
lose the ability to filter the queue by direction. The rows stay separate.

**M1 — the release cap now binds.** `_releasable_transfer_qty()` was the
missing primitive: a *need* rounds up, a *pool* must never round up past
itself. Used at four sites in `decide()`.

> `decide()` is the **PO ordering** path. `scan_network_opportunities()` is the
> **transfer** path. They are different questions and this session only
> shipped the second one.

---

## 2. The Odoo addon — one module became three

A client can buy transfers, or replenishment, or telemetry, in any
combination. That is only real if the modules do not quietly need each other,
so the split is **enforced by tests**, not by intention.

| module | depends on | what it is |
|---|---|---|
| `oasis_connector` | `base` **only** | shared root menu, groups, settings |
| `oasis_transfers` | `oasis_connector`, `stock` | the review queue |
| `oasis_telemetry` | `oasis_connector` | `oasis.sync`, mapping, push client, cron |

`connectors/odoo/oasis_transfers/tests/test_module_split.py` asserts the
dependency set exactly, and runs the whole approve path **with telemetry
absent**. A client who wants OASIS to move stock between their own stores is
never made to stream anything to the Cloud Hub.

### What else changed in the addon

- **The console embed is closed.** Removing a menu hid an entrance, not the
  door — the action was still callable by RPC. It is gone. The Streamlit and
  Flet consoles stay deliberately outside Odoo.
- **Multi-company record rule** on `oasis.transfer.suggestion`, global, so it
  protects somebody. A test asserts it is global.
- **POS is no longer a hard dependency.** The addon tests run in both
  configurations, POS installed and not — 59 tests each way.
- **The lifecycle terminates.** States are `new / approved / done / rejected`.
  `_mark_completed()` fires on `_action_done()`; `_release_from_dead_picking()`
  fires on cancel/unlink, so a suggestion whose document dies stops claiming to
  be in flight.
- **Completed work has its own area** — `action_oasis_transfer_completed` +
  `menu_oasis_transfer_completed`, not a filter somebody has to know to apply.
  The data is retained for audit; it just stops crowding "what needs deciding
  today".

---

## 3. Three new read-only modes

| mode | file | what it answers |
|---|---|---|
| `--mode odoo-preflight` | `oasis/logic/odoo_preflight.py` | *Can Transfers safely run against THIS Odoo?* Judged per-warehouse, on the worst site. |
| `--mode odoo-rhythm` | `oasis/logic/odoo_supplier_rhythm.py` | Derives cadence from `stock.picking` and lead time from `purchase.order`. Writes the three pattern files. |
| `--mode odoo-pilot` | `oasis/logic/odoo_pilot_report.py` | *Is the pilot working?* Acceptance, latency, where the engine is overruled, what moved, and whether it hurt. |

All three change nothing.

`odoo-rhythm` refuses to overwrite richer data (`_may_replace`) and writes
atomically with a `.bak`. Thresholds: `MIN_RECEIPTS_FOR_RHYTHM = 3`,
`MIN_RECEIPTS_FOR_CONFIDENCE = 6`, `MAX_SENSIBLE_GAP_DAYS = 180`.

`odoo-pilot`'s most important line is **donors left short** — the one outcome
that would make a chain stop trusting the queue. It reports zero on the depot;
the release cap is why.

---

## 4. Wiring into a real customer's Odoo

**`OASIS_ODOO_STORES`** — the scan previously read
`store_network_seed.json`, a gitignored depot fixture. At any real customer
that is a `FileNotFoundError`. `store_universe()` now resolves in precedence:
`OASIS_ODOO_STORES` → depot seed → *every* warehouse in Odoo. The scan also
calls `load_dotenv()`, so `.env` is honoured.

**Large reads are paged, not capped.** `_read_paged()` with `PAGE_SIZE = 5000`,
`MAX_PAGES = 200`, applied to sell-through, open transfers and open PO lines.
`_warn_if_truncated()` says what was lost and what it costs. `read_group` was
tried first and killed the Odoo container.

**Geocoding: `connectors/odoo/geocode_warehouses.py`** — `--from-seed`,
`--set CODE=lat,lon`, `--show`, `--separate-addresses`. It deliberately calls
**no third-party geocoding API**: sending a customer's site addresses to an
external service is their data-protection decision, not ours.

---

## 5. The trial runbook

Rehearsed end to end against the live depot:

1. `--mode odoo-preflight` — read-only readiness
2. `--mode odoo-rhythm` — derive cadence and lead time from their own history
3. set `OASIS_ODOO_STORES` in `.env`
4. geocode the warehouses (`--separate-addresses` first if they share partners)
5. run the scan; suggestions post to the queue
6. `--mode odoo-pilot` weekly — the scorecard

A **Supervised Pilot Playbook** covering the customer-facing side is
published at
<https://claude.ai/code/artifact/5b3e61c9-c0cd-4d59-adc9-c839b2cb2dc9>:
one customer not three, no savings figure promised, an agreed exit, a named
queue owner, and four weeks with explicit gates — Week 0 read-only, Week 1
visible-untouched, Week 2 a handful by hand, Week 3 daily rhythm.

---

## 6. What is verified, and how

**Suite:** 1,360 passed locally; **1,340 passed / 47 skipped on a clean clone**
in `python:3.10-slim` (the GitHub Actions configuration). Addon: **59 tests**
in each POS configuration. e2e against the live depot: **12 passed / 0 failed**.

**Invariants measured on live depot data:**

| invariant | result |
|---|---|
| order independence | 0.0000% — 3,567 / 3,567 identical |
| donor protection | 0 breaches of 1,113 pairs |
| self-transfers | 0 |
| 999 cover sentinel | 0 (was 1,431) |
| pass-through | 0 (was 17) |

---

## 7. Traps paid for once — do not pay again

**Access rules had never been in git.** `*.csv` in `.gitignore` had excluded
every `ir.model.access.csv` since the connector was written. A clean clone
installed the modules with **no access rules at all**. Fixed by
`!connectors/odoo/*/security/*.csv`. This is the most consequential thing
found all session, and it was found *only* by running against a fresh
checkout.

**Reproduce CI with a clone, never the working tree.** My first container run
mounted the working tree, which carries untracked files CI never sees, and
reported a false green.

**`os.environ.setdefault` returns the existing value.** CI sets its own
`OASIS_SEED_PASSWORD`, so the DB was seeded with one password and the tests
signed in with another. Take setdefault's *return value*; never assume it set
what you passed.

**Warehouses can share an address partner.** 16 of 17 shared one, so writing
per-store coordinates overwrote the same record 14 times and collapsed the
chain onto a single point. The geocode script now refuses and tells you to run
`--separate-addresses` first.

**Never parse an identifier out of a display label.** The pilot scorecard
truncated a warehouse label to 28 characters and recovered a two-letter stub
instead of the code (`"Meridian Fresh Northgate (NGATE-009)"` → `"NG"`), missed
the site lookup, fell back to company-wide stock, and raised a donor alarm that
was not real. Resolve the code from `stock.warehouse`. A false alarm on that
line is worse than no line.

**And keep customer names out of `oasis/`.**
`tests/test_command_center_parity.py::test_the_customer_name_survives_only_where_it_must`
scans every file under `oasis/` for the tokens in `oasis/logic/demo_identity.py`
and fails the build. It caught exactly this — the comment above originally used
the reference customer's real chain and branch. Use the fictional identity
(`Meridian Fresh`, and the branches in `DEMO_BRANCHES`); do not add a file to
the allowlist to get around it.

**An autouse fixture can blind a test.** `_trial_is_not_a_clock` patched
`_first_run` and silently disabled four tests that establish posture by
*writing* state — one of which had been passing for the wrong reason.
`@pytest.mark.real_trial_clock` opts out.

**A migration silenced the application.** `fileConfig` in `migrations/env.py`
disabled every existing logger. It needs `disable_existing_loggers=False`.

**Git Bash mangles Odoo test tags.** `--test-tags /oasis_connector` becomes a
path; zero tests run and it reports success. Prefix with `MSYS_NO_PATHCONV=1`.

---

## 8. Deliberately not done

- **`oasis_ordering`** — the ordering module. Transfers had to be finished
  first; that was the explicit instruction.
- **F5** — two engines answering one question. Deferred knowingly.
- **F3** — de-duplicating headline value across PULL/PUSH rows. **Do not merge
  the rows**; the direction filter is worth more.
- **Automation.** *"Right now let us have the review as the product."*
  Revisit only after a live run where suggestions are approved consistently.

**Hands off — the operator's own uncommitted work:** `oasis_allocation/`, the
`store_allocation` line in `oasis/logic/license_manager.py`, and
`st_gat_dashboard.py`. Standing instruction: leave them alone.

---

## 9. Open threads

- **CI.** The `test` job failed on the last push (lint passed). Reproduce with
  a clean clone in `python:3.10-slim`, not the working tree.
- **`main`** is still one rewritten commit from `origin/main`, by choice.
- **Commercial policy** for the supplier portal — pricing, T&Cs, invoicing —
  remains the open item there, and it is not an engineering one.
