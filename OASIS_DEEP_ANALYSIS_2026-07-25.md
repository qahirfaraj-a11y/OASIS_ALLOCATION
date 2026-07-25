# O.A.S.I.S. — Deep Analysis: Journey, Underutilization, Logic Mismatches
**Date:** 2026-07-25 · **Scope:** client side (the shipped zip → the retailer's machine)
**Method:** static trace of the shipped surface — every file read, every claim below cites file:line.
**Status:** analysis only. No code changed.

---

## 0. Executive read

The client side is *built*. What this analysis finds is that it is not *delivered*: the
gap is between the working tree (where everything works, because this machine has the
data and config files) and the release zip (where several load-bearing files are
excluded by design). Three of the five first-run choices a retailer sees do not work
from a clean zip, and the entire Chapter-11 engine layer is dormant because its one
config file is double-excluded from the package.

Nothing here is a design failure. Every finding is a wiring gap — a value written in
one place and read from another, or a file the packager was told to drop.

| # | Severity | Finding | Where it bites |
|---|---|---|---|
| S1 | **Ship-blocking** | Engine config never ships → all Chapter-11 engines silently off | Every client install |
| S2 | **High** | "Connect a POS" choice never reaches the consoles | Retailer's first hour |
| S3 | **High** | 2 of 5 onboarding buttons fail on a clean zip (need unshipped spreadsheets) | Retailer's first screen |
| S4 | **High** | `apply_init` writes `source="init"`, unknown to `SOURCES`/badge/trial | Catalogue installs |
| S5 | **High** | Command Center (:8501) is outside suite login + SSO | Every console hop |
| S6 | Medium | Suite bar doesn't know which console it's in (`license_module` ≠ console key) | Navigation |
| S7 | Medium | Home ignores `resolved_db_path()` — contradicts the consoles it launches | Home panel + Start All |
| S8 | Medium | 8 of 10 supplier insight cards are built, guarded, tested — never emitted | Supplier value |
| S9 | Medium | ~11 user-facing `--mode`s have no UI affordance; Home prints CLI text instead | Discoverability |
| S10 | Low | `install.bat` closing instructions drift from the actual product | First impression |

---

## S1 — SHIP-BLOCKING: the Chapter-11 engine layer is dark in every client release

`oasis/data/oasis_engines_config.json` is the feature-flag + parameter file for the
engine layer. It is excluded from the release **twice**:

- blacklist mode: `_EXCLUDE_GLOBS` contains `"oasis/data/*.json"` — `release_packager.py:61`
- strict whitelist mode (the default, `clean=True`): `_OASIS_DATA_WHITELIST = {"supplier_calendar.py", "__init__.py"}` → everything else under `oasis/data` returns `False, "oasis/data payload"` — `release_packager.py:107`, `:149-151`

Six shipped consumers read that exact filename and **all** fail soft to empty config:

| Consumer | Line | Fallback when the file is missing |
|---|---|---|
| `oasis/logic/order_engine.py` | `:78-104` | `return {"engines": {}}` |
| `oasis/logic/lata_shield.py` | `:28-38` | `return {}` |
| `oasis/logic/mande_triage.py` | `:29` | empty |
| `oasis/logic/dharam_revenue.py` | `:29` | empty |
| `oasis/logic/amit_gatekeeper.py` | `:45` | empty |
| `oasis/logic/amit_governance.py` | `:35` | empty |

The consequence chain is exact: `order_engine.is_engine_enabled()` reads
`engines.get(name, {}).get('enabled', False)` — `order_engine.py:106-109`. With
`{"engines": {}}` **every engine returns False**, so `_load_engine_caches()`
(`:111+`) loads nothing and AMIT / LATA / DHARAM / MANDE never activate. The
Operations Console's engine tab renders the empty state
`"No engine config — oasis_engines_config.json not found."` (`oasis/ui/shell.py:842`).

The retailer therefore runs the base order engine while holding a licence for modules
whose engines are switched off — and nothing tells them. `install.bat`'s preflight
doesn't check for it. `DEPLOYMENT_GUIDE.md:156` even instructs the operator to
*"Verify `oasis/data/oasis_engines_config.json` is present and engines are
`enabled: true`"* — a file the zip is built never to contain. (`Dockerfile:58` copies
a root-level copy, which is a third, separate path.)

**Fix shape:** ship `oasis/data/oasis_engines_config.default.json` (add to
`_OASIS_DATA_WHITELIST`), have `_load_engines_config` fall back to it before falling
back to `{}`, and add a preflight assertion so a config-less install is loud, not
silent. Verify first that the current file holds only flags/parameters and no client
data — if it does, sanitise into the default.

---

## S2 — HIGH: "Connect a POS" is a dead end — the wizard's choice never reaches the consoles

Trace the third onboarding card end to end:

1. UI writes the URL → `OB.apply_connect(url.strip())` — `oasis/ui/onboarding.py:71`
2. `apply_connect` verifies reachability, then records **`db_url=db_url`** — `oasis/logic/onboarding.py:174`
3. `resolved_db_path()` — the declared "single source of truth for the active store DB" — consults `OASIS_DB_PATH`, then onboarding **`db_path`**, then profile `db_path`, then the default. It never reads `db_url` — `oasis/logic/onboarding.py:81-103`
4. `oasis/logic/db.py:41-49` `get_pos_db_url()` returns `os.getenv("OASIS_POS_DB_URL") or get_db_url()` — env only. Nothing in the codebase ever writes `OASIS_POS_DB_URL`.

So after a *successful* connect, `app.py:25` and `app_intel.py:25` open
`oasis/data/rhapta_pos.db` — a file that on a connect-only install **does not exist**.

The wizard knows. It prints the workaround as product copy:

> `st.info("Set OASIS_POS_DB_URL to this URL (or in your .env) so every console reads it, then reload.")` — `oasis/ui/onboarding.py:74-75`

We shipped a manual env-var edit as one of four front-door options. Worse, the
provenance chip then reads `"DATA: connected POS (<host>)"` from the stored `db_url`
(`oasis/ui/onboarding.py:141-145`) while the console is actually reading a
nonexistent local sqlite file — the honesty rule (audit C1/G1) is violated by exactly
the path that most needs it.

**Fix shape:** make the connect path persist the URL where the runtime reads it
(write `OASIS_POS_DB_URL` into a config the process loads at startup), and have
`resolved_db_path()` / `get_pos_db_url()` consult the onboarding record as a tier in
their priority chain. Then the badge tells the truth by construction.

---

## S3 — HIGH: two of five onboarding paths need files the release never ships

The wizard offers **five** paths (`oasis/ui/onboarding.py:32-104`):

| Card | Action | On a clean client zip |
|---|---|---|
| 🧪 Sample store | `apply_demo` → `demo_seed.demo_catalog_rows()` (in code) | ✅ works |
| 📭 Start fresh | `apply_empty` → empty schema | ✅ works |
| 🔌 Connect a POS | `apply_connect` | ⚠️ connects, then see **S2** |
| 🏢 Build from Catalogue | `apply_init` → `init_install()` — reads catalogue spreadsheets | ❌ `catalog_error` |
| 🏬 Multi-store demo | `apply_multi_demo` → `init_install(profile="multi")` | ❌ `catalog_error` |

`.xlsx`, `.xls`, `.csv` are all in `_EXCLUDE_EXTS` (`release_packager.py:53-55`) and
`oasis/data` payload is dropped wholesale in whitelist mode. Both `init_install`
paths depend on those spreadsheets, so both buttons return the error branch the UI
already handles (`oasis/ui/onboarding.py:87-88`, `:103`).

This is the retailer's *first screen*: two buttons error, one needs a hand-edited env
var. Note also that `install.bat:75-77` advertises exactly four choices — *"sample
store, start empty, connect your POS, or the multi-store demo network"* — naming one
of the two broken paths and omitting the fifth card that actually exists.

The multi-store card is the sharper problem, because it's the one we added in the last
wave specifically so a multi install goes through an explicit choice
(`onboarding.py:149-162`). Its unit test monkeypatches `init_install`
(`tests/test_suite_sso.py:76`), so the test passes on a machine where the real call
would fail.

**Fix shape:** either give `demo_seed` a code-resident multi-store topology (same
approach that makes the single-store sample work without spreadsheets), or gate both
cards behind a "catalogue files detected" check so the retailer sees an explanation
and an upload affordance instead of a red error.

---

## S4 — HIGH: `apply_init` writes a source value the rest of the system doesn't recognise

`SOURCES = ("demo", "empty", "connect")` — `oasis/logic/onboarding.py:29`.
`apply_init` records `_record("init", ...)` — `:209`.

Three consequences, all silent:

1. `is_onboarded()` → True (it only checks truthiness of `source`, `:59-60`), so the wizard hides — correct.
2. `data_source_badge()` has branches for `demo`, `empty`, `connect`, then `else` → **`"DATA: not onboarded — run first-launch setup from Home"`** (`oasis/ui/onboarding.py:136-147`). A real catalogue-built store is labelled un-onboarded in every console header, permanently.
3. `apply_init` never calls `_maybe_restart_trial` (compare `apply_empty:143`, `apply_connect:173`). The one path that is unambiguously the retailer's own real data is the one path that does **not** get the fresh 14-day clock the trial-restart rule exists to give (`onboarding.py:107-123`).

**Fix shape:** add `"init"` to `SOURCES`, give the badge an `init` branch naming the
store/profile, and call `_maybe_restart_trial` in `apply_init`.

---

## S5 — HIGH: the Command Center (:8501) sits outside suite login and SSO

`ops_dashboard.py` calls `console_gate` (`:68`), `data_source_badge` (`:70`) and
`suite_links(st, "command")` (`:74-75`) — but it never calls
`oasis.ui.auth.require_login`. It imports `authenticate` from `auth_manager` directly
(`:48`) and runs an entirely parallel auth stack: its own `show_login_screen()`
(`:498`), its own `login_form` (`:520`), a direct `authenticate(username, password,
DB_PATH)` call (`:532`), and an explicit "unauthenticated state" fail-closed fallback
user at `:564-571`.

Because `try_adopt_sso` only runs inside `require_login` (`oasis/ui/auth.py:139+`),
the Command Center cannot adopt a suite `sid`. The divergence is deeper than a missing
call: the Command Center stores its user under the bare session key `'user'`
(`ops_dashboard.py:494-497`, `:565`), while the suite uses `oasis.ui.auth.USER_KEY`.
Even if a `sid` were adopted, the two halves would not see each other's session. And the suite bar we shipped last wave
*does* append `?sid=` to it — the exclusion is only for `hub`
(`oasis/ui/home.py:103`). So the user clicks a signed-in link and lands on a second,
different login form. SSO in practice covers `app.py`, `app_intel.py`,
`st_gat_dashboard.py`, `allocation_app.py` — not the console the sid link points at.

Separately: `home_app.py` (`:24`) calls `render_home_page` with no auth at all, and
Home can start/stop console processes (`oasis/ui/home.py:189-203`), activate a
licence (`:246-248`) and reset onboarding (`:274`). It's also where a first-time user
starts — so no `sid` exists until they log into a console, meaning Home's own
`link_button` opens (`:223`) always hit a login form. The front door doesn't
participate in the single-sign-on it fronts.

---

## S6 — MEDIUM: the suite bar doesn't know which console it's in

`oasis/ui/shell.py:128` — `suite_links(st, license_module)`.
`license_module` is a **licence module name**, and both consoles pass `"core"`
(`app.py:36`, `app_intel.py:36`).

`suite_links` skips the current console with `if c["key"] == current_key: continue`
(`oasis/ui/home.py:100-101`). `"core"` matches no key in `CONSOLES` (`ops`, `command`,
`intel`, `stgat`, `hub`), so the skip never fires: the Operations Console lists
"Operations" in its own suite bar, and Operations and Intelligence render byte-identical
bars. The user loses their sense of place — exactly what the bar was added to give.

`tests/test_suite_sso.py:65` calls `suite_links(st, "ops")` directly, so the test
exercises a call shape production never makes.

**Fix shape:** add an explicit `console_key` parameter to `render_console` and pass
`"ops"` / `"intel"`; keep `license_module` for the licence gate only.

---

## S7 — MEDIUM: Home ignores `resolved_db_path()` — the fragmentation W-7 was meant to close

`resolved_db_path()` exists precisely so nothing hardcodes the default
(`oasis/logic/onboarding.py:81-92`, docstring: *"Every console and the Home app should
call this instead of hardcoding a default path. This closes W-7"*).

Who actually calls it: `app.py:25`, `app_intel.py:25`, `license_manager.py:480`,
`supervisor.py:147`.

Who doesn't:
- `oasis/ui/home.py` — builds the path by hand from `os.getenv("OASIS_DB_PATH", <default>)` at `:160`, `:184`, `:253` (three independent copies of the same expression)
- `entrypoint.py` — the same hardcoded fallback at `:254, 741, 758, 767, 777, 786, 854, 889, 906, 966, 975, 988` (twelve occurrences)
- `oasis/logic/install_profile.py:74`

The visible failure: a store built at a non-default path (init / multi profile) shows
**"Store DB — missing"** on Home's System panel while the consoles it launches open
the store fine. Worse, `Start All Consoles` passes that wrong path down as
`db_path=_db` (`home.py:191`), so Home actively hands each console a path the console
would otherwise have resolved correctly. Home contradicts the product it fronts.

---

## S8 — MEDIUM (underutilization): 8 of 10 supplier insight cards are never emitted

`oasis/logic/insight_emitter.py` exports ten card builders:

| Builder | Line | Emitted by `build_cards`? |
|---|---|---|
| `reliability_card` | `:82` | ✅ (`insight_push.py:107`) |
| `sei_card` | `:272` | ✅ (`insight_push.py:123`) |
| `velocity_card` | `:101` | ❌ |
| `halo_card` | `:113` | ❌ |
| `reorder_card` | `:129` | ❌ |
| `broken_halo_card` | `:141` | ❌ |
| `archetype_card` | `:160` | ❌ |
| `capital_efficiency_card` | `:177` | ❌ |
| `ncp_card` | `:215` | ❌ |
| `cannibalization_card` | `:254` | ❌ |

Plus the helpers `efficiency_band` (`:202`) and `ncp_position` (`:243`), reachable only
through the unemitted cards.

`insight_push.build_cards()` (`:89-129`) has exactly two source blocks: the supplier
scorecard → reliability, and `mande_purge_report.json` → SEI. Everything else the
emitter can shape — velocity, halo/attachment, reorder, broken halo, archetype mix,
capital efficiency, NCP, cannibalisation — is written, forbidden-field-guarded
(`_assert_supplier_safe`, `:45`), consent-gated on the hub, tier-mapped and unit-tested,
and then never sent. This is the largest block of finished-but-unreachable capability
on the client side, and it's on the revenue side of the product (the two-sided
exchange), not a nice-to-have.

Note this is a *wiring* gap, not a data gap: the on-prem engine already computes these
numbers for its own consoles. `build_cards` simply has no block that reads them.

---

## S9 — MEDIUM (navigation): Home prints CLI commands where it could offer a button

`entrypoint.py:528-546` declares **56** `--mode` choices. `OASIS.bat` exposes **7**
(`:20-37`): home, ops, intel, command, market, hub, license-status.

User-facing modes with no affordance in any UI:
`backup`, `restore`, `value-report`, `metering-report`, `supplier-scorecard`,
`category-report`, `sku-deepdive`, `assess`, `upgrade`, `push-insights`, `serve`.

The sharpest examples are on Home itself, which *names the command instead of running it*:

- `"Latest backup: none — run --mode backup"` — `oasis/ui/home.py:265`
- `"No value report yet — run --mode value-report."` — `oasis/ui/home.py:271`

Both are one-click actions the page already has the context for. As written, the
retailer's path to their first backup is: read a caption, find a terminal, activate the
venv, type a mode string.

`serve` is the supervised service shipped last wave — reachable only by discovering
`serve.bat` in the folder. OASIS.bat's menu, the single front door we consolidated 31
launchers into, doesn't mention it, and neither does `install.bat`'s closing block.

---

## S10 — LOW: `install.bat`'s closing instructions drift from the product

`install.bat:82-93` tells the retailer:

- *"Launch: double-click run_oasis_home.bat"* — but `OASIS.bat` is now the single menu, and it self-heals (runs `install.bat` when the venv is missing, `OASIS.bat:7-11`), so it's strictly the better front door.
- four data choices, one of which (multi-store demo) fails on a clean zip — see **S3**.
- *"change with --mode set-password"* with no venv-qualified command, so the copy-paste doesn't work.
- no mention of `serve.bat` / `register_service.bat`, both of which are whitelisted and shipped (`release_packager.py:88`).

---

## S11 — module-level dead code: almost none (the good news)

A reference scan of every module in `oasis/logic` and `oasis/ui` against what the
shipped entry points (`entrypoint.py`, `app.py`, `app_intel.py`, `ops_dashboard.py`,
`home_app.py`, `st_gat_dashboard.py`, `allocation_app.py`, `intraday_sim.py`) plus the
whole `oasis/` package actually reference:

| Module | Referenced by |
|---|---|
| `oasis/logic/ai_employee.py` | **nothing** — not shipped code, not tests, not dev scripts |
| `oasis/logic/pulse_sync.py` | **nothing** |
| `oasis/logic/simulation_pipeline.py` | dev scripts only (6 refs, no shipped caller, no test) |

`oasis/ui`: **zero** unreferenced modules.

So the underutilization in this codebase is *not* at the module level — it's at the
**function and feature level** (S8's eight unemitted card builders, S9's eleven
unreachable modes). That's a meaningfully better position to be in: there is very
little abandoned code, but there is a lot of finished code with no path to a user.

The three modules above are worth a decision rather than a fix — delete, or wire up if
they represent intended capability. They ship today (both `oasis/logic` files are
inside the whitelisted package) and add surface area a client could stumble into.

---

## Journey summary — what a real retailer actually experiences

| Step | Designed | Actual (clean zip) |
|---|---|---|
| Unzip → `install.bat` | venv, deps, licence status | ✅ works |
| Launch | `OASIS.bat` menu | ✅ works (but installer points elsewhere — S10) |
| First-run choice | 5 paths | 2 work, 1 needs manual env edit, 2 error — S2/S3 |
| Data provenance chip | always honest | lies on `connect`, says "not onboarded" on `init` — S2/S4 |
| Engine layer | AMIT/LATA/DHARAM/MANDE per licence | **all off, silently** — S1 |
| Log in once | suite SSO | Command Center re-prompts; Home never mints a sid — S5 |
| Move between consoles | bar shows siblings | bar shows self, identical everywhere — S6 |
| Home system panel | matches the consoles | says "missing" for non-default DBs — S7 |
| Routine ops (backup, value report) | in-product | CLI-only, named in captions — S9 |
| Supplier value out | 10 card kinds | 2 — S8 |

The through-line: **the working tree is the only environment where OASIS is whole.**
Every high-severity finding is a file or value that exists here and not there, or is
written under one key and read under another. That's a good place to be — none of it
is a redesign — but it means the release artifact has never been exercised as a
retailer would exercise it.

---

## Recommended order of work

**Wave A — make the zip whole (nothing else matters until this is true)**
1. S1 — ship a default engine config + preflight assertion.
2. S3 — code-resident multi-store topology, or an honest gate on the two catalogue cards.
3. S2 — persist the POS URL where the runtime reads it; make the badge true by construction.
4. S4 — `"init"` into `SOURCES`, badge branch, trial restart.

**Wave B — one login, one sense of place**
5. S5 — `require_login` in `ops_dashboard`; decide Home's auth posture and let it mint the sid.
6. S6 — explicit `console_key` for `suite_links`.
7. S7 — Home and `entrypoint` call `resolved_db_path()`.

**Wave C — surface what's already built**
8. S8 — extend `build_cards` to the remaining eight kinds (biggest value-per-line on the list).
9. S9 — buttons on Home for backup / value report; `serve` in the OASIS.bat menu.
10. S10 — installer copy.

**Verification standard:** the `_st()` crash last wave was caught by a live browser
click, not by unit tests, and three findings here (S3, S6, plus S5's SSO reach) are
invisible to the current tests because the tests monkeypatch or call helpers with
argument shapes production never uses. So Wave A should be verified by *unzipping the
built artifact into a clean directory and running it there* — not by running the
working tree.
