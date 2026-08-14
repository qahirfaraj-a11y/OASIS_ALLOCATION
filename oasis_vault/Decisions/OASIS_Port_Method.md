# Method: Mapping OASIS onto Any POS

Written 2026-08-13, after the RXL schema profile turned out to be substantially
wrong. Companion to [[RXL_Integration_Log_2026-08]].

## Why this exists

The first RXL profile was written against **documentation**. When it was finally
run against a real RXL database it failed at the first command, and three of the
columns it mapped did not exist — including three that OASIS marks **required**.

That failure mode will repeat for every future POS unless the method changes.
The fix is not "be more careful reading docs"; it is to derive the mapping from
a live system.

## The procedure

1. **Restore or attach the vendor's real database.** An *empty* schema is enough
   for mapping — it still has every table, column and type. RXL's `TESTING11`
   had 999 tables and ~0 business rows, and that was sufficient to find six
   blockers.
2. **Arm a statement + error capture** filtered to that database
   (SQL Server: Extended Events with an `event_file` target and
   `STARTUP_STATE=ON` so it survives restarts).
3. **Snapshot row counts for every table** before touching the UI.
4. **Perform the real workflow in the vendor's own UI** — create a store, a
   till, items, stock, then ring a sale.
5. **Diff the row counts.** This reveals every table the vendor writes to,
   including sequence counters, audit rows and hierarchy links that no amount of
   schema reading would surface.
6. **Read the captured INSERTs** for the true column shape and value formats,
   then build the mapping from that evidence.
7. **Execute the generated DDL against the real database.** Generating it is not
   proof. Two of the six RXL blockers (missing `GO` batch separators, and views
   self-referencing when placed in a non-default schema) were invisible until
   the script actually ran.

## Hard-won rules

- **Validate at COLUMN level, not table level.** Two of the three RXL drift
  failures were columns on tables that existed. A table-only preflight declares
  such a database healthy, then fails at runtime.
- **A vendor that already uses your canonical table names is the hard case.**
  You cannot create a view named `ITEM_MST` that selects `FROM ITEM_MST`. Put
  views in their own schema, qualify the **source** explicitly (`dbo.`), and set
  the service account's `DEFAULT_SCHEMA` — otherwise the view binds to itself.
- **A 1:1 column mapping is not always expressible.** RXL keeps the item/vendor
  link on `BASIC_CP_MST`, not on the item. Any profile format needs a raw-SQL
  escape hatch plus an explicit `provides` list so validation still works.
- **Captured literals show how the app WRITES, not how the column is STORED.**
  Check declared types before drawing conclusions. (See the `BILL_DT` correction
  in [[RXL_Integration_Log_2026-08]].)
- **Never write to the vendor's POS database.** OASIS keeps its own store; every
  write goes to `store_engine`. Preflight now WARNs when the POS and OASIS store
  resolve to the same database, because otherwise OASIS silently creates its own
  tables inside a live production POS.
- **Attributes the ERP does not carry are ours, not theirs.** RXL has no lead
  time, reliability or order frequency — those come from OASIS's derived supplier
  patterns. Map them to `NULL` and be explicit about the provenance.
