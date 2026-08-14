/* ---------------------------------------------------------------------------
   OASIS read-only login for the RXL database (TESTING11).

   REPLACE <YourStrongPasswordHere> BEFORE RUNNING. Do not commit it anywhere.

   Why DEFAULT_SCHEMA matters: the OASIS adapter queries UNQUALIFIED table names
   (FROM ITEM_MST, not FROM OASIS.ITEM_MST). Setting this login's default schema
   to OASIS makes those names resolve to our canonical views first, falling back
   to dbo for anything not overridden — with no adapter change. Without it the
   adapter reads dbo.ITEM_MST directly, whose columns do NOT match the contract
   (no DEPARTMENT, no ACTIVE_FLAG, no SUPPLIER_CD) and every query fails.

   The grants are SELECT-only. OASIS must never write to a client's POS; its own
   store is a separate database (preflight now WARNs if they are the same one).
--------------------------------------------------------------------------- */

USE master;
GO

IF NOT EXISTS (SELECT 1 FROM sys.server_principals WHERE name = N'oasis_ro')
    CREATE LOGIN oasis_ro
        WITH PASSWORD = N'<YourStrongPasswordHere>',
             CHECK_POLICY = ON,
             DEFAULT_DATABASE = TESTING11;
GO

USE TESTING11;
GO

IF NOT EXISTS (SELECT 1 FROM sys.database_principals WHERE name = N'oasis_ro')
    CREATE USER oasis_ro
        FOR LOGIN oasis_ro
        WITH DEFAULT_SCHEMA = OASIS;      -- the critical line
GO

/* Read the canonical views... */
GRANT SELECT ON SCHEMA::OASIS TO oasis_ro;
GO

/* ...and the underlying dbo tables the views select FROM. A view does not
   confer rights on its base tables unless ownership chaining applies, and it
   does not here because the view and table owners differ. */
GRANT SELECT ON SCHEMA::dbo TO oasis_ro;
GO

/* Verify: this should report OASIS, and resolve ITEM_MST to the view. */
SELECT  USER_NAME()                              AS db_user,
        SCHEMA_NAME(default_schema_id)           AS default_schema
FROM    sys.database_principals
WHERE   name = N'oasis_ro';
GO
