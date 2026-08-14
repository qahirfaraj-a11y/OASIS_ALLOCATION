"""Porting OASIS onto a live POS: the view layer must emit RUNNABLE DDL.

`--mode build-views` is the first step of a live port. Everything downstream —
preflight, intel bootstrap, ordering — reads the canonical names those views
provide. Five ways the emitted script was not runnable. The first three were
found by generating DDL from the shipped RXL profile instead of a hand-made
fixture; the last two only by EXECUTING it against a real RXL database
(TESTING11, 999 tables), which is why they survived every previous review:

  * a "_README" string key at the top of the profile crashed generation
  * views were emitted unqualified, colliding with the same-named source table
  * ``:store_level`` bind placeholders survived into CREATE VIEW
  * no ``GO`` between statements — SQL Server rejects every CREATE OR ALTER
    VIEW after the first, so only view #1 was ever created
  * the source was unqualified, so a view in schema OASIS bound to ITSELF
    ("contains a self-reference") instead of to the dbo table

The profile's column mapping was also largely wrong against the real schema
(ITEM_MST.DEPARTMENT/ACTIVE_FLAG/VENDOR_CD, VENDOR_ADDRESS_MST.VENDOR_NAME and
GRN_HDR do not exist); see rxl_schema_profile.json's _corrections note.
"""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.client_schema import (batch_separator_for, create_clause_for,
                                       generate_view_ddl,
                                       self_referencing_tables,
                                       substitute_params, unresolved_params,
                                       validate_profile)
from oasis.logic.release_packager import should_ship_clean

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
RXL_PROFILE = os.path.join(ROOT, "rxl_schema_profile.json")


@pytest.fixture
def rxl():
    if not os.path.exists(RXL_PROFILE):
        pytest.skip("rxl_schema_profile.json not present")
    with open(RXL_PROFILE, encoding="utf-8") as fh:
        return json.load(fh)


def test_the_rxl_profile_ships():
    """A client cannot port to a live RXL without the mapping."""
    assert should_ship_clean("rxl_schema_profile.json")[0]


def test_documentation_keys_do_not_crash_generation(rxl):
    """The profile leads with a _README string; specs are dicts."""
    stmts = generate_view_ddl(rxl, create_clause_for("mssql"), schema="OASIS")
    assert stmts
    assert not any("_README" in s for s in stmts)


def test_metadata_keys_are_ignored_not_rendered():
    profile = {
        "_README": "notes",
        "_version": 3,
        "ITEM_MST": {"source": "PRODUCTS", "columns": {"ITM_CD": "CODE"}},
    }
    stmts = generate_view_ddl(profile, "CREATE VIEW")
    assert len(stmts) == 1 and "PRODUCTS" in stmts[0]


def test_rxl_collides_without_a_schema_and_the_profile_says_so(rxl):
    """RXL already uses the canonical table names — that IS the collision.

    Only checks tables mapped via `source`. ITEM_MST and STOCK_MASTER are
    raw-`sql` views (they need JOINs), and those qualify their own sources.
    """
    clashes = self_referencing_tables(rxl)
    assert "POS_SALES_DTL" in clashes and "POS_SALES_HDR" in clashes
    assert validate_profile(rxl)["self_referencing"] == clashes


def test_raw_sql_views_must_qualify_their_own_sources(rxl):
    """A gotcha worth pinning: `source_schema` cannot rewrite a raw-`sql` body.

    generate_view_ddl qualifies the `source` of a 1:1 mapping, but a hand-written
    `sql` view is emitted verbatim — so if its FROM clauses are unqualified it
    will self-reference once placed in a non-default schema, exactly the failure
    a live SQL Server gave us. The profile's raw views therefore say `dbo.`
    themselves, and must keep doing so.
    """
    for canon in ("ITEM_MST", "STOCK_MASTER"):
        spec = rxl[canon]
        assert spec.get("sql"), f"{canon} is expected to be a raw-sql view"
        body = spec["sql"]
        assert "FROM dbo." in body, f"{canon} must qualify its FROM with dbo."
        assert f"FROM {canon}" not in body, (
            f"{canon} selects FROM an unqualified {canon} — that self-references"
        )


def test_a_schema_qualifies_the_view_but_never_the_source(rxl):
    """OASIS.STOCK_MASTER AS SELECT ... FROM dbo.STOCK_MASTER."""
    stmts = generate_view_ddl(rxl, create_clause_for("mssql"), schema="OASIS",
                              source_schema="dbo")
    stock = next(s for s in stmts if "STOCK_MASTER" in s)
    assert "VIEW OASIS.STOCK_MASTER" in stock
    assert "FROM dbo.STOCK_MASTER" in stock
    assert "FROM OASIS.STOCK_MASTER" not in stock


def test_an_unqualified_source_would_self_reference(rxl):
    """The failure a live SQL Server gave us: inside a view in schema OASIS, an
    unqualified source binds to the VIEW, not the table — "contains a
    self-reference" — so every view must name its source schema."""
    stmts = generate_view_ddl(rxl, create_clause_for("mssql"), schema="OASIS")
    # POS_SALES_DTL is a 1:1 `source` mapping, so it is the one the
    # source_schema logic actually rewrites (the raw-sql views self-qualify).
    dtl = next(s for s in stmts if "VIEW OASIS.POS_SALES_DTL" in s)
    assert "FROM POS_SALES_DTL" in dtl           # no source_schema given
    qualified = generate_view_ddl(rxl, create_clause_for("mssql"),
                                  schema="OASIS", source_schema="dbo")
    dtl_q = next(s for s in qualified if "VIEW OASIS.POS_SALES_DTL" in s)
    assert "FROM dbo.POS_SALES_DTL" in dtl_q


def test_a_source_that_is_already_qualified_is_left_alone():
    profile = {"ITEM_MST": {"source": "other.PRODUCTS",
                            "columns": {"ITM_CD": "CODE"}}}
    sql = generate_view_ddl(profile, "CREATE VIEW", schema="OASIS",
                            source_schema="dbo")[0]
    assert "FROM other.PRODUCTS" in sql and "dbo.other" not in sql


def test_mssql_needs_a_batch_separator_between_views():
    """CREATE OR ALTER VIEW must be first in its batch; without GO, SQL Server
    rejects every statement after the first."""
    assert batch_separator_for("mssql") == "GO"
    assert batch_separator_for("postgres") == ""


def test_a_raw_sql_view_declares_what_it_provides():
    """Real RXL needs joins (SUPPLIER_CD is on BASIC_CP_MST, not ITEM_MST), so
    the 1:1 mapping cannot express ITEM_MST at all."""
    profile = {"ITEM_MST": {"sql": "SELECT a.X AS ITM_CD FROM dbo.A a JOIN dbo.B b ON 1=1",
                            "provides": ["ITM_CD"]}}
    sql = generate_view_ddl(profile, "CREATE VIEW", schema="OASIS")[0]
    assert sql.startswith("CREATE VIEW OASIS.ITEM_MST AS SELECT")
    assert "JOIN dbo.B" in sql


def test_the_rxl_item_view_supplies_the_required_contract_columns(rxl):
    """ITEM_MST is the table RXL matches least: DEPARTMENT, ACTIVE_FLAG and
    SUPPLIER_CD all come from somewhere other than a same-named column."""
    provides = set(rxl["ITEM_MST"]["provides"])
    for required in ("ITM_CD", "ITM_LONG_NAME", "DEPARTMENT", "SUPPLIER_CD",
                     "ACTIVE_FLAG"):
        assert required in provides


def test_a_renamed_source_is_not_a_collision(rxl):
    """SUPPLIER_MST reads VENDOR_ADDRESS_MST, so it never collided."""
    assert "SUPPLIER_MST" not in self_referencing_tables(rxl)


def test_bind_placeholders_are_detected(rxl):
    """CREATE VIEW cannot bind a parameter — catch it before the DBA does."""
    stmts = generate_view_ddl(rxl, create_clause_for("mssql"), schema="OASIS")
    assert "store_level" in unresolved_params(stmts)


def test_substituting_the_store_level_makes_the_ddl_literal(rxl):
    stmts = generate_view_ddl(rxl, create_clause_for("mssql"), schema="OASIS")
    stmts = substitute_params(stmts, {"store_level": "1"})
    assert unresolved_params(stmts) == []
    stock = next(s for s in stmts if "STOCK_MASTER" in s)
    assert "SM_LEVEL_NUMBER = 1" in stock
    assert ":store_level" not in stock


def test_the_rxl_profile_satisfies_the_required_contract(rxl):
    """Every REQUIRED canonical column is mapped or supplied as a literal."""
    v = validate_profile(rxl)
    assert v["ok"], (
        f"missing tables: {v['missing_tables']}, "
        f"missing columns: {v['missing_columns']}"
    )


def test_a_malformed_spec_is_reported_not_raised():
    """validate_profile is what a DBA runs to check their own profile."""
    v = validate_profile({"ITEM_MST": "oops, a string"})
    assert not v["ok"]
    assert "ITEM_MST" in v["missing_columns"]
