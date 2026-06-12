"""Tests for the composable GreenfieldPipeline orchestrator."""

import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.allocation_strategies import (
    GreenfieldPipeline, GREENFIELD_PASS_SEQUENCE,
)
from oasis.logic.order_engine import OrderEngine
from tests.test_allocation_snapshot import FIXTURE_PRODUCTS

BUDGET = 1_000_000.0


@pytest.fixture
def engine(tmp_path):
    data_dir = tmp_path / "data"
    data_dir.mkdir(exist_ok=True)
    return OrderEngine(str(data_dir))


def _products():
    return json.loads(json.dumps(FIXTURE_PRODUCTS))


def _result_signature(result):
    return (
        {r["product_name"]: int(r.get("recommended_quantity", 0))
         for r in result["recommendations"]},
        round(float(result["summary"].get("total_cash_used", 0)), 2),
    )


def test_pipeline_matches_engine_method(engine):
    via_engine = engine.apply_greenfield_allocation(_products(), total_budget=BUDGET)
    via_pipeline = GreenfieldPipeline(engine).execute(_products(), budget=BUDGET)
    assert _result_signature(via_engine) == _result_signature(via_pipeline)


def test_default_sequence_names():
    names = [n for n, _ in GREENFIELD_PASS_SEQUENCE]
    assert names == [
        "preprocess", "pass1_width", "pass1_5_liquidity_prune",
        "pass2_depth", "premium_trim", "pass2b_flex_pool",
        "pass3_anchor_mov", "pass4_mop_up",
    ]


def test_skip_pass4_reduces_utilization(engine):
    full = GreenfieldPipeline(engine).execute(_products(), budget=BUDGET)
    no_mopup = (
        GreenfieldPipeline(engine)
        .skip("pass4_mop_up")
        .execute(_products(), budget=BUDGET)
    )
    assert (no_mopup["summary"]["total_cash_used"]
            < full["summary"]["total_cash_used"])
    assert no_mopup["summary"]["mop_up_cash"] == 0


def test_skip_does_not_mutate_default_sequence(engine):
    p = GreenfieldPipeline(engine)
    p.skip("pass4_mop_up")
    assert "pass4_mop_up" not in p.pass_names
    assert any(n == "pass4_mop_up" for n, _ in GREENFIELD_PASS_SEQUENCE)
    assert "pass4_mop_up" in GreenfieldPipeline(engine).pass_names


def test_replace_with_noop(engine):
    baseline = GreenfieldPipeline(engine).skip("pass2b_flex_pool")

    def noop_flex(ctx):
        ctx.p2b_cost = 0.0
        ctx.p2b_consignment_val = 0.0

    replaced = GreenfieldPipeline(engine).replace("pass2b_flex_pool", noop_flex)
    # A no-op replacement must behave exactly like skipping the pass
    # (the final audit recomputes costs from physical quantities).
    r1 = baseline.execute(_products(), budget=BUDGET)
    r2 = replaced.execute(_products(), budget=BUDGET)
    assert _result_signature(r1) == _result_signature(r2)


def test_insert_custom_stage_sees_context(engine):
    seen = {}

    def spy_stage(ctx):
        seen["budget"] = ctx.total_budget
        seen["pass1_cost"] = ctx.pass1_cost
        seen["n_recs"] = len(ctx.recommendations)

    (GreenfieldPipeline(engine)
        .insert_after("pass1_width", "spy", spy_stage)
        .execute(_products(), budget=BUDGET))

    assert seen["budget"] == BUDGET
    assert seen["pass1_cost"] > 0
    assert seen["n_recs"] == len(FIXTURE_PRODUCTS)


def test_insert_before_runs_in_order(engine):
    calls = []
    p = GreenfieldPipeline(engine)
    p.insert_before("pass2_depth", "marker_a", lambda ctx: calls.append("a"))
    p.insert_after("pass2_depth", "marker_b", lambda ctx: calls.append("b"))
    p.execute(_products(), budget=BUDGET)
    assert calls == ["a", "b"]
    assert p.pass_names.index("marker_a") < p.pass_names.index("pass2_depth")
    assert p.pass_names.index("marker_b") > p.pass_names.index("pass2_depth")


def test_skip_preprocess_rejected(engine):
    with pytest.raises(ValueError, match="preprocess"):
        GreenfieldPipeline(engine).skip("preprocess")


def test_unknown_pass_raises_keyerror(engine):
    with pytest.raises(KeyError, match="no_such_pass"):
        GreenfieldPipeline(engine).skip("no_such_pass")
