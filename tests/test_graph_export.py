"""Tests for the native product-graph export (pure builders)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.graph_export import (
    substitution_edges, structural_edges, build_graph, NODE_COLUMNS,
)


def _rec(id, dept, sup, price, vel=0.0, rank=999):
    return {"id": id, "department": dept, "supplier": sup, "price": price,
            "velocity_ads": vel, "sales_rank": rank}


class TestSubstitutionEdges:
    def test_only_within_department(self):
        recs = [_rec("A", "WINE", "S1", 100), _rec("B", "WINE", "S2", 105),
                _rec("C", "BEER", "S3", 102)]
        edges = substitution_edges(recs, k=5)
        # A<->B (same dept) only; never crosses into BEER
        for e in edges:
            assert e["relation"] == "substitution"
        targets_of_A = {e["target"] for e in edges if e["source"] == "A"}
        assert "B" in targets_of_A and "C" not in targets_of_A

    def test_topk_cap(self):
        recs = [_rec(f"P{i}", "D", "S", 100 + i) for i in range(10)]
        edges = substitution_edges(recs, k=3)
        per_source = {}
        for e in edges:
            per_source.setdefault(e["source"], 0)
            per_source[e["source"]] += 1
        assert all(c <= 3 for c in per_source.values())

    def test_weight_higher_for_closer(self):
        # P0 price 100; P1 price 101 (close); P2 price 200 (far)
        recs = [_rec("P0", "D", "S", 100), _rec("P1", "D", "S", 101),
                _rec("P2", "D", "S", 200)]
        edges = [e for e in substitution_edges(recs, k=5) if e["source"] == "P0"]
        w = {e["target"]: e["weight"] for e in edges}
        assert w["P1"] > w["P2"]

    def test_singleton_department_no_edges(self):
        assert substitution_edges([_rec("solo", "D", "S", 10)]) == []


class TestStructuralEdges:
    def test_supply_and_demand(self):
        edges = structural_edges([_rec("A", "WINE", "ACME", 100)])
        rels = {(e["relation"], e["target"]) for e in edges}
        assert ("upstream_supply", "ACME") in rels
        assert ("downstream_demand", "WINE") in rels

    def test_skips_blank_supplier(self):
        edges = structural_edges([_rec("A", "WINE", "", 100)])
        assert all(e["relation"] != "upstream_supply" for e in edges)


class TestBuildGraph:
    def test_node_types_and_columns(self):
        recs = [_rec("A", "WINE", "ACME", 100), _rec("B", "WINE", "ACME", 110),
                _rec("C", "BEER", "BREW", 90)]
        nodes, edges = build_graph(recs, k=5)
        types = {}
        for n in nodes:
            types[n["type"]] = types.get(n["type"], 0) + 1
            # every node row carries the full column set
            assert set(NODE_COLUMNS) <= set(n.keys())
        assert types["SKU"] == 3
        assert types["Supplier"] == 2   # ACME, BREW
        assert types["Department"] == 2  # WINE, BEER
        # edges include structural + substitution
        assert any(e["relation"] == "substitution" for e in edges)
        assert any(e["relation"] == "upstream_supply" for e in edges)
