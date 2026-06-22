"""Tests for market-basket affinity (oasis/logic/basket_affinity.py)."""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.basket_affinity import (
    affinity_metrics, anchors_and_attachments, cooccurrence, link_edges,
)


class TestCooccurrence:
    def test_counts_pairs_and_items(self):
        txns = [["A", "B", "C"], ["A", "B"], ["A", "C"]]
        pairs, items, n = cooccurrence(txns)
        assert n == 3
        assert items["A"] == 3 and items["B"] == 2 and items["C"] == 2
        assert pairs[("A", "B")] == 2   # baskets 1,2
        assert pairs[("A", "C")] == 2   # baskets 1,3
        assert pairs[("B", "C")] == 1   # basket 1 only

    def test_dedupes_within_basket(self):
        # a repeated item in one bill counts once for that basket
        pairs, items, n = cooccurrence([["A", "A", "B"]])
        assert items["A"] == 1 and pairs[("A", "B")] == 1

    def test_min_item_count_prunes_rare(self):
        # C appears once; with min_item_count=2 it is dropped before pairing
        txns = [["A", "B", "C"], ["A", "B"], ["A", "B"]]
        pairs, _, _ = cooccurrence(txns, min_item_count=2)
        assert ("A", "C") not in pairs and ("B", "C") not in pairs
        assert pairs[("A", "B")] == 3


class TestAffinityMetrics:
    def test_lift_above_and_below_chance(self):
        # A&B always together (perfect association); A&D independent-ish
        txns = [["A", "B"]] * 8 + [["A", "D"], ["A", "D"], ["B"], ["D"]]
        pairs, items, n = cooccurrence(txns)
        m = {(d["a"], d["b"]): d for d in affinity_metrics(pairs, items, n, min_count=2)}
        ab = m[("A", "B")]
        assert ab["co_count"] == 8
        assert abs(ab["support"] - 8 / 12) < 1e-9
        assert abs(ab["conf_a_to_b"] - 8 / 10) < 1e-9   # P(B|A) = 8/10
        assert ab["lift"] > 1.0                          # genuine association

    def test_min_count_filters(self):
        txns = [["A", "B"], ["A", "C"]]   # each pair co-occurs once
        assert affinity_metrics(*cooccurrence(txns), min_count=2) == []


class TestLinkEdges:
    def test_bidirectional_weighted_and_lift_gated(self):
        txns = [["A", "B"]] * 5 + [["A", "C"]] * 5 + [["B"], ["C"]]
        metrics = affinity_metrics(*cooccurrence(txns), min_count=2)
        edges = link_edges(metrics, min_lift=1.0, min_count=2)
        # every kept pair yields two directed link edges with weight = co_count
        pairset = {(e["source"], e["target"]) for e in edges}
        assert ("A", "B") in pairset and ("B", "A") in pairset
        assert all(e["relation"] == "link" and e["weight"] >= 2 for e in edges)

    def test_high_min_lift_drops_everything(self):
        txns = [["A", "B"], ["A", "B"], ["A"], ["B"]]
        metrics = affinity_metrics(*cooccurrence(txns), min_count=2)
        assert link_edges(metrics, min_lift=99.0) == []


class TestAnchorsAndAttachments:
    def test_only_high_velocity_anchors(self):
        # MILK&COOKIES travel together (lift > 1); BREAD&EGGS are the rest
        txns = [["MILK", "COOKIES"]] * 6 + [["BREAD", "EGGS"]] * 4
        metrics = affinity_metrics(*cooccurrence(txns), min_count=2)
        # MILK is a high-velocity anchor; COOKIES is slow → not an anchor
        vel = {"MILK": 5.0, "COOKIES": 0.05, "BREAD": 0.0, "EGGS": 0.0}
        res = anchors_and_attachments(metrics, vel, min_anchor_ads=0.1, min_count=2)
        assert "MILK" in res and "COOKIES" not in res
        assert res["MILK"][0]["attachment"] == "COOKIES"
        assert res["MILK"][0]["co_count"] == 6
