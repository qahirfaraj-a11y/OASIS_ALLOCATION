"""Candidate generation: where a shop could actually go.

Site scoring was fed a regular lattice — every square kilometre treated as an
available place to trade from. Measured, that is not a small idealisation. The
131 sites real supermarket operators chose scored WORSE as candidates than
points drawn at random in proportion to population (median 24,018 against
57,304), and only 11% beat the random median.

It is not the clustering penalty: deleting each store's cluster-mates before
scoring barely moved it. It is that the points maximising reachable population
are the middle of dense residential blocks, where a supermarket cannot go.
Restricting candidates to places where commerce demonstrably exists recovers
about three quarters of that gap without changing the scoring at all.
"""

import csv
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import geo_sources as GS
from oasis.logic import site_candidates as SC


def _write_amenities(root, points):
    path = SC.amenity_path(root)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["latitude", "longitude", "kind"])
        w.writeheader()
        for la, lo, k in points:
            w.writerow({"latitude": la, "longitude": lo, "kind": k})
    return path


def _root(tmp_path):
    (tmp_path / "oasis" / "data").mkdir(parents=True, exist_ok=True)
    return str(tmp_path)


def _cluster(lat, lon, n, kind="discretionary", spread=0.0001):
    """n amenities packed into one place.

    ``spread`` is per-step in degrees; 0.0001 is about 11 m, so twenty of them
    span roughly 220 m — a mall or a high street, and comfortably inside the
    400 m clustering radius. An earlier version of this helper used 0.0008 and
    spread the same twenty shops over 1.8 km, which the clusterer correctly
    split into three; the fixture was wrong, not the code.
    """
    return [(lat + i * spread, lon, kind) for i in range(n)]


class TestClustering:
    def test_a_mall_is_one_candidate_not_twenty_shops(self):
        """The median amenity sits 41 m from its nearest neighbour, because a
        mall is twenty of them. Twenty candidates 40 m apart is not a
        shortlist."""
        pts = _cluster(-1.30, 36.80, 20)
        nodes = SC.commercial_nodes(pts, cluster_km=0.4, min_amenities=3)
        assert len(nodes) == 1
        assert nodes[0]["amenities"] == 20

    def test_separate_places_stay_separate(self):
        pts = _cluster(-1.30, 36.80, 6) + _cluster(-1.34, 36.86, 5)
        nodes = SC.commercial_nodes(pts, cluster_km=0.4, min_amenities=3)
        assert len(nodes) == 2

    def test_the_coordinate_is_a_real_amenity_not_a_centroid(self):
        """A centroid of shops around a roundabout lands in the roundabout, and
        the entire point of this module is to stop emitting coordinates that are
        not places."""
        ring = [(-1.30, 36.80, "staple"), (-1.30, 36.81, "staple"),
                (-1.295, 36.805, "staple"), (-1.305, 36.805, "staple")]
        nodes = SC.commercial_nodes(ring, cluster_km=2.0, min_amenities=3)
        assert len(nodes) == 1
        assert (nodes[0]["lat"], nodes[0]["lon"]) in {
            (round(a, 6), round(b, 6)) for a, b, _ in ring}

    def test_a_lone_shop_is_not_a_retail_site(self):
        pts = _cluster(-1.30, 36.80, 6) + [(-1.50, 36.60, "staple")]
        nodes = SC.commercial_nodes(pts, cluster_km=0.4, min_amenities=3)
        assert [n["amenities"] for n in nodes] == [6]

    def test_kinds_are_counted_separately(self):
        pts = _cluster(-1.30, 36.80, 4, "discretionary") + \
              _cluster(-1.3005, 36.80, 3, "staple")
        n = SC.commercial_nodes(pts, cluster_km=0.4, min_amenities=3)[0]
        assert n["amenities"] == 7
        assert n["discretionary"] == 4 and n["staple"] == 3

    def test_busiest_first(self):
        pts = _cluster(-1.30, 36.80, 4) + _cluster(-1.40, 36.90, 9)
        nodes = SC.commercial_nodes(pts, cluster_km=0.4, min_amenities=3)
        assert [n["amenities"] for n in nodes] == [9, 4]

    def test_no_amenities_is_not_a_crash(self):
        assert SC.commercial_nodes([]) == []


class TestCandidates:
    def test_no_amenity_file_says_what_to_do(self, tmp_path):
        r = SC.candidates(root=_root(tmp_path))
        assert r["candidates"] == []
        assert "Fetch your region" in r["error"]

    def test_it_returns_commercial_nodes(self, tmp_path):
        root = _root(tmp_path)
        _write_amenities(root, _cluster(-1.30, 36.80, 8))
        r = SC.candidates(root=root)
        assert r["error"] is None
        assert len(r["candidates"]) == 1
        assert r["amenities"] == 8

    def test_a_site_beside_your_own_branch_is_dropped(self, tmp_path):
        """Huff ranks a site 200 m from your own shop highly because it captures
        the same people. That is a relocation argument, not a new one."""
        root = _root(tmp_path)
        _write_amenities(root, _cluster(-1.30, 36.80, 8))
        r = SC.candidates(root=root, exclude=[{"lat": -1.3005, "lon": 36.80}],
                          min_distance_km=1.5)
        assert r["candidates"] == []
        assert r["too_close"] == 1

    def test_outside_the_fetched_competitor_region_is_dropped(self, tmp_path):
        """The population grid is national, so it happily covers a town far past
        where the competitor download stopped — and that town then scores as
        uncontested with zero rivals, because none were ever looked for. Four
        such sites reached a shortlist before this check existed."""
        root = _root(tmp_path)
        _write_amenities(root, _cluster(-1.30, 36.80, 8) +
                         _cluster(-1.30, 37.60, 8))
        with open(GS.coverage_path(root), "w", encoding="utf-8") as f:
            json.dump({"south": -1.5, "west": 36.5,
                       "north": -1.0, "east": 37.0}, f)
        r = SC.candidates(root=root, catchment_km=10.0)
        assert r["outside_competitor_region"] == 1
        assert [c["lon"] for c in r["candidates"]] == [36.8]

    def test_an_unrecorded_region_does_not_silently_exclude(self, tmp_path):
        """No coverage file means an older install. The honest answer is
        'unknown', not a guess in either direction — so nothing is dropped."""
        root = _root(tmp_path)
        _write_amenities(root, _cluster(-1.30, 36.80, 8))
        assert GS.load_coverage(root) is None
        r = SC.candidates(root=root)
        assert len(r["candidates"]) == 1
        assert r["outside_competitor_region"] == 0


class TestCoverageRecord:
    """"No rivals here" is a finding inside the fetched box and an absence of
    data outside it, and nothing recorded which was which."""

    def test_a_point_inside_is_covered(self, tmp_path):
        root = _root(tmp_path)
        with open(GS.coverage_path(root), "w", encoding="utf-8") as f:
            json.dump({"south": -2.0, "west": 36.0,
                       "north": -1.0, "east": 37.0}, f)
        assert GS.covers_competitors(-1.5, 36.5, 10.0, root) is True

    def test_a_point_near_the_edge_is_not(self, tmp_path):
        root = _root(tmp_path)
        with open(GS.coverage_path(root), "w", encoding="utf-8") as f:
            json.dump({"south": -2.0, "west": 36.0,
                       "north": -1.0, "east": 37.0}, f)
        assert GS.covers_competitors(-1.02, 36.5, 10.0, root) is False
        assert GS.covers_competitors(-1.5, 36.99, 10.0, root) is False

    def test_unrecorded_is_none_not_false(self, tmp_path):
        assert GS.covers_competitors(-1.5, 36.5, 10.0, _root(tmp_path)) is None

    def test_a_corrupt_record_is_not_a_crash(self, tmp_path):
        root = _root(tmp_path)
        with open(GS.coverage_path(root), "w", encoding="utf-8") as f:
            f.write("{not json")
        assert GS.load_coverage(root) is None


class TestThePartialFetchGuard:
    """A partial fetch must never replace a complete field. The first version
    of this guard refused only when EVERY band failed; a re-fetch where most
    bands were throttled returned five stores, passed that test, and overwrote
    a complete 133-store field — gitignored client state, with no copy to
    restore from."""

    def test_a_shrinking_partial_fetch_is_refused(self, tmp_path, monkeypatch):
        root = _root(tmp_path)
        with open(GS.cache_path(root), "w", encoding="utf-8", newline="") as f:
            w = csv.DictWriter(f, fieldnames=["Store_Name", "Latitude",
                                              "Longitude", "Chain", "Source"])
            w.writeheader()
            for i in range(40):
                w.writerow({"Store_Name": f"Naivas {i}", "Latitude": -1.3,
                            "Longitude": 36.8 + i * 1e-4, "Chain": "Naivas",
                            "Source": "OSM_Overpass"})

        calls = {"n": 0}

        class Resp:
            def __init__(self, code, js):
                self.status_code, self._js = code, js

            def raise_for_status(self):
                if self.status_code >= 400:
                    raise RuntimeError(f"HTTP {self.status_code}")

            def json(self):
                return self._js

        def fake_get(url, params=None, headers=None, timeout=None):
            calls["n"] += 1
            if calls["n"] <= 2:
                return Resp(200, {"elements": [{
                    "lat": -1.28, "lon": 36.81,
                    "tags": {"name": "Naivas One", "shop": "supermarket"}}]})
            return Resp(429, {})

        import requests
        monkeypatch.setattr(requests, "get", fake_get)
        monkeypatch.setattr(GS, "_BAND_BACKOFF_S", 0.0)
        monkeypatch.setattr(GS, "_BAND_PAUSE_S", 0.0)

        res = GS.fetch_competitors(["Naivas"], "-1.95,36.10,-0.70,37.70",
                                   root=root)
        assert res["written"] == 0
        assert res["kept_existing"] == 40
        assert "would have replaced" in res["error"]
        # and the field on disk is untouched
        with open(GS.cache_path(root), encoding="utf-8") as f:
            assert sum(1 for _ in csv.DictReader(f)) == 40
