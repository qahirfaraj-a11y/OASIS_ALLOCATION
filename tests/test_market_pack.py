"""The shipped competitive field, and the channel for correcting it.

WHY THESE EXIST

A retailer cannot assemble their own competitive field from memory. Measured by
dropping stores at random from the real national matrix, someone who knows 30%
of their rivals sees every candidate site overstated by 2.26x — and the bias
runs one way, toward opportunity. So OASIS ships the matrix.

That decision costs two obligations, and both are tested here:

  * the matrix is a DERIVATIVE DATABASE of OpenStreetMap, so it may only travel
    under ODbL with its notice attached. ``load_pack`` refuses a pack whose
    notice is missing, and the release whitelist ships the two together;
  * a shipped extract is wrong in ways a refresh will not fix — OSM carries no
    floor areas, misses branches, and keeps closed chains. Corrections
    therefore live in their own file and survive a re-fetch.
"""

import csv
import json
import os
import sys

import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import geo_sources as GS
from oasis.logic import release_packager as RP


def _root(tmp_path):
    (tmp_path / "oasis" / "data").mkdir(parents=True, exist_ok=True)
    return str(tmp_path)


def _write_pack(root, rows, licence=True, country="KEN"):
    path = GS.pack_path(country, root)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["Store_Name", "Latitude",
                                          "Longitude", "Chain", "Source"])
        w.writeheader()
        w.writerows(rows)
    if licence:
        with open(GS.pack_licence_path(country, root), "w",
                  encoding="utf-8") as f:
            f.write("ODbL 1.0\n" + GS.OSM_ATTRIBUTION + "\n")
    return path


_TWO = [{"Store_Name": "Rival A", "Latitude": -1.30, "Longitude": 36.80,
         "Chain": "Alpha", "Source": "OSM_Overpass"},
        {"Store_Name": "Rival B", "Latitude": -1.31, "Longitude": 36.81,
         "Chain": "Beta", "Source": "OSM_Overpass"}]


class TestTheShippedPack:
    def test_a_fresh_install_has_a_competitive_field(self, tmp_path):
        """The whole reason the pack exists: no fetch, still not empty.

        Before this, first run scored every candidate against ZERO rivals and
        reported it as opportunity rather than as a gap in the data.
        """
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        res = GS.load_competitors(root=root)
        assert res["error"] is None
        assert len(res["rows"]) == 2
        assert res["from_pack"] is True

    def test_a_pack_without_its_licence_is_refused(self, tmp_path):
        """ODbL 4.3. A notice lost in transit is exactly what it guards."""
        root = _root(tmp_path)
        _write_pack(root, _TWO, licence=False)
        res = GS.load_competitors(root=root)
        assert res["rows"] == []
        assert "licence" in res["error"].lower()

    def test_no_pack_and_no_fetch_says_so_rather_than_scoring_empty(
            self, tmp_path):
        res = GS.load_competitors(root=_root(tmp_path))
        assert res["rows"] == []
        assert "fetch" in res["error"].lower()

    def test_a_client_fetch_outranks_the_shipped_pack(self, tmp_path):
        """They refreshed their own region, so they meant it."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        with open(GS.cache_path(root), "w", encoding="utf-8",
                  newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n"
                    "Mine,-1.29,36.79,Gamma,OSM_Overpass\n")
        res = GS.load_competitors(root=root)
        assert [r["Chain"] for r in res["rows"]] == ["Gamma"]
        assert not res["from_pack"]

    def test_the_operators_own_banner_is_still_dropped_from_the_pack(
            self, tmp_path):
        """One national matrix serves every client, so exclusion is at load."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        GS.save_own_chain(["Alpha"], root=root)
        res = GS.load_competitors(root=root)
        assert [r["Chain"] for r in res["rows"]] == ["Beta"]
        assert res["own_excluded"] == 1


class TestCorrections:
    def test_a_missed_branch_can_be_added(self, tmp_path):
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        res = GS.correct_competitor("add", -1.32, 36.82, chain="Gamma",
                                    name="Gamma Ngong", root=root)
        assert res["saved"], res.get("error")
        chains = [r["Chain"] for r in GS.load_competitors(root=root)["rows"]]
        assert sorted(chains) == ["Alpha", "Beta", "Gamma"]

    def test_a_closed_store_can_be_removed(self, tmp_path):
        """OSM keeps chains that have stopped trading. A phantom rival
        suppresses a real site, so this is not cosmetic."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        assert GS.correct_competitor("remove", -1.30, 36.80, root=root)["saved"]
        chains = [r["Chain"] for r in GS.load_competitors(root=root)["rows"]]
        assert chains == ["Beta"]

    def test_a_branch_floor_area_beats_the_chain_default(self, tmp_path):
        """Huff pull is PROPORTIONAL to floor area. Correcting one branch and
        having the chain average overwrite it would be silently useless."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        GS.save_chain_profiles({"alpha": {"size_sqft": 8000.0, "pull": 1.0}},
                               root=root)
        assert GS.correct_competitor("edit", -1.30, 36.80, size_sqft=31_000,
                                     root=root)["saved"]
        rows = {r["Chain"]: r for r in GS.load_competitors(root=root)["rows"]}
        assert rows["Alpha"]["size_sqft"] == 31_000.0
        assert rows["Alpha"]["size_is_default"] is False

    def test_corrections_survive_a_refetch(self, tmp_path):
        """The point of a separate file. A client who spends an afternoon
        fixing their market must be able to press Update afterwards."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        GS.correct_competitor("add", -1.32, 36.82, chain="Gamma", root=root)
        GS.correct_competitor("remove", -1.30, 36.80, root=root)

        # a refresh replaces the EXTRACT wholesale
        with open(GS.cache_path(root), "w", encoding="utf-8",
                  newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n"
                    "Rival A,-1.30,36.80,Alpha,OSM_Overpass\n"
                    "Rival B,-1.31,36.81,Beta,OSM_Overpass\n")
        chains = sorted(r["Chain"] for r in
                        GS.load_competitors(root=root)["rows"])
        assert chains == ["Beta", "Gamma"]

    def test_removing_then_re_adding_the_same_point_leaves_one_store(
            self, tmp_path):
        """A stale 'removed' entry would delete the store the client just
        put back, and they would have no way to see why."""
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        GS.correct_competitor("remove", -1.30, 36.80, root=root)
        GS.correct_competitor("add", -1.30, 36.80, chain="Alpha", root=root)
        rows = GS.load_competitors(root=root)["rows"]
        assert sum(1 for r in rows if r["Chain"] == "Alpha") == 1

    def test_an_added_rival_needs_a_chain(self, tmp_path):
        res = GS.correct_competitor("add", -1.3, 36.8, root=_root(tmp_path))
        assert not res["saved"]

    @pytest.mark.parametrize("lat,lon", [(95.0, 36.8), (-1.3, 400.0)])
    def test_impossible_coordinates_are_rejected(self, lat, lon, tmp_path):
        res = GS.correct_competitor("add", lat, lon, chain="X",
                                    root=_root(tmp_path))
        assert not res["saved"]

    def test_an_unknown_action_is_rejected(self, tmp_path):
        res = GS.correct_competitor("drop", -1.3, 36.8, chain="X",
                                    root=_root(tmp_path))
        assert not res["saved"]

    def test_a_corrupt_overrides_file_does_not_take_the_field_down(
            self, tmp_path):
        root = _root(tmp_path)
        _write_pack(root, _TWO)
        with open(GS.overrides_path(root), "w", encoding="utf-8") as f:
            f.write("{not json")
        assert len(GS.load_competitors(root=root)["rows"]) == 2


class TestChainNaming:
    def test_a_format_sub_brand_is_not_a_second_chain(self):
        """A retailer's own sub-fascia counted as a separate competitor: two
        floor-area entries to fill in, two store counts, and the chain's real
        pull split across both."""
        brands = ["Vega", "Vega Foodplus"]
        assert GS.match_chain("Vega Foodplus Supermarket Riverside",
                              brands) == "Vega"
        assert GS.match_chain("Vega Ngong Road", brands) == "Vega"

    def test_a_chain_actually_called_foodplus_survives(self):
        """The rule strips a SUFFIX, so it must not eat the whole name."""
        assert GS.match_chain("Foodplus Ruaka", ["Foodplus"]) == "Foodplus"


class TestThePackShips:
    def test_the_matrix_and_its_licence_travel_together(self):
        """Whitelist is default-deny. Shipping the CSV without the notice
        would strip the ODbL obligation at packaging time — and would break
        every install, since load_pack refuses a pack without it."""
        rel = "oasis/data/market_packs/market_matrix_KEN.csv"
        lic = "oasis/data/market_packs/market_matrix_KEN.LICENCE.txt"
        assert RP.should_ship_clean(rel, 13_000)[0]
        assert RP.should_ship_clean(lic, 1_000)[0]

    def test_the_pack_directory_is_not_a_hole_for_arbitrary_payload(self):
        bad = "oasis/data/market_packs/pos_transactions.db"
        assert not RP.should_ship_clean(bad, 5_000)[0]
        huge = "oasis/data/market_packs/market_matrix_KEN.csv"
        assert not RP.should_ship_clean(huge, 50_000_000)[0]

    def test_the_rest_of_oasis_data_is_still_payload(self):
        assert not RP.should_ship_clean("oasis/data/competitor_network.csv",
                                        9_000)[0]
        assert not RP.should_ship_clean("oasis/data/own_chain.json", 100)[0]

    def test_client_state_never_ships(self):
        """Corrections are the CLIENT's, and a fetched extract is their own
        copy of public data. Neither belongs in anyone else's zip."""
        assert not RP.should_ship_clean(
            "oasis/data/" + GS.OVERRIDES_FILE, 400)[0]


class TestTheRealPack:
    """Guards on the artefact actually in the tree."""

    def _rows(self):
        path = GS.pack_path("KEN")
        if not os.path.exists(path):
            pytest.skip("no KEN pack built in this tree")
        with open(path, encoding="utf-8") as f:
            return list(csv.DictReader(f))

    def test_it_carries_its_licence(self):
        if not os.path.exists(GS.pack_path("KEN")):
            pytest.skip("no KEN pack built in this tree")
        assert os.path.exists(GS.pack_licence_path("KEN"))
        text = open(GS.pack_licence_path("KEN"), encoding="utf-8").read()
        assert "ODbL" in text and "OpenStreetMap" in text

    def test_every_row_has_a_usable_position_and_chain(self):
        for r in self._rows():
            assert -5.0 <= float(r["Latitude"]) <= 5.5
            assert 33.0 <= float(r["Longitude"]) <= 42.5
            assert r["Chain"].strip()

    def test_no_two_stores_of_one_chain_sit_on_top_of_each_other(self):
        """OSM maps a shop as a node AND a building often enough that the
        first build shipped one greengrocer twice. Two rows is two
        competitors to the scorer."""
        import math
        seen = {}
        for r in self._rows():
            lat, lon = float(r["Latitude"]), float(r["Longitude"])
            for la, lo in seen.setdefault(r["Chain"], []):
                d = math.hypot((lat - la) * 110_574,
                               (lon - lo) * 111_320 * math.cos(math.radians(lat)))
                assert d > 100.0, f"{r['Chain']} duplicated at {lat},{lon}"
            seen[r["Chain"]].append((lat, lon))

    def test_git_does_not_ignore_the_pack(self):
        """The release packager copies from the WORKING TREE, so a gitignored
        pack builds perfectly here and ships as nothing from a clean checkout.

        `*.csv` did exactly that. It is the same trap that had silently
        excluded every Odoo ir.model.access.csv, one directory over — and the
        failure is invisible by construction, because a site with no rivals
        scores as opportunity rather than as missing data.
        """
        import subprocess
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.isdir(os.path.join(repo, ".git")):
            pytest.skip("not a git checkout")
        for rel in ("oasis/data/market_packs/market_matrix_KEN.csv",
                    "oasis/data/market_packs/market_matrix_KEN.LICENCE.txt"):
            r = subprocess.run(["git", "check-ignore", "-q", rel], cwd=repo)
            assert r.returncode != 0, rel + " is gitignored — it cannot ship"

    def test_git_DOES_ignore_client_state(self):
        """Corrections and a fetched extract belong to one install."""
        import subprocess
        repo = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
        if not os.path.isdir(os.path.join(repo, ".git")):
            pytest.skip("not a git checkout")
        for rel in ("oasis/data/" + GS.OVERRIDES_FILE,
                    "oasis/data/" + GS.CACHE_FILE):
            r = subprocess.run(["git", "check-ignore", "-q", rel], cwd=repo)
            assert r.returncode == 0, rel + " would be committed"

    def test_place_names_did_not_become_chains(self):
        """A junk brand does not merely add rows — because match_chain
        resolves longest-name-wins, 'Greenspan' (a mall) outbid 'Naivas' and
        took a real Naivas branch with it."""
        chains = {r["Chain"].lower() for r in self._rows()}
        assert not chains & {"greenspan", "naivasha", "buffalo mall"}
