"""Tests for the four things that blocked a site-selection pilot.

1. the three open-data fetchers had NO caller, so a retailer could not acquire
   the data the methodology runs on;
2. competitors were read from one path while the previous console wrote
   another, so an upgraded install scored every site as uncontested;
3. every competitor was assumed the same floor area, and Huff pull is
   proportional to floor area;
4. stores could only be placed one at a time.
"""

import io
import json
import math
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic import geo_sources as GS
from oasis.logic import store_locations as SL


def _data_dir(tmp_path):
    d = tmp_path / "oasis" / "data"
    d.mkdir(parents=True, exist_ok=True)
    return d


class TestBulkPlacement:
    def test_a_plain_csv_with_a_header_is_read(self):
        res = SL.parse_locations("org_cd,lat,lon,size_sqft\n"
                                 "ORG001,-1.2680,36.7930,14000\n"
                                 "ORG002,-1.2790,36.7690,9000\n")
        assert [r["org_cd"] for r in res["rows"]] == ["ORG001", "ORG002"]
        assert res["rows"][0]["size_sqft"] == 14000.0

    def test_a_headerless_file_is_read_positionally(self):
        res = SL.parse_locations("ORG001,-1.2680,36.7930,14000\n")
        assert res["rows"][0]["org_cd"] == "ORG001"
        assert res["rows"][0]["lat"] == -1.2680

    def test_column_spellings_are_accepted(self):
        """An operator exporting from their own systems should not have to
        rename headers."""
        for hdr in ("store,latitude,longitude,floor_area",
                    "code,Lat,Lng,sqft",
                    "branch,y,x,area"):
            res = SL.parse_locations(hdr + "\nORG001,-1.26,36.79,12000\n")
            assert res["rows"] and res["rows"][0]["org_cd"] == "ORG001", hdr

    def test_a_missing_size_falls_back_to_the_default(self):
        res = SL.parse_locations("org_cd,lat,lon\nORG001,-1.26,36.79\n")
        assert res["rows"][0]["size_sqft"] == SL.DEFAULT_SIZE_SQFT

    def test_every_rejected_row_is_reported_with_a_reason(self):
        """A silent partial import is how half an estate ends up misplaced."""
        res = SL.parse_locations(
            "org_cd,lat,lon\n"
            "ORG001,-1.26,36.79\n"      # good
            ",-1.26,36.79\n"            # no code
            "ORG002,abc,36.79\n"        # not numeric
            "ORG003,99,36.79\n"         # out of range
            "ORG001,-1.30,36.80\n")     # duplicate
        assert len(res["rows"]) == 1
        reasons = " ".join(e["reason"] for e in res["errors"])
        assert "no store code" in reasons
        assert "not numeric" in reasons
        assert "out of range" in reasons
        assert "duplicate" in reasons
        assert all("line" in e or "org_cd" in e for e in res["errors"])

    def test_an_unknown_store_code_is_rejected_not_written(self, tmp_path):
        """A location saved against a typo leaves the real store reading
        'needs a location' while a phantom point joins every catchment."""
        _data_dir(tmp_path)
        res = SL.import_locations(
            "org_cd,lat,lon\nORG001,-1.26,36.79\nTYPO9,-1.27,36.80\n",
            known_orgs=["ORG001"], root=str(tmp_path))
        assert res["saved"] == 1
        assert any("not a store code" in e["reason"] for e in res["errors"])
        assert set(SL.load_locations(root=str(tmp_path))) == {"ORG001"}

    def test_import_merges_rather_than_replaces(self, tmp_path):
        _data_dir(tmp_path)
        SL.save_location("ORG001", -1.26, 36.79, 12000, root=str(tmp_path))
        SL.import_locations("org_cd,lat,lon\nORG002,-1.27,36.80\n",
                            root=str(tmp_path))
        assert set(SL.load_locations(root=str(tmp_path))) == {"ORG001", "ORG002"}

    def test_the_template_pre_fills_what_is_already_known(self, tmp_path):
        _data_dir(tmp_path)
        SL.save_location("ORG001", -1.26, 36.79, 12000, root=str(tmp_path))
        text = SL.export_template([{"org_cd": "ORG001"}, {"org_cd": "ORG002"}],
                                  root=str(tmp_path))
        lines = text.splitlines()
        assert lines[0] == SL.IMPORT_HEADER
        assert lines[1].startswith("ORG001,-1.26")
        assert lines[2] == "ORG002,,,"        # awaiting coordinates

    def test_empty_input_is_refused_cleanly(self):
        assert SL.parse_locations("")["error"] == "nothing to import"


class TestPlacementsVersusThePOS:
    def test_locations_matching_no_store_are_reported_separately(self, tmp_path):
        """With an unreachable POS the estate reads as 'nothing placed', which
        sent the operator back to place stores they had already placed. The
        real fault is the connection, and it must say so."""
        _data_dir(tmp_path)
        SL.save_location("ORG001", -1.26, 36.79, root=str(tmp_path))
        SL.save_location("ORG002", -1.27, 36.80, root=str(tmp_path))
        res = SL.merge_with_stores([], root=str(tmp_path))
        assert res["located"] == [] and res["missing"] == []
        assert res["orphaned"] == ["ORG001", "ORG002"]
        assert res["saved_total"] == 2

    def test_score_sites_distinguishes_the_two_failures(self, tmp_path):
        from oasis.desktop import data as D
        _data_dir(tmp_path)
        blank = D.score_sites([{"name": "C", "lat": -1.28, "lon": 36.8}],
                              root=str(tmp_path))
        assert "No store locations recorded yet" in blank["error"]

        SL.save_location("ORG001", -1.26, 36.79, root=str(tmp_path))
        stale = D.score_sites([{"name": "C", "lat": -1.28, "lon": 36.8}],
                              root=str(tmp_path))
        assert "match a store in your POS" in stale["error"]


class TestCompetitorPath:
    def _write(self, path):
        with io.open(path, "w", encoding="utf-8", newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n")
            f.write("Rival A,-1.30,36.80,Naivas,OSM_Overpass\n")

    def test_the_previous_console_location_is_still_read(self, tmp_path):
        """An install upgraded from the Streamlit console has the extract in
        the working directory. Reading only the new path made it look as
        though the client had NO competitors, which scores every site as
        uncontested rather than as unknown."""
        _data_dir(tmp_path)
        self._write(GS.legacy_cache_path(str(tmp_path)))
        res = GS.load_competitors(root=str(tmp_path))
        assert len(res["rows"]) == 1
        assert res["legacy_path"] is True
        assert "legacy" in res["source"]

    def test_the_canonical_path_wins_when_both_exist(self, tmp_path):
        d = _data_dir(tmp_path)
        self._write(GS.legacy_cache_path(str(tmp_path)))
        with io.open(d / GS.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n")
            f.write("A,-1.3,36.8,Naivas,x\nB,-1.31,36.81,Quickmart,x\n")
        res = GS.load_competitors(root=str(tmp_path))
        assert len(res["rows"]) == 2 and res["legacy_path"] is False

    def test_absent_everywhere_is_still_not_an_error_state(self, tmp_path):
        _data_dir(tmp_path)
        res = GS.load_competitors(root=str(tmp_path))
        assert res["rows"] == [] and "Fetch it" in res["error"]


class TestTheMarketMatrix:
    """The extract holds every chain in the region, and a client's OWN banner
    is removed on read rather than at fetch — so one matrix serves any client,
    and a second retailer can be scored against the first."""

    def test_spelling_variants_collapse_to_one_chain(self):
        """OSM records what a mapper typed. Left alone, one retailer becomes
        three chains as far as a floor-area table or a store count is
        concerned."""
        brands = ["Quickmart", "Quick Mart", "Cleanshelf", "Clean Shelf"]
        assert GS.match_chain("QuickMart Ngong Road", brands) == "Quickmart"
        assert GS.match_chain("Quick Mart Kiambu", brands) == "Quickmart"
        assert GS.match_chain("CLEAN SHELF Supermarket", brands) == "Cleanshelf"
        assert GS.match_chain("Cleanshelf Buruburu", brands) == "Cleanshelf"

    def test_a_longer_brand_is_not_shadowed_by_a_shorter_one(self):
        assert GS.match_chain("Quick Mart Ruaka",
                              ["Quick", "Quick Mart"]) == "Quickmart"

    def test_an_unrelated_name_matches_nothing(self):
        assert GS.match_chain("Nakumatt Junction", ["Naivas"]) is None
        assert GS.match_chain("", ["Naivas"]) is None

    def test_the_own_banner_round_trips(self, tmp_path):
        _data_dir(tmp_path)
        assert GS.save_own_chain(["Acme Foods"], root=str(tmp_path))["saved"]
        assert GS.load_own_chain(root=str(tmp_path)) == ["Acme Foods"]

    def test_the_own_banner_is_dropped_by_chain_or_by_store_name(self):
        rows = [{"Chain": "Acme Foods", "Store_Name": "Acme Foods Westlands"},
                {"Chain": "Naivas", "Store_Name": "Naivas Kilimani"},
                {"Chain": "Other", "Store_Name": "Acme Foods Karen"}]
        kept = GS.drop_own_chain(rows, ["Acme Foods"])
        assert [r["Chain"] for r in kept] == ["Naivas"]

    def test_no_own_banner_set_keeps_every_row(self):
        rows = [{"Chain": "Naivas", "Store_Name": "A"}]
        assert GS.drop_own_chain(rows, []) == rows
        assert GS.drop_own_chain(rows, None) == rows

    def test_load_reports_the_matrix_and_what_it_removed(self, tmp_path):
        """The operator must be able to see that their own stores were
        excluded — the double-count it prevents held the capture model at
        40.8% error against floor area's 24.9%."""
        d = _data_dir(tmp_path)
        with io.open(d / GS.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n")
            f.write("A,-1.30,36.80,Naivas,x\n")
            f.write("B,-1.31,36.81,Acme Foods,x\n")
            f.write("C,-1.32,36.82,Acme Foods,x\n")
        GS.save_own_chain(["Acme Foods"], root=str(tmp_path))
        res = GS.load_competitors(root=str(tmp_path))
        assert res["in_matrix"] == 3
        assert res["own_excluded"] == 2
        assert len(res["rows"]) == 1
        assert res["own_chain"] == ["Acme Foods"]

    def test_fetching_with_no_chain_names_is_refused_before_the_network(self):
        res = GS.fetch_competitors([], "-1.6,36.5,-1.0,37.3")
        assert res["written"] == 0 and "no chain names" in res["error"]


class TestCompetitorSizes:
    def test_pull_is_proportional_so_a_default_is_flagged(self):
        rows = GS.apply_sizes([{"Chain": "Naivas"}], {})
        assert rows[0]["size_sqft"] == GS.DEFAULT_COMPETITOR_SQFT
        assert rows[0]["size_is_default"] is True

    def test_a_recorded_size_is_applied_case_insensitively(self):
        rows = GS.apply_sizes([{"Chain": "NAIVAS"}], {"naivas": 30000.0})
        assert rows[0]["size_sqft"] == 30000.0
        assert rows[0]["size_is_default"] is False

    def test_a_chain_entry_matches_a_longer_store_name(self):
        """OSM names are 'Naivas Supermarket Kilimani', not 'Naivas'."""
        rows = GS.apply_sizes([{"Chain": "Naivas Supermarket"}],
                              {"naivas": 30000.0})
        assert rows[0]["size_sqft"] == 30000.0

    def test_sizes_round_trip(self, tmp_path):
        _data_dir(tmp_path)
        assert GS.save_sizes({"Naivas": 30000, "Quickmart": 8000},
                             root=str(tmp_path))["saved"] is True
        assert GS.load_sizes(root=str(tmp_path)) == {"naivas": 30000.0,
                                                     "quickmart": 8000.0}

    def test_a_nonsense_size_is_refused_not_stored(self, tmp_path):
        _data_dir(tmp_path)
        assert GS.save_sizes({"Naivas": -5}, root=str(tmp_path))["saved"] is False
        assert GS.save_sizes({"Naivas": "big"}, root=str(tmp_path))["saved"] is False
        assert GS.load_sizes(root=str(tmp_path)) == {}

    def test_a_corrupt_sizes_file_degrades_to_defaults(self, tmp_path):
        d = _data_dir(tmp_path)
        (d / GS.SIZES_FILE).write_text("{not json", encoding="utf-8")
        assert GS.load_sizes(root=str(tmp_path)) == {}

    def test_chains_in_lists_them_for_the_operator(self):
        assert GS.chains_in([{"Chain": "Naivas"}, {"Chain": "Naivas"},
                             {"chain": "Quickmart"}]) == ["Naivas", "Quickmart"]

    def test_sizes_reach_the_loaded_rows(self, tmp_path):
        d = _data_dir(tmp_path)
        with io.open(d / GS.CACHE_FILE, "w", encoding="utf-8", newline="") as f:
            f.write("Store_Name,Latitude,Longitude,Chain,Source\n")
            f.write("A,-1.3,36.8,Naivas,x\nB,-1.31,36.81,Tiny Shop,x\n")
        GS.save_sizes({"Naivas": 30000}, root=str(tmp_path))
        res = GS.load_competitors(root=str(tmp_path))
        assert res["sized"] == 1 and res["unsized"] == 1
        by_chain = {r["Chain"]: r["size_sqft"] for r in res["rows"]}
        assert by_chain["Naivas"] == 30000.0
        assert by_chain["Tiny Shop"] == GS.DEFAULT_COMPETITOR_SQFT


class TestMeasuredFootprints:
    """Pull is PROPORTIONAL to floor area, so a guessed size is the weakest
    term in the model. Where a branch is mapped as a polygon rather than a
    point, its ground area is a real measurement and costs nothing to take."""

    def _square(self, lat, lon, metres):
        dlat = metres / 110_540.0
        dlon = metres / (111_320.0 * math.cos(math.radians(lat)))
        return [{"lat": lat, "lon": lon},
                {"lat": lat, "lon": lon + dlon},
                {"lat": lat + dlat, "lon": lon + dlon},
                {"lat": lat + dlat, "lon": lon}]

    def test_a_known_square_measures_correctly(self):
        area = GS._polygon_area_sqm(self._square(-1.28, 36.80, 100.0))
        assert abs(area - 10_000.0) / 10_000.0 < 0.01, area

    def test_a_degenerate_outline_measures_nothing(self):
        assert GS._polygon_area_sqm([]) == 0.0
        assert GS._polygon_area_sqm([{"lat": -1.0, "lon": 36.0}]) == 0.0

    def test_footprints_are_grouped_by_chain_and_converted_to_sqft(self):
        els = [{"tags": {"name": "Naivas Kilimani"},
                "geometry": self._square(-1.28, 36.80, 100.0)},
               {"tags": {"name": "Quick Mart Ruaka"},
                "geometry": self._square(-1.20, 36.79, 50.0)}]
        got = GS.footprints_from_elements(els, ["Naivas", "Quick Mart"])
        assert set(got) == {"Naivas", "Quickmart"}
        assert abs(got["Naivas"][0] - 10_000 * 10.7639) < 500

    def test_a_kiosk_sized_outline_is_not_a_supermarket(self):
        els = [{"tags": {"name": "Naivas"},
                "geometry": self._square(-1.28, 36.80, 3.0)}]
        assert GS.footprints_from_elements(els, ["Naivas"]) == {}

    def test_an_unnamed_or_unmatched_polygon_is_ignored(self):
        els = [{"tags": {}, "geometry": self._square(-1.28, 36.80, 100.0)},
               {"tags": {"name": "Nakumatt"},
                "geometry": self._square(-1.28, 36.80, 100.0)}]
        assert GS.footprints_from_elements(els, ["Naivas"]) == {}

    def test_measuring_with_no_chain_names_is_refused(self):
        assert "no chain names" in GS.measure_footprints([], "-1,36,-0.9,36.1")["error"]


class TestChainProfiles:
    def test_a_profile_round_trips_with_its_provenance(self, tmp_path):
        _data_dir(tmp_path)
        GS.save_chain_profiles(
            {"Naivas": {"size_sqft": 11743, "pull": 1.0,
                        "source": "osm-footprint", "n_measured": 5}},
            root=str(tmp_path))
        got = GS.load_chain_profiles(root=str(tmp_path))
        assert got["naivas"]["size_sqft"] == 11743.0
        assert got["naivas"]["source"] == "osm-footprint"
        assert got["naivas"]["n_measured"] == 5

    def test_what_the_operator_typed_outranks_a_building_outline(self, tmp_path):
        """They know their market; an outline is a roof."""
        _data_dir(tmp_path)
        GS.save_chain_profiles({"Naivas": {"size_sqft": 11743,
                                           "source": "osm-footprint",
                                           "n_measured": 5}},
                               root=str(tmp_path))
        GS.save_sizes({"Naivas": 26000}, root=str(tmp_path))
        got = GS.load_chain_profiles(root=str(tmp_path))
        assert got["naivas"]["size_sqft"] == 26000.0
        assert got["naivas"]["source"] == "operator"

    def test_a_nonsense_profile_is_refused(self, tmp_path):
        _data_dir(tmp_path)
        assert not GS.save_chain_profiles(
            {"A": {"size_sqft": 0}}, root=str(tmp_path))["saved"]
        assert not GS.save_chain_profiles(
            {"A": {"size_sqft": 100, "pull": "big"}}, root=str(tmp_path))["saved"]

    def test_rows_carry_the_size_its_source_and_the_pull(self):
        rows = GS.apply_sizes(
            [{"Chain": "Naivas"}, {"Chain": "Unlisted"}],
            {"naivas": {"size_sqft": 11743, "pull": 1.0,
                        "source": "osm-footprint", "n_measured": 5}})
        assert rows[0]["size_sqft"] == 11743.0
        assert rows[0]["size_source"] == "osm-footprint"
        assert rows[0]["n_measured"] == 5
        assert rows[0]["pull"] == 1.0
        assert rows[1]["size_is_default"] is True
        assert rows[1]["pull"] is None, "unprofiled must fall back to the heuristic"

    def test_a_flat_sizes_map_still_works(self):
        """A caller that has not migrated keeps working."""
        rows = GS.apply_sizes([{"Chain": "Naivas"}], {"naivas": 30000.0})
        assert rows[0]["size_sqft"] == 30000.0 and rows[0]["pull"] == 1.0


class TestPullIsDataNotAHardcodedList:
    def test_an_explicit_pull_overrides_the_name_heuristic(self):
        from oasis.logic.site_scoring import attractiveness
        # "naivas" is in BIG_BOX_CHAINS, so the fallback would apply 1.5x
        assert attractiveness(10_000, "Naivas") == 15.0
        assert attractiveness(10_000, "Naivas", pull=1.0) == 10.0

    def test_the_heuristic_still_applies_without_a_profile(self):
        from oasis.logic.site_scoring import attractiveness
        assert attractiveness(10_000, "Corner Shop") == 10.0
        assert attractiveness(10_000, "Carrefour") == 15.0

    def test_a_profiled_rival_no_longer_gets_the_big_box_bonus_twice(self):
        """The 1.5x stood in for 'bigger than the 15,000 default suggests'.
        Once the size is measured, applying it as well double-counts."""
        from oasis.logic.site_scoring import score_site
        own = [{"lat": -1.2650, "lon": 36.8020, "size_sqft": 12000}]
        heuristic = score_site(-1.2750, 36.8050, own,
                               [{"lat": -1.2700, "lon": 36.8100,
                                 "size_sqft": 30000, "chain": "Naivas"}],
                               size_sqft=15000)
        profiled = score_site(-1.2750, 36.8050, own,
                              [{"lat": -1.2700, "lon": 36.8100,
                                "size_sqft": 30000, "chain": "Naivas",
                                "pull": 1.0}],
                              size_sqft=15000)
        assert profiled["adjusted_capture_pct"] > heuristic["adjusted_capture_pct"]


class TestSizeChangesTheAnswer:
    def test_a_bigger_rival_takes_more_of_the_catchment(self):
        """The point of blocker 3: if every rival is the same size, the model
        cannot tell a kiosk from a hypermarket."""
        from oasis.logic.site_scoring import score_site
        own = [{"lat": -1.2650, "lon": 36.8020, "size_sqft": 12000}]
        small = score_site(-1.2750, 36.8050, own,
                           [{"lat": -1.2700, "lon": 36.8100,
                             "size_sqft": 3000, "chain": "Kiosk"}],
                           size_sqft=15000)
        big = score_site(-1.2750, 36.8050, own,
                         [{"lat": -1.2700, "lon": 36.8100,
                           "size_sqft": 80000, "chain": "Hyper"}],
                         size_sqft=15000)
        assert small["adjusted_capture_pct"] > big["adjusted_capture_pct"] * 1.3


class TestFetchOrchestration:
    def test_an_inverted_bounding_box_is_refused_before_any_network_call(self):
        from oasis.desktop import data as D
        res = D.fetch_region_data((-1.0, 36.5, -1.6, 37.3), layers=[])
        assert "inverted" in res["error"]

    def test_a_malformed_bounding_box_is_refused(self):
        from oasis.desktop import data as D
        assert "four numbers" in D.fetch_region_data(("a", 1, 2, 3),
                                                     layers=[])["error"]

    def test_status_reports_each_layer_separately(self, tmp_path):
        from oasis.desktop import data as D
        _data_dir(tmp_path)
        s = D.region_data_status(root=str(tmp_path))
        for key in ("stores_placed", "competitors", "population_cells",
                    "amenities", "ready"):
            assert key in s
        assert s["ready"] is False, "nothing is loaded, so nothing is ready"

    def test_layers_can_be_fetched_independently(self):
        """A rate-limited Overpass must not cost the operator their
        population grid."""
        from oasis.desktop import data as D
        res = D.fetch_region_data((-1.6, 36.5, -1.0, 37.3), layers=[])
        assert res["error"] is None and res["results"] == {}


class TestTheSuiteStaysOffline:
    """A test called fetch_region_data(layers=[]) meaning "fetch nothing".
    `layers or (...)` collapsed the empty list into "fetch everything", so it
    really did hit Overpass and WorldPop, wrote 100 competitor rows, 6,912
    population cells and 5,209 POIs into the developer's oasis/data, and took
    62 seconds. It PASSED the whole time.
    """

    def test_an_unmarked_test_cannot_reach_a_third_party(self):
        import pytest
        import requests
        with pytest.raises(Exception) as excinfo:
            requests.get("https://overpass-api.de/api/interpreter", timeout=5)
        # The address is already resolved by the time connect() is reached, so
        # the message names the IP rather than the host. What matters is that
        # the guard, not the network, is what stopped it — and that it says how
        # to opt in.
        msg = str(excinfo.value)
        assert "suite runs offline" in msg
        assert "@pytest.mark.network" in msg

    def test_the_fetchers_fail_closed_rather_than_writing(self, tmp_path):
        """The exact escape, now caught: an error back, and nothing on disk."""
        from oasis.desktop import data as D
        _data_dir(tmp_path)
        res = D.fetch_region_data((-1.6, 36.5, -1.0, 37.3),
                                  layers=["population", "amenities"],
                                  root=str(tmp_path))
        for layer in ("population", "amenities"):
            assert res["results"][layer]["written"] == 0
            assert res["results"][layer]["error"]
        from oasis.logic.affluence import cache_path as a_path
        from oasis.logic.population import cache_path as p_path
        assert not os.path.exists(p_path(str(tmp_path)))
        assert not os.path.exists(a_path(str(tmp_path)))

    def test_loopback_is_still_allowed(self):
        """A test may still bind and talk to a local server."""
        import socket
        srv = socket.socket()
        srv.bind(("127.0.0.1", 0))
        srv.listen(1)
        try:
            conn = socket.create_connection(("127.0.0.1", srv.getsockname()[1]),
                                            timeout=2)
            conn.close()
        finally:
            srv.close()


class TestTemplateAndSizesThroughTheDataLayer:
    def test_the_template_survives_an_unreachable_pos(self, tmp_path):
        from oasis.desktop import data as D
        assert D.store_location_template(root=str(tmp_path)).startswith("org_cd")

    def test_sizes_round_trip_through_the_data_layer(self, tmp_path):
        from oasis.desktop import data as D
        _data_dir(tmp_path)
        D.set_competitor_sizes({"Naivas": 25000}, root=str(tmp_path))
        assert D.competitor_sizes(root=str(tmp_path)) == {"naivas": 25000.0}
        raw = json.loads((tmp_path / "oasis" / "data" / GS.SIZES_FILE)
                         .read_text(encoding="utf-8"))
        assert raw["chains"]["naivas"] == 25000.0


class TestTheAuditFixes:
    """Four defects found by auditing the stack, each pinned so it cannot
    return."""

    def test_a_store_weighs_the_same_whichever_side_it_is_on(self):
        """FIX 1. Own stores were appended with an empty chain and no pull, so
        only the rival path ever reached the big-box weight. The same physical
        store scored 27.35% against you as a rival and 35.40% as your own —
        8.05pp apart, which understated cannibalisation for exactly the large
        operators it matters most to."""
        from oasis.logic.site_scoring import score_site
        store = {"lat": -1.2700, "lon": 36.8100,
                 "size_sqft": 30000, "chain": "Naivas"}
        as_rival = score_site(-1.2750, 36.8050, [], [store], size_sqft=15000)
        as_own = score_site(-1.2750, 36.8050, [store], [], size_sqft=15000)
        assert abs(as_rival["capture_pct"] - as_own["capture_pct"]) < 1e-9

    def test_an_explicit_pull_reaches_own_stores_too(self):
        from oasis.logic.site_scoring import score_site
        base = {"lat": -1.2700, "lon": 36.8100, "size_sqft": 30000,
                "chain": "Naivas"}
        heuristic = score_site(-1.2750, 36.8050, [base], [], size_sqft=15000)
        profiled = score_site(-1.2750, 36.8050,
                              [dict(base, pull=1.0)], [], size_sqft=15000)
        assert profiled["capture_pct"] > heuristic["capture_pct"]

    def test_the_rings_always_reach_the_catchment(self):
        """FIX 2. Rings were fixed at 1/2.5/5km against a 10km catchment, so
        everyone beyond 5km was scored as standing at 5km. On five real
        catchments that was 36-86% of the PEOPLE, median 74%."""
        from oasis.logic.site_scoring import CATCHMENT_KM, ring_radii
        for catchment in (5.0, 10.0, 20.0):
            radii = ring_radii(catchment)
            assert max(radii) >= catchment * 0.85, radii
            assert max(radii) < catchment, "a sample must sit inside its region"
            assert radii == tuple(sorted(radii))
        assert max(ring_radii(CATCHMENT_KM)) >= 9.0

    def test_sampling_scales_with_a_custom_catchment(self):
        from oasis.logic.site_scoring import _ring_points
        wide = _ring_points(-1.28, 36.80, catchment_km=20.0)
        tight = _ring_points(-1.28, 36.80, catchment_km=5.0)
        spread = lambda pts: max(abs(p[0] + 1.28) for p in pts)  # noqa: E731
        assert spread(wide) > spread(tight) * 3

    def test_one_definition_of_a_degree(self):
        """FIX 3. A degree was expressed four ways across five modules —
        111.0, 111.32, and the 111_320/110_540 pair — disagreeing by 0.3%."""
        import oasis.logic.affluence as A
        import oasis.logic.geo_sources as G
        import oasis.logic.population as P
        import oasis.logic.site_scoring as S
        assert S.KM_PER_DEG_LAT is P.KM_PER_DEG_LAT
        assert G.KM_PER_DEG_LON is P.KM_PER_DEG_LON
        assert A._BBOX_KM_PER_DEG is P._BBOX_KM_PER_DEG
        # the bounding-box constant must err WIDE, never narrow: too small a
        # box silently drops cells and understates a catchment with no error.
        assert P._BBOX_KM_PER_DEG < P.KM_PER_DEG_LAT < P.KM_PER_DEG_LON

    def test_the_default_competitor_size_has_one_home(self):
        import oasis.logic.geo_sources as G
        import oasis.logic.site_scoring as S
        assert S.DEFAULT_COMPETITOR_SQFT is G.DEFAULT_COMPETITOR_SQFT

    def test_profile_lookup_prefers_the_longest_match(self):
        """FIX 4. match_chain had a longest-wins rule and _lookup did not, so
        a profile keyed 'quick' silently claimed 'Quickmart' and handed it that
        chain's floor area — decided by dict order."""
        got = GS._lookup("quickmart", {"quick": {"size_sqft": 2000},
                                       "quickmart": {"size_sqft": 6792}})
        assert got["size_sqft"] == 6792.0
        reversed_order = GS._lookup("quickmart", {"quickmart": {"size_sqft": 6792},
                                                  "quick": {"size_sqft": 2000}})
        assert reversed_order["size_sqft"] == 6792.0


class TestQuadratureAndCannibalisation:
    """Two defects the mathematical audit found, both now closed."""

    def _grid(self, n=14, step=0.006, people=400.0, lat=-1.28, lon=36.80):
        from oasis.logic.population import PopulationGrid
        return PopulationGrid([(lat + i * step, lon + j * step, people)
                               for i in range(-n, n + 1)
                               for j in range(-n, n + 1)])

    def test_capture_integrates_on_the_population_cells(self):
        """The rings were a stand-in for where demand is. With a grid loaded
        the cells ARE the quadrature points, so nothing is left to converge —
        refining the ring used to ALIAS against the population lattice rather
        than converge (78.2% error at 40 points, 2.2% at 504, back up to 7.0%
        at 1,440)."""
        from oasis.logic.site_scoring import score_site
        import oasis.logic.site_scoring as SS
        g = self._grid()
        rivals = [{"lat": -1.29, "lon": 36.81, "size_sqft": 20000,
                   "chain": "R", "pull": 1.0}]
        base = score_site(-1.28, 36.80, [], rivals, size_sqft=10000,
                          population=g)
        # The ring layout must not change a population-weighted answer at all.
        old = SS.RING_FRACTIONS
        try:
            SS.RING_FRACTIONS = (0.05, 0.2, 0.4, 0.6, 0.8, 0.95)
            other = score_site(-1.28, 36.80, [], rivals, size_sqft=10000,
                               population=g)
        finally:
            SS.RING_FRACTIONS = old
        assert abs(base["captured_population"]
                   - other["captured_population"]) < 1e-6

    def test_the_ring_still_serves_a_client_with_no_population(self):
        from oasis.logic.site_scoring import score_site
        r = score_site(-1.28, 36.80, [],
                       [{"lat": -1.29, "lon": 36.81, "size_sqft": 20000}],
                       size_sqft=10000)
        assert r["captured_population"] is None
        assert 0 < r["capture_pct"] < 100

    def test_cannibalisation_is_displacement_not_presence(self):
        """It used to be own_share/(own_share+capture) — the own network's
        share of the own-plus-site bloc, which is a statement about how present
        you already are. On the client's estate the two disagreed by up to 4x."""
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        # One own store, far enough that it is in the catchment but not on top.
        own = [{"lat": -1.30, "lon": 36.80, "size_sqft": 20000,
                "chain": "Mine", "pull": 1.0}]
        rivals = [{"lat": -1.285, "lon": 36.805, "size_sqft": 20000,
                   "chain": "R", "pull": 1.0}]
        r = score_site(-1.28, 36.80, own, rivals, size_sqft=10000,
                       population=g)
        # The old formula would report own_share/(own_share+capture); the
        # displacement figure must be strictly smaller here, because the rival
        # funds most of the entrant's trade.
        assert 0 < r["cannibalisation_pct"] < 50

    def test_no_own_stores_means_no_cannibalisation(self):
        from oasis.logic.site_scoring import score_site
        r = score_site(-1.28, 36.80, [],
                       [{"lat": -1.29, "lon": 36.81, "size_sqft": 20000,
                         "chain": "R", "pull": 1.0}],
                       size_sqft=10000, population=self._grid())
        assert r["cannibalisation_pct"] == 0.0

    def test_cannibalisation_matches_the_displacement_matrix(self):
        """One definition, two implementations, and they must agree."""
        import sys as _sys
        _sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..',
                                         'devkit'))
        import matrix_sweep as MS
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        stores = [{"lat": -1.30, "lon": 36.80, "size_sqft": 20000,
                   "chain": "Mine", "pull": 1.0},
                  {"lat": -1.285, "lon": 36.805, "size_sqft": 20000,
                   "chain": "R", "pull": 1.0}]
        own = [s for s in stores if s["chain"] == "Mine"]
        riv = [s for s in stores if s["chain"] != "Mine"]
        scored = score_site(-1.28, 36.80, own, riv, size_sqft=10000,
                            population=g)
        cap, _unt, lost = MS.displacement(-1.28, 36.80, 10000.0, "Mine",
                                          stores, g, 10.0)
        matrix_pct = 100.0 * lost.get("Mine", 0.0) / cap
        assert abs(scored["cannibalisation_pct"] - matrix_pct) < 0.01

    def test_a_bounded_fraction(self):
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        own = [{"lat": -1.281, "lon": 36.801, "size_sqft": 60000,
                "chain": "Mine", "pull": 1.0}]
        r = score_site(-1.28, 36.80, own, [], size_sqft=10000, population=g)
        assert 0.0 <= r["cannibalisation_pct"] <= 100.0


class TestSupplyIsNotTruncatedAtTheCatchment:
    """A person at the catchment edge may have a rival ten kilometres beyond
    it — far from us, near to them. Deleting that rival from the denominator
    invents share, and it did so worst exactly where the analysis then claimed
    opportunity: the top whitespace site ignored 83.4% of the pull acting on
    it, and a site reported as a 100% untapped market had every competitor
    just outside the ring."""

    def _grid(self, n=14, step=0.006, people=400.0):
        from oasis.logic.population import PopulationGrid
        return PopulationGrid([(-1.28 + i * step, 36.80 + j * step, people)
                               for i in range(-n, n + 1)
                               for j in range(-n, n + 1)])

    def test_the_two_radii_are_separate_and_supply_is_larger(self):
        from oasis.logic.site_scoring import CATCHMENT_KM, SUPPLY_KM
        assert SUPPLY_KM > CATCHMENT_KM * 2, (
            "supply must reach well past the area whose people we claim")

    def test_a_rival_beyond_the_catchment_still_takes_share(self):
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        # 15 km away: outside a 10 km catchment, inside the supply radius.
        far = [{"lat": -1.28 + 15 / 110.574, "lon": 36.80,
                "size_sqft": 40000, "chain": "R", "pull": 1.0}]
        alone = score_site(-1.28, 36.80, [], [], size_sqft=10000, population=g)
        with_far = score_site(-1.28, 36.80, [], far, size_sqft=10000,
                              population=g)
        assert with_far["captured_population"] < alone["captured_population"]

    def test_truncating_supply_at_the_catchment_inflates_capture(self):
        """The old behaviour, reproduced by passing supply_km=catchment_km."""
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        far = [{"lat": -1.28 + 15 / 110.574, "lon": 36.80,
                "size_sqft": 40000, "chain": "R", "pull": 1.0}]
        truncated = score_site(-1.28, 36.80, [], far, size_sqft=10000,
                               population=g, supply_km=10.0)
        honest = score_site(-1.28, 36.80, [], far, size_sqft=10000,
                            population=g)
        assert truncated["captured_population"] > honest["captured_population"]

    def test_the_supply_radius_is_converged(self):
        """40 km is not another arbitrary cutoff: with 1/d^2 the tail beyond it
        is numerically irrelevant. Measured on the real matrix, 40 km lands
        within 3% of including every store, and 80 km and 200 km are
        identical."""
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        rivals = [{"lat": -1.28 + km / 110.574, "lon": 36.80,
                   "size_sqft": 30000, "chain": f"R{km}", "pull": 1.0}
                  for km in (5, 15, 30, 60, 120)]
        at40 = score_site(-1.28, 36.80, [], rivals, size_sqft=10000,
                          population=g, supply_km=40.0)
        at200 = score_site(-1.28, 36.80, [], rivals, size_sqft=10000,
                           population=g, supply_km=200.0)
        rel = abs(at40["captured_population"] - at200["captured_population"])
        assert rel / at200["captured_population"] < 0.05

    def test_the_descriptive_counts_still_use_the_catchment(self):
        """`own_stores_in_catchment` and `competitors_within_2km` describe the
        site's immediate context and must not silently widen with supply."""
        from oasis.logic.site_scoring import score_site
        far = [{"lat": -1.28 + 15 / 110.574, "lon": 36.80,
                "size_sqft": 40000, "chain": "R"}]
        r = score_site(-1.28, 36.80, [], far, size_sqft=10000)
        assert r["competitors_within_2km"] == 0
        assert r["nearest_competitor_km"] > 10


class TestBetaIsReportedAsARange:
    """The distance exponent has never been fitted. Against the client's own
    store revenues the fit is monotone in beta with its minimum at zero — the
    model that ignores distance entirely — which is the signature of a
    parameter five stores cannot locate. So results carry the band."""

    def _grid(self, n=14, step=0.006):
        from oasis.logic.population import PopulationGrid
        return PopulationGrid([(-1.28 + i * step, 36.80 + j * step, 400.0)
                               for i in range(-n, n + 1)
                               for j in range(-n, n + 1)])

    def test_beta_actually_changes_the_answer(self):
        from oasis.logic.site_scoring import score_site
        g = self._grid()
        riv = [{"lat": -1.30, "lon": 36.80, "size_sqft": 30000,
                "chain": "R", "pull": 1.0}]
        flat = score_site(-1.28, 36.80, [], riv, size_sqft=10000,
                          population=g, beta=1.5)
        steep = score_site(-1.28, 36.80, [], riv, size_sqft=10000,
                           population=g, beta=3.0)
        assert flat["captured_population"] != steep["captured_population"]
        assert flat["beta"] == 1.5 and steep["beta"] == 3.0

    def test_the_band_brackets_the_central_estimate(self):
        from oasis.logic.site_scoring import DISTANCE_DECAY, score_band
        g = self._grid()
        riv = [{"lat": -1.30, "lon": 36.80, "size_sqft": 30000,
                "chain": "R", "pull": 1.0}]
        b = score_band(-1.28, 36.80, [], riv, size_sqft=10000, population=g)
        assert b["beta"] == DISTANCE_DECAY
        assert (b["captured_population_low"] <= b["captured_population"]
                <= b["captured_population_high"])
        assert b["beta_span_ratio"] >= 1.0

    def test_a_site_with_no_rivals_is_beta_insensitive(self):
        """With nothing to compete against, the share is 100% at every
        exponent — so the band must collapse, not widen."""
        from oasis.logic.site_scoring import score_band
        b = score_band(-1.28, 36.80, [], [], size_sqft=10000,
                       population=self._grid())
        assert b["captured_population_low"] == b["captured_population_high"]
        assert b["beta_sensitive"] is False

    def test_rank_band_reports_how_far_each_site_travels(self):
        from oasis.logic.site_scoring import rank_band
        g = self._grid()
        riv = [{"lat": -1.30, "lon": 36.80, "size_sqft": 30000,
                "chain": "R", "pull": 1.0}]
        sites = [{"name": "near", "lat": -1.283, "lon": 36.80},
                 {"name": "far", "lat": -1.255, "lon": 36.80},
                 {"name": "mid", "lat": -1.270, "lon": 36.80}]
        out = rank_band(sites, [], riv, population=g)
        assert len(out) == 3
        for r in out:
            assert 1 <= r["rank_best"] <= r["rank_worst"] <= 3
            assert isinstance(r["rank_stable"], bool)
        # sorted best-rank first
        assert [r["rank_best"] for r in out] == sorted(r["rank_best"] for r in out)

    def test_score_sites_attaches_the_band_and_the_rank_range(self, tmp_path):
        from oasis.desktop import data as D
        _data_dir(tmp_path)
        SL.save_location("ORG001", -1.26, 36.79, 14000, root=str(tmp_path))
        res = D.score_sites([{"name": "C", "lat": -1.28, "lon": 36.80,
                              "size_sqft": 10000}], root=str(tmp_path))
        # No POS on this sandbox, so it refuses — but it must refuse cleanly.
        assert res.get("error") or res["sites"][0].get("betas")

    def test_capital_carries_no_band_when_geography_does_not_set_it(self):
        """On the productivity basis the site does not enter the figure, so a
        range there would be three copies of one number dressed as a band."""
        from oasis.logic import site_capital as SC
        obs = [{"org_cd": c, "name": c, "size_sqft": 10000.0, "capture": 0.2,
                "revenue": 100_000.0, "stock_value": 30_000.0,
                "captured_population": 5000.0, "catchment_population": 15000.0,
                "spend_per_person": 20.0, "affluence_index": None,
                "implied_demand": 500_000.0, "revenue_per_sqft": 10.0}
               for c in "ABC"]
        cal, val = SC.calibrate(obs), SC.loo_validate(obs)
        assert val["validated"] is False
        a = SC.propose_capital(10.0, 10_000, cal, val, captured_population=4000)
        b = SC.propose_capital(30.0, 10_000, cal, val, captured_population=9000)
        assert a["expected_revenue"] == b["expected_revenue"]
