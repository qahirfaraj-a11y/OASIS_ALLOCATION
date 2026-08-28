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
