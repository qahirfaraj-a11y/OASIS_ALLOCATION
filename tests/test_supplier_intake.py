"""Tests for the supplier & ordering-calendar intake (Universe Init v1.1)."""

import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.logic.supplier_intake import (
    _norm_days, weekdays_to_doy, supplier_schedule, patterns_overrides,
    fresh_suppliers, apply_supplier_intake,
)


class TestNormDays:
    def test_daily(self):
        assert _norm_days("DAILY") == "DAILY"

    def test_weekdays(self):
        assert _norm_days("Mon,Wed,Fri") == [0, 2, 4]

    def test_blank_is_none(self):
        assert _norm_days("") is None and _norm_days(None) is None


class TestSchedule:
    def test_daily_type(self):
        rows = [{"Supplier_Name": "Brookside", "Type": "DAILY"}]
        assert supplier_schedule(rows, 2026) == {"Brookside": "DAILY"}

    def test_weekday_expansion(self):
        rows = [{"Supplier_Name": "ACME", "Type": "REGULAR", "Order_Days": "Mon"}]
        sched = supplier_schedule(rows, 2026)
        doys = sched["ACME"]
        # every entry is a Monday in 2026
        import datetime
        assert all(datetime.date(2026, 1, 1).fromordinal(
            datetime.date(2026, 1, 1).toordinal() + (d - 1)).weekday() == 0 for d in doys)
        assert len(doys) >= 52

    def test_blank_order_days_omitted(self):
        # REGULAR with no Order_Days -> derive later -> not in schedule
        rows = [{"Supplier_Name": "X", "Type": "REGULAR", "Order_Days": ""}]
        assert supplier_schedule(rows, 2026) == {}


class TestPatternsAndFresh:
    def test_patterns_overrides(self):
        rows = [{"Supplier_Name": "ACME", "Type": "REGULAR",
                 "Lead_Time_Days": "2", "Reliability": "0.9"}]
        ov = patterns_overrides(rows)
        assert ov["ACME"]["estimated_delivery_days"] == 2
        assert ov["ACME"]["reliability_score"] == 0.9

    def test_daily_sets_order_frequency(self):
        ov = patterns_overrides([{"Supplier_Name": "B", "Type": "DAILY"}])
        assert ov["B"]["order_frequency"] == "daily"

    def test_fresh_list(self):
        rows = [{"Supplier_Name": "Dairy Co", "Type": "FRESH"},
                {"Supplier_Name": "Dry Co", "Type": "REGULAR"}]
        assert fresh_suppliers(rows) == ["Dairy Co"]


class TestApplyAndCalendarConsumption:
    def test_apply_writes_artifacts_and_calendar_reads_them(self, tmp_path):
        d = str(tmp_path)
        with open(os.path.join(d, "suppliers.csv"), "w", encoding="utf-8") as f:
            f.write("Supplier_Name,Type,Order_Days,Lead_Time_Days\n")
            f.write("Brookside Dairy,DAILY,,1\n")
            f.write("ACME Dry,REGULAR,Mon,3\n")
        res = apply_supplier_intake(d, year=2026)
        assert res["applied"] is True
        sched = json.load(open(os.path.join(d, "supplier_schedule.json"), encoding="utf-8"))
        assert sched["Brookside Dairy"] == "DAILY"
        assert isinstance(sched["ACME Dry"], list)
        # supplier_patterns overrides written
        sp = json.load(open(os.path.join(d, "supplier_patterns_2025_updated.json"), encoding="utf-8"))
        assert sp["ACME DRY"]["estimated_delivery_days"] == 3

        # SupplierCalendar consumes the JSON (DAILY resolved, fuzzy name match)
        from oasis.data.supplier_calendar import SupplierCalendar
        cal = SupplierCalendar(os.path.join(d, "Supplier_Order_Calendar_2026.xlsx"))
        cal.load()
        assert cal.get_schedule("SA0012 - Brookside Dairy Limited") == "DAILY"

    def test_no_csv_is_noop(self, tmp_path):
        assert apply_supplier_intake(str(tmp_path))["applied"] is False
