"""Tests for the shared UI library (oasis/ui): theme tokens, WCAG contrast,
and the pure HTML builders for components incl. the U0 journey primitives.

These exercise the import-safe pure helpers — no running Streamlit server.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))

from oasis.ui import theme
from oasis.ui import components as C


class TestContrast:
    def test_secondary_text_passes_aa_on_slate(self):
        # The whole point of SYS v2.9 over the old #888-on-#0b0e14: secondary
        # text must meet WCAG AA (>=4.5:1) on the slate background.
        assert theme.passes_aa(theme.PALETTE.text_secondary, theme.PALETTE.deep_slate)
        assert theme.passes_aa(theme.PALETTE.text_primary, theme.PALETTE.deep_slate)

    def test_primary_text_on_surface_passes(self):
        assert theme.passes_aa(theme.PALETTE.text_primary, theme.PALETTE.slate_surface)

    def test_passes_aa_detects_failures(self):
        # Sanity: the function correctly rejects a genuinely low-contrast pair
        # (light grey on white ≈ 1.6:1).
        assert not theme.passes_aa("#AAAAAA", "#FFFFFF")

    def test_contrast_ratio_bounds(self):
        assert theme.contrast_ratio("#FFFFFF", "#000000") == \
            __import__("pytest").approx(21.0, abs=0.1)
        assert theme.contrast_ratio("#123456", "#123456") == \
            __import__("pytest").approx(1.0, abs=0.01)

    def test_hex_shorthand_supported(self):
        assert theme.contrast_ratio("#fff", "#000") > 20


class TestThemeCss:
    def test_css_contains_tokens_and_fonts(self):
        css = theme.theme_css()
        assert theme.PALETTE.teal in css
        assert "Montserrat" in css and "Space+Mono" in css
        assert "--oasis-teal" in css

    def test_inject_is_idempotent(self):
        class FakeState(dict):
            pass

        class FakeSt:
            def __init__(self):
                self.session_state = FakeState()
                self.calls = 0

            def markdown(self, *a, **k):
                self.calls += 1

        st = FakeSt()
        theme.inject_theme(st)
        theme.inject_theme(st)
        theme.inject_theme(st)
        assert st.calls == 1  # injected exactly once


class TestComponentHtml:
    def test_metric_card_escapes_and_includes_value(self):
        h = C._html_metric_card("Cash <Used>", "KES 1,200", sub="98% util")
        assert "KES 1,200" in h
        assert "<Used>" not in h  # escaped
        assert "&lt;Used&gt;" in h

    def test_metric_card_status_accent(self):
        h = C._html_metric_card("X", "1", status="danger")
        assert theme.PALETTE.danger in h

    def test_supplier_chip_maps_classification(self):
        assert "Reliable" in C._html_supplier_status_chip("RELIABLE")
        assert "Hostile" in C._html_supplier_status_chip("hostile")
        assert theme.PALETTE.danger in C._html_supplier_status_chip("HOSTILE")

    def test_supplier_chip_is_icon_label_colour(self):
        # accessibility: not colour alone — must carry an icon glyph + label text
        h = C._html_supplier_status_chip("WATCH")
        assert "Watch" in h          # label
        assert theme.STATUS["warning"][0] in h  # icon glyph

    def test_alert_card_severity(self):
        assert theme.PALETTE.danger in C._html_alert_card("t", "b", "danger")

    def test_error_panel_is_friendly(self):
        h = C._html_error_panel("Could not load scorecard")
        assert "Something went wrong" in h
        assert "Traceback" not in h

    def test_empty_state(self):
        h = C._html_empty_state("No data yet", "Upload a scorecard to begin")
        assert "No data yet" in h


class TestSafeRender:
    class FakeSt:
        def __init__(self):
            self.markdown_calls = []

        def markdown(self, html, **k):
            self.markdown_calls.append(html)

    def test_success_returns_true_and_runs(self):
        st = self.FakeSt()
        ran = {}
        ok = C.safe_render(lambda ctx: ran.setdefault("yes", True), {"st": st}, st_module=st)
        assert ok is True
        assert ran.get("yes") is True

    def test_exception_is_caught_and_paneled(self):
        st = self.FakeSt()

        def boom(ctx):
            raise RuntimeError("kaboom")

        ok = C.safe_render(boom, {"st": st}, st_module=st)
        assert ok is False                      # did not propagate
        joined = " ".join(st.markdown_calls)
        assert "pick another page" in joined    # friendly panel shown
        assert "kaboom" not in joined           # raw error NOT on screen
        assert "Traceback" not in joined


class TestJourneyPrimitives:
    def test_mode_phase_badge(self):
        h = C._html_mode_phase_badge("active", 4, "DHARAM", 4_200_000)
        assert "ACTIVE" in h
        assert "Phase 4" in h and "DHARAM" in h
        assert "KES 4,200,000 recovered" in h

    def test_value_meter_clamps_and_formats(self):
        assert "100.0%" in C._html_value_recovered_meter(150, 100)  # clamp >100
        assert "0.0%" in C._html_value_recovered_meter(0, 100)
        assert "KES 50,000" in C._html_value_recovered_meter(50_000, 200_000)

    def test_value_meter_zero_target_safe(self):
        # no divide-by-zero
        h = C._html_value_recovered_meter(0, 0)
        assert "0.0%" in h

    def test_journey_rail_marks_current_and_done(self):
        h = C._html_journey_rail(3)  # at "Fund"
        assert "current" in h
        assert "done" in h
        for stage in C.JOURNEY_STAGES:
            assert stage in h

    def test_journey_rail_has_seven_stages(self):
        assert len(C.JOURNEY_STAGES) == 7
        assert C.JOURNEY_STAGES[0] == "Diagnose"
        assert C.JOURNEY_STAGES[-1] == "Sustain"


class TestInteractiveWithFakeStreamlit:
    """confirm_button / decision_gate_card need st — drive with a fake."""

    class FakeSt:
        def __init__(self, click=None):
            self.session_state = {}
            self._click = click or set()

        def markdown(self, *a, **k):
            pass

        def button(self, label, key=None, **k):
            return key in self._click

    def test_confirm_button_two_step(self):
        # 1st render: arm button shown, not confirmed
        st = self.FakeSt(click={"pay_arm"})
        assert C.confirm_button("Pay", key="pay", st_module=st) is False
        assert st.session_state.get("_confirm_armed_pay") is True
        # 2nd render (armed): confirming click returns True
        st2 = self.FakeSt(click={"pay_go"})
        st2.session_state["_confirm_armed_pay"] = True
        assert C.confirm_button("Pay", key="pay", st_module=st2) is True

    def test_decision_gate_blocks_when_not_ready(self):
        st = self.FakeSt(click={"g1"})
        # can_advance False → no button, returns False even if clicked
        assert C.decision_gate_card("Gate", "not yet", "Active",
                                    can_advance=False, key="g1", st_module=st) is False

    def test_decision_gate_advances_when_ready_and_clicked(self):
        st = self.FakeSt(click={"g2"})
        assert C.decision_gate_card("Gate", "targets met", "Active",
                                    can_advance=True, key="g2", st_module=st) is True
