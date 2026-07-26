"""
OASIS shared UI components (U1) + journey primitives (U0).

Design: each component has a pure ``_html_*`` builder (import-safe, unit
testable, returns an HTML string) and a thin public ``render`` wrapper that
calls ``st.markdown(..., unsafe_allow_html=True)``. Interactive components
(confirm_button, decision_gate_card) require Streamlit and return a value.

All HTML is built from the SYS v2.9 tokens in ``theme`` — no inline colours.
"""

from __future__ import annotations

import html as _html
from typing import Optional, Sequence, Tuple

from . import theme

# The 7 journey stages (see OASIS_Customer_Journey.md). Index == phase order.
JOURNEY_STAGES: Tuple[str, ...] = (
    "Diagnose", "Prove", "Stabilise", "Fund", "Shield", "Automate", "Sustain",
)

MODES = ("SHADOW", "ACTIVE", "AUTONOMOUS")


def _esc(v) -> str:
    return _html.escape(str(v if v is not None else ""))


def _fmt_kes(amount: float) -> str:
    try:
        return f"KES {float(amount):,.0f}"
    except (TypeError, ValueError):
        return "KES 0"


def _html_metric_card(label: str, value, sub: Optional[str] = None,
                      status: Optional[str] = None, help_text: Optional[str] = None) -> str:
    accent = ""
    if status in theme.STATUS:
        _, colour, _ = theme.STATUS[status]
        accent = f"border-left:4px solid {colour};"
    sub_html = f'<div class="oasis-metric-sub">{_esc(sub)}</div>' if sub else ""
    title_attr = f' title="{_esc(help_text)}"' if help_text else ""
    return (
        f'<div class="oasis-card" style="{accent}"{title_attr}>'
        f'<div class="oasis-metric-label">{_esc(label)}</div>'
        f'<div class="oasis-metric-value">{_esc(value)}</div>'
        f'{sub_html}</div>'
    )


def metric_card(label, value, sub=None, status=None, help_text=None, st_module=None):
    _render(_html_metric_card(label, value, sub, status, help_text), st_module)


def kpi_row(items: Sequence[dict], st_module=None):
    """items: list of {label, value, sub?, status?, help?}. Renders in columns."""
    st = st_module or _st()
    cols = st.columns(len(items)) if items else []
    for col, item in zip(cols, items):
        with col:
            metric_card(item.get("label", ""), item.get("value", ""),
                        item.get("sub"), item.get("status"),
                        help_text=item.get("help"), st_module=st)


# ── Alerts / cards ───────────────────────────────────────────────────────
def _html_alert_card(title: str, body: str, severity: str = "warning") -> str:
    icon, colour, _ = theme.STATUS.get(severity, theme.STATUS["info"])
    return (
        f'<div class="oasis-card" style="border-left:4px solid {colour};">'
        f'<div style="color:{colour};font-weight:700;">{icon} {_esc(title)}</div>'
        f'<div style="color:var(--oasis-text-2);font-size:0.9em;margin-top:4px;">'
        f'{_esc(body)}</div></div>'
    )


def alert_card(title, body, severity="warning", st_module=None):
    _render(_html_alert_card(title, body, severity), st_module)


def _html_supplier_status_chip(classification: str) -> str:
    key, label = theme.SUPPLIER_STATUS.get(
        str(classification).upper(), ("info", str(classification)))
    icon, colour, _ = theme.STATUS[key]
    return (
        f'<span class="oasis-chip" style="color:{colour};border-color:{colour};">'
        f'{icon} {_esc(label)}</span>'
    )


def supplier_status_chip(classification, st_module=None):
    _render(_html_supplier_status_chip(classification), st_module)


# ── Error & empty states (U4 primitives, available now) ──────────────────
def _html_error_panel(message: str) -> str:
    colour = theme.PALETTE.danger
    return (
        f'<div class="oasis-card" style="border-left:4px solid {colour};">'
        f'<div style="color:{colour};font-weight:700;">Something went wrong</div>'
        f'<div style="color:var(--oasis-text-2);font-size:0.9em;margin-top:4px;">'
        f'{_esc(message)}</div></div>'
    )


def error_panel(message: str, detail: Optional[str] = None, st_module=None):
    """Friendly error for the user; full detail goes to the logger, not the screen."""
    if detail:
        import logging
        logging.getLogger("OASIS.UI").error("UI error: %s | %s", message, detail)
    _render(_html_error_panel(message), st_module)


def _html_empty_state(title: str, body: str) -> str:
    return (
        f'<div class="oasis-card" style="text-align:center;padding:32px;">'
        f'<div style="font-weight:700;font-size:1.1em;">{_esc(title)}</div>'
        f'<div style="color:var(--oasis-text-2);margin-top:6px;">{_esc(body)}</div>'
        f'</div>'
    )


def empty_state(title: str, body: str, st_module=None):
    _render(_html_empty_state(title, body), st_module)


def safe_render(render_fn, ctx, st_module=None) -> bool:
    """Run a page render, converting any exception into a friendly panel.

    Full detail (with traceback) goes to the structured logger; the user sees
    a calm message, never a Python traceback. Returns True on success, False
    if the render raised. This is the U4 catch-all wrapped around every page.
    """
    st = st_module or ctx.get("st") or _st()
    try:
        render_fn(ctx)
        return True
    except Exception as e:
        import logging
        import traceback
        logging.getLogger("OASIS.UI").error(
            "Page render failed: %s\n%s", e, traceback.format_exc())
        # error_panel called without detail so it doesn't double-log the line.
        _render(_html_error_panel(
            "This screen hit a problem and couldn't load. The team has been "
            "notified — try again, or pick another page from the left."), st)
        return False


# ── U0: Journey primitives ───────────────────────────────────────────────
def _html_mode_phase_badge(mode: str, phase_no: int, phase_name: str,
                           value_recovered: float) -> str:
    mode_u = str(mode).upper()
    colour = {
        "SHADOW": theme.PALETTE.info,
        "ACTIVE": theme.PALETTE.warning,
        "AUTONOMOUS": theme.PALETTE.success,
    }.get(mode_u, theme.PALETTE.text_secondary)
    return (
        f'<div class="oasis-badge">'
        f'<span style="color:{colour};font-weight:700;">● {_esc(mode_u)}</span>'
        f'<span style="color:var(--oasis-text-2);">Phase {_esc(phase_no)} · '
        f'{_esc(phase_name)}</span>'
        f'<span class="oasis-mono" style="color:var(--oasis-teal);">'
        f'{_esc(_fmt_kes(value_recovered))} recovered</span>'
        f'</div>'
    )


def mode_phase_badge(mode, phase_no, phase_name, value_recovered, st_module=None):
    _render(_html_mode_phase_badge(mode, phase_no, phase_name, value_recovered),
            st_module)


def _html_value_recovered_meter(recovered: float, target: float) -> str:
    pct = 0.0
    if target and target > 0:
        pct = max(0.0, min(100.0, recovered / target * 100.0))
    return (
        f'<div style="margin:8px 0;">'
        f'<div class="oasis-metric-label">Capital recovered</div>'
        f'<div class="oasis-mono" style="font-size:1.4em;color:var(--oasis-teal);">'
        f'{_esc(_fmt_kes(recovered))}'
        f'<span style="color:var(--oasis-text-2);font-size:0.6em;"> / '
        f'{_esc(_fmt_kes(target))} target</span></div>'
        f'<div class="oasis-meter-track"><div class="oasis-meter-fill" '
        f'style="width:{pct:.1f}%;"></div></div></div>'
    )


def value_recovered_meter(recovered, target, st_module=None):
    _render(_html_value_recovered_meter(recovered, target), st_module)


def _html_journey_rail(current_index: int) -> str:
    steps = []
    for i, name in enumerate(JOURNEY_STAGES):
        cls = "done" if i < current_index else ("current" if i == current_index else "")
        steps.append(f'<div class="oasis-rail-step {cls}">{i}. {_esc(name)}</div>')
    return f'<div class="oasis-rail">{"".join(steps)}</div>'


def journey_rail(current_index: int, st_module=None):
    """Render the 7-stage ladder with the current stage highlighted."""
    _render(_html_journey_rail(int(current_index)), st_module)


def decision_gate_card(title: str, evidence: str, next_stage: str,
                       can_advance: bool, key: str, st_module=None) -> bool:
    """Human-confirmed gate (journey decision #4 is never auto-advance).

    Renders the evidence and, if `can_advance`, an explicit Approve button.
    Returns True only when the operator/exec clicks Approve this run.
    """
    st = st_module or _st()
    icon = "✓" if can_advance else "…"
    colour = theme.PALETTE.success if can_advance else theme.PALETTE.warning
    _render(
        f'<div class="oasis-card" style="border-left:4px solid {colour};">'
        f'<div style="color:{colour};font-weight:700;">{icon} {_esc(title)}</div>'
        f'<div style="color:var(--oasis-text-2);font-size:0.9em;margin-top:4px;">'
        f'{_esc(evidence)}</div></div>', st)
    if can_advance:
        return st.button(f"Advance to {next_stage}", key=key, type="primary")
    return False


def confirm_button(label: str, key: str, confirm_label: str = "Confirm",
                   st_module=None) -> bool:
    """Two-step confirm for financial/destructive actions.

    First click arms; second click (within the same session) confirms.
    Returns True only on the confirming click.
    """
    st = st_module or _st()
    armed_key = f"_confirm_armed_{key}"
    if not st.session_state.get(armed_key):
        if st.button(label, key=f"{key}_arm"):
            st.session_state[armed_key] = True
        return False
    if st.button(f"⚠ {confirm_label}", key=f"{key}_go", type="primary"):
        st.session_state[armed_key] = False
        return True
    if st.button("Cancel", key=f"{key}_cancel"):
        st.session_state[armed_key] = False
    return False


# ── internals ────────────────────────────────────────────────────────────
def _st(st_module=None):
    # Return the caller's streamlit module if supplied (spec_tag/status_ticker
    # pass it through); otherwise import the global one. Accepting the optional
    # arg keeps _st(st_module) and the `st_module or _st()` idiom both valid.
    if st_module is not None:
        return st_module
    import streamlit as st
    return st


def _render(html_str: str, st_module=None) -> None:
    st = st_module or _st()
    st.markdown(html_str, unsafe_allow_html=True)


# ── OASIS SYSTEMS v1.0 brand primitives ────────────────────────────────────
def _html_spec_tag(text: str, hot: bool = False) -> str:
    """Render a spec-sheet-style bracket tag: [ AUDITED LOGIC_ENGINE_V3.1 ].

    Set ``hot=True`` for the teal-highlighted variant (system-alive tags).
    """
    cls = "oasis-tag hot" if hot else "oasis-tag"
    return f'<span class="{cls}">{_esc(text)}</span>'


def spec_tag(text: str, hot: bool = False, st_module=None):
    _st(st_module).markdown(_html_spec_tag(text, hot), unsafe_allow_html=True)


def _html_status_ticker(items: Sequence[dict]) -> str:
    """The brand's signature ticker line: [● SYSTEM READINESS: PEAK].

    Each item is {"label": ..., "value": ...}. All items get the teal dot
    treatment — the ticker is intentionally uniform (not colour-coded), the
    dot alone signals liveness.
    """
    parts = []
    for it in items:
        label = _esc(it.get("label", ""))
        value = _esc(str(it.get("value", "")))
        parts.append(
            f'<span class="item"><span class="dot"></span>'
            f'{label}: <span class="val">{value}</span></span>'
        )
    return f'<div class="oasis-ticker">{"".join(parts)}</div>'


def status_ticker(items: Sequence[dict], st_module=None):
    _st(st_module).markdown(_html_status_ticker(items), unsafe_allow_html=True)


def render_top_header(console_title: str, console_key: str = "ops", st_module=None):
    """Top navigation bar component (UX Heuristic 3 & 4) rendering:
    - Spec tag & title
    - Persistent Home link
    - Data source badge
    """
    st = st_module or _st()
    from .onboarding import data_source_badge
    spec_tag(f"AUDITED LOGIC_ENGINE_V2.3 · {console_title.upper()}", hot=True, st_module=st)
    data_source_badge(st)
