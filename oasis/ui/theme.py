"""
OASIS design tokens & theme (SYS v2.9).

Single source of truth for colour, type, and spacing. The pure helpers
(`theme_css`, `contrast_ratio`, token constants) are import-safe and unit
testable without a running Streamlit server; `inject_theme()` is the only
function that touches Streamlit and is a no-op-after-first-call guard.
"""

from __future__ import annotations

from dataclasses import dataclass


# ── SYS v2.9 palette ─────────────────────────────────────────────────────
# Values follow the Visual System Guide: system teal-turquoise accent,
# deep slate chrome, platform-white surfaces. Secondary text is chosen to
# pass WCAG AA (>=4.5:1) on the slate background (the old #888 on #0b0e14
# failed this).
@dataclass(frozen=True)
class Palette:
    # Brand
    teal: str = "#00E5C4"          # system teal-turquoise — primary accent / "alive"
    teal_dark: str = "#00B89E"     # hover / accent on light surfaces
    # Chrome
    deep_slate: str = "#10181D"    # app background
    slate_surface: str = "#1A2731" # cards / panels
    slate_border: str = "#2B3A44"  # hairlines
    platform_white: str = "#F4F7F9"
    # Text
    text_primary: str = "#F4F7F9"  # on slate
    text_secondary: str = "#AEBEC8"  # on slate — AA compliant
    text_muted: str = "#8497A3"    # on slate — AA for large/secondary only
    # Status (used as icon + label + colour, never colour alone)
    success: str = "#00E5C4"       # teal — healthy / reliable
    warning: str = "#FFB020"       # amber — watch
    danger: str = "#FF5247"        # ruby — hostile / critical
    info: str = "#4FB0FF"


@dataclass(frozen=True)
class Type:
    display: str = "'Montserrat', system-ui, sans-serif"   # headings / UI
    mono: str = "'Space Mono', ui-monospace, monospace"    # data / money / codes
    font_import: str = (
        "https://fonts.googleapis.com/css2?"
        "family=Montserrat:wght@400;500;600;700;800&"
        "family=Space+Mono:wght@400;700&display=swap"
    )


@dataclass(frozen=True)
class Space:
    xs: str = "4px"
    sm: str = "8px"
    md: str = "16px"
    lg: str = "24px"
    xl: str = "32px"
    radius: str = "14px"


PALETTE = Palette()
TYPE = Type()
SPACE = Space()

# Status vocabulary: status_key -> (icon, colour, label). Status is always
# conveyed by icon + label + colour together (accessibility).
STATUS = {
    "success": ("✓", PALETTE.success, "Healthy"),
    "warning": ("!", PALETTE.warning, "Watch"),
    "danger": ("✕", PALETTE.danger, "Critical"),
    "info": ("i", PALETTE.info, "Info"),
}

# Supplier classification (LATA) -> status key + label.
SUPPLIER_STATUS = {
    "RELIABLE": ("success", "Reliable"),
    "WATCH": ("warning", "Watch"),
    "HOSTILE": ("danger", "Hostile"),
}


# ── WCAG contrast helpers (pure) ─────────────────────────────────────────
def _hex_to_rgb(hex_color: str) -> tuple:
    h = hex_color.lstrip("#")
    if len(h) == 3:
        h = "".join(c * 2 for c in h)
    return tuple(int(h[i:i + 2], 16) for i in (0, 2, 4))


def _relative_luminance(hex_color: str) -> float:
    def _chan(c: int) -> float:
        s = c / 255.0
        return s / 12.92 if s <= 0.03928 else ((s + 0.055) / 1.055) ** 2.4
    r, g, b = (_chan(c) for c in _hex_to_rgb(hex_color))
    return 0.2126 * r + 0.7152 * g + 0.0722 * b


def contrast_ratio(fg: str, bg: str) -> float:
    """WCAG contrast ratio between two hex colours (1.0 – 21.0)."""
    l1, l2 = _relative_luminance(fg), _relative_luminance(bg)
    hi, lo = max(l1, l2), min(l1, l2)
    return (hi + 0.05) / (lo + 0.05)


def passes_aa(fg: str, bg: str, large: bool = False) -> bool:
    """True if the pair meets WCAG AA (4.5:1 body, 3.0:1 large text)."""
    return contrast_ratio(fg, bg) >= (3.0 if large else 4.5)


# ── Stylesheet (pure builder + injector) ─────────────────────────────────
def theme_css() -> str:
    """Return the full theme stylesheet as a string (testable)."""
    p, t, s = PALETTE, TYPE, SPACE
    return f"""
@import url('{t.font_import}');
:root {{
    --oasis-teal: {p.teal};
    --oasis-teal-dark: {p.teal_dark};
    --oasis-bg: {p.deep_slate};
    --oasis-surface: {p.slate_surface};
    --oasis-border: {p.slate_border};
    --oasis-text: {p.text_primary};
    --oasis-text-2: {p.text_secondary};
    --oasis-success: {p.success};
    --oasis-warning: {p.warning};
    --oasis-danger: {p.danger};
    --oasis-radius: {s.radius};
}}
.stApp {{ background: var(--oasis-bg); color: var(--oasis-text);
          font-family: {t.display}; }}
h1, h2, h3, h4 {{ font-family: {t.display}; font-weight: 700;
                  letter-spacing: -0.3px; color: var(--oasis-text); }}
.oasis-card {{ background: var(--oasis-surface);
               border: 1px solid var(--oasis-border);
               border-radius: var(--oasis-radius);
               padding: {s.lg}; margin: {s.sm} 0; }}
.oasis-mono, .oasis-metric-value {{ font-family: {t.mono}; }}
.oasis-metric-label {{ font-size: 0.78em; color: var(--oasis-text-2);
                       text-transform: uppercase; letter-spacing: 0.5px; }}
.oasis-metric-value {{ font-size: 2em; font-weight: 700;
                       color: var(--oasis-text); }}
.oasis-metric-sub {{ font-size: 0.85em; color: var(--oasis-text-2); }}
.oasis-chip {{ display: inline-flex; align-items: center; gap: 6px;
               padding: 3px 10px; border-radius: 999px; font-size: 0.8em;
               font-weight: 600; border: 1px solid; }}
.oasis-badge {{ display: inline-flex; align-items: center; gap: 10px;
                background: var(--oasis-surface); border: 1px solid var(--oasis-border);
                border-radius: 999px; padding: 6px 14px; font-size: 0.85em; }}
.oasis-rail {{ display: flex; gap: 6px; align-items: center; }}
.oasis-rail-step {{ flex: 1; text-align: center; font-size: 0.72em;
                    padding: 6px 4px; border-radius: 8px;
                    border: 1px solid var(--oasis-border);
                    color: var(--oasis-text-2); }}
.oasis-rail-step.done {{ border-color: var(--oasis-teal);
                         color: var(--oasis-teal); }}
.oasis-rail-step.current {{ background: var(--oasis-teal);
                            color: var(--oasis-bg); font-weight: 700;
                            border-color: var(--oasis-teal); }}
.oasis-meter-track {{ background: var(--oasis-border); border-radius: 999px;
                      height: 10px; overflow: hidden; }}
.oasis-meter-fill {{ background: var(--oasis-teal); height: 100%; }}
"""


_THEME_FLAG = "_oasis_theme_injected"


def inject_theme(st_module=None) -> None:
    """Inject the theme once per session. Pass `st` or let it import.

    Safe to call on every page render — guarded by a session flag so the
    stylesheet is emitted only once. Tenant primary/accent colors from
    branding.json override the default palette variables here.
    """
    if st_module is None:
        import streamlit as st_module
    if st_module.session_state.get(_THEME_FLAG):
        return
    st_module.markdown(f"<style>{theme_css()}</style>", unsafe_allow_html=True)
    # Whitelabel: overwrite the two accent CSS variables (rest of the palette
    # is intentionally shared so contrast + status colors don't drift).
    try:
        from ..logic.branding import load_branding
        b = load_branding()
        st_module.markdown(
            f"<style>:root {{ --oasis-teal: {b.primary_color}; "
            f"--oasis-teal-dark: {b.accent_color}; }}</style>",
            unsafe_allow_html=True)
    except Exception:
        pass
    st_module.session_state[_THEME_FLAG] = True
