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
# --- OASIS SYSTEMS v1.0 Brand Ecosystem -------------------------------------
# Exact tokens from the primary spec sheet (Obsidian Mode):
#   Deep #0B1020  ·  System #00F5D4  ·  Platform #FFFFFF  ·  Neutral #E0E0E0
# The chrome takes a "technical spec sheet" personality: monospace uppercase
# labels ([ AUDITED / SYSTEM READY / NODAL INTEGRITY ]), teal status ticker
# dots, and a display family (JetBrains-family) chosen to match the brand's
# angular geometric weight.
@dataclass(frozen=True)
class Palette:
    # Brand — exact from the spec sheet
    teal: str = "#00F5D4"           # SYSTEM #00F5D4 — primary accent / "alive"
    teal_dark: str = "#00C4AB"       # accent on light surfaces / hover
    teal_glow: str = "#00F5D466"     # soft glow (40% alpha) for dot halos
    # Chrome
    deep_obsidian: str = "#0B1020"   # DEEP #0B1020 — app background
    obsidian_raise: str = "#141A2E"  # cards / panels — one step up from bg
    obsidian_border: str = "#1F2740" # hairlines
    platform_white: str = "#FFFFFF"  # PLATFORM #FFFFFF
    neutral: str = "#E0E0E0"         # NEUTRAL #E0E0E0
    # Text (AA-tested on the obsidian background)
    text_primary: str = "#FFFFFF"    # on obsidian
    text_secondary: str = "#B8C0D9"  # on obsidian — AA compliant
    text_muted: str = "#7A85A8"      # on obsidian — AA for large text
    # Status (icon + label + colour vocabulary — accessibility)
    success: str = "#00F5D4"         # teal — SECURE / PEAK / CONFIRMED
    warning: str = "#FFC857"         # amber — WATCH
    danger: str = "#FF4D6D"          # ruby — HOSTILE / VULNERABLE
    info: str = "#7DD3FC"


@dataclass(frozen=True)
class Type:
    # Angular / geometric family to match the brand's spec-sheet personality.
    display: str = "'JetBrains Sans', 'Inter', system-ui, sans-serif"
    mono: str = "'JetBrains Mono', ui-monospace, 'Space Mono', monospace"
    font_import: str = (
        "https://fonts.googleapis.com/css2?"
        "family=Inter:wght@400;500;600;700;800;900&"
        "family=JetBrains+Mono:wght@400;500;700&display=swap"
    )


@dataclass(frozen=True)
class Space:
    xs: str = "4px"
    sm: str = "8px"
    md: str = "16px"
    lg: str = "24px"
    xl: str = "40px"    # more generous — spec-sheet whitespace
    radius: str = "10px"  # tighter — technical rather than friendly


PALETTE = Palette()
TYPE = Type()
SPACE = Space()


# ── Back-compat: SYS v2.9 attr names alias into the new brand tokens ──
# Old callers referenced `deep_slate` / `slate_surface` etc.; we keep those
# names working (proxying to the obsidian tokens) so existing code and tests
# don't need to change beyond the rename.
def _palette_alias(inst: Palette, old: str, new: str) -> None:
    object.__setattr__(inst, old, getattr(inst, new))


_palette_alias(PALETTE, "deep_slate", "deep_obsidian")
_palette_alias(PALETTE, "slate_surface", "obsidian_raise")
_palette_alias(PALETTE, "slate_border", "obsidian_border")

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
    --oasis-teal-glow: {p.teal_glow};
    --oasis-bg: {p.deep_obsidian};
    --oasis-surface: {p.obsidian_raise};
    --oasis-border: {p.obsidian_border};
    --oasis-neutral: {p.neutral};
    --oasis-text: {p.text_primary};
    --oasis-text-2: {p.text_secondary};
    --oasis-text-3: {p.text_muted};
    --oasis-success: {p.success};
    --oasis-warning: {p.warning};
    --oasis-danger: {p.danger};
    --oasis-radius: {s.radius};
    --oasis-mono: {t.mono};
    --oasis-display: {t.display};
}}
/* --- App shell -------------------------------------------------------- */
.stApp {{ background: var(--oasis-bg); color: var(--oasis-text);
          font-family: var(--oasis-display);
          background-image:
              radial-gradient(circle at 20% 0%, rgba(0,245,212,0.05) 0%, transparent 40%),
              radial-gradient(circle at 80% 100%, rgba(0,245,212,0.03) 0%, transparent 40%);
          background-attachment: fixed; }}

/* Editorial display headings — heavy, tight, uppercase for H1 */
h1 {{ font-family: var(--oasis-display); font-weight: 800;
      letter-spacing: -0.02em; text-transform: uppercase;
      color: var(--oasis-text); }}
h2, h3 {{ font-family: var(--oasis-display); font-weight: 700;
         letter-spacing: -0.01em; color: var(--oasis-text); }}
h4 {{ font-family: var(--oasis-mono); font-weight: 500;
      text-transform: uppercase; letter-spacing: 0.15em; font-size: 0.85em;
      color: var(--oasis-text-2); }}

/* Spec-sheet bracket tags — [ AUDITED / SYSTEM READY / etc. ] */
.oasis-tag {{ font-family: var(--oasis-mono); font-size: 0.72em;
              letter-spacing: 0.14em; text-transform: uppercase;
              color: var(--oasis-text-3); }}
.oasis-tag::before {{ content: "[ "; }}
.oasis-tag::after  {{ content: " ]"; }}
.oasis-tag.hot {{ color: var(--oasis-teal); }}

/* Cards — hairline border, no soft shadow (technical, not fluffy) */
.oasis-card {{ background: var(--oasis-surface);
               border: 1px solid var(--oasis-border);
               border-radius: var(--oasis-radius);
               padding: {s.lg}; margin: {s.sm} 0;
               position: relative; }}
.oasis-card::before {{ content: ""; position: absolute; top: 0; left: {s.md};
                       right: {s.md}; height: 1px;
                       background: linear-gradient(90deg, transparent,
                            var(--oasis-teal-glow), transparent);
                       opacity: 0.6; }}

/* Metrics — monospace value, uppercase micro-label */
.oasis-mono, .oasis-metric-value {{ font-family: var(--oasis-mono); }}
.oasis-metric-label {{ font-family: var(--oasis-mono); font-size: 0.72em;
                       color: var(--oasis-text-3);
                       text-transform: uppercase; letter-spacing: 0.15em; }}
.oasis-metric-value {{ font-size: 2.1em; font-weight: 500;
                       color: var(--oasis-text); font-variant-numeric: tabular-nums;
                       margin-top: 4px; }}
.oasis-metric-sub {{ font-size: 0.78em; color: var(--oasis-text-2);
                     font-family: var(--oasis-mono); }}

/* Status ticker dots — the brand's signature "● SYSTEM READINESS: PEAK" line */
.oasis-ticker {{ display: flex; flex-wrap: wrap; gap: {s.md};
                 font-family: var(--oasis-mono); font-size: 0.78em;
                 text-transform: uppercase; letter-spacing: 0.1em; }}
.oasis-ticker .item {{ display: inline-flex; align-items: center; gap: 8px;
                       color: var(--oasis-text-2); }}
.oasis-ticker .dot {{ width: 8px; height: 8px; border-radius: 50%;
                      background: var(--oasis-teal);
                      box-shadow: 0 0 8px var(--oasis-teal-glow); }}
.oasis-ticker .val {{ color: var(--oasis-teal); font-weight: 700; }}

/* Chips + badges */
.oasis-chip {{ display: inline-flex; align-items: center; gap: 6px;
               padding: 3px 10px; border-radius: 999px; font-size: 0.75em;
               font-weight: 500; font-family: var(--oasis-mono);
               text-transform: uppercase; letter-spacing: 0.08em;
               border: 1px solid; }}
.oasis-badge {{ display: inline-flex; align-items: center; gap: 10px;
                background: var(--oasis-surface); border: 1px solid var(--oasis-border);
                border-radius: 999px; padding: 6px 14px; font-size: 0.85em;
                font-family: var(--oasis-mono); letter-spacing: 0.06em; }}

/* Journey rail */
.oasis-rail {{ display: flex; gap: 6px; align-items: center; }}
.oasis-rail-step {{ flex: 1; text-align: center; font-size: 0.7em;
                    padding: 8px 4px; border-radius: 6px;
                    border: 1px solid var(--oasis-border);
                    color: var(--oasis-text-3);
                    font-family: var(--oasis-mono); text-transform: uppercase;
                    letter-spacing: 0.1em; }}
.oasis-rail-step.done {{ border-color: var(--oasis-teal-dark);
                         color: var(--oasis-teal); }}
.oasis-rail-step.current {{ background: var(--oasis-teal);
                            color: var(--oasis-bg); font-weight: 700;
                            border-color: var(--oasis-teal); }}

/* Progress meter */
.oasis-meter-track {{ background: var(--oasis-border); border-radius: 999px;
                      height: 8px; overflow: hidden; }}
.oasis-meter-fill {{ background: linear-gradient(90deg,
                        var(--oasis-teal-dark), var(--oasis-teal));
                     height: 100%; box-shadow: 0 0 8px var(--oasis-teal-glow); }}

/* Streamlit primitives → brand-tint (tabs, buttons, inputs) */
button[kind="primary"], .stButton>button {{ font-family: var(--oasis-mono);
    text-transform: uppercase; letter-spacing: 0.12em; font-size: 0.82em;
    border-radius: 6px; border: 1px solid var(--oasis-border); }}
.stButton>button:hover {{ border-color: var(--oasis-teal);
    color: var(--oasis-teal); }}
[data-baseweb="tab"] {{ font-family: var(--oasis-mono);
    text-transform: uppercase; letter-spacing: 0.12em; font-size: 0.8em; }}
[data-baseweb="tab"][aria-selected="true"] {{ color: var(--oasis-teal); }}
[data-baseweb="tab-highlight"] {{ background: var(--oasis-teal); }}
[data-testid="stMetricLabel"] {{ font-family: var(--oasis-mono);
    text-transform: uppercase; letter-spacing: 0.14em;
    color: var(--oasis-text-3); font-size: 0.72em; }}
[data-testid="stMetricValue"] {{ font-family: var(--oasis-mono);
    font-variant-numeric: tabular-nums; color: var(--oasis-text); }}
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
